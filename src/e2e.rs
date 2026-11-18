//! End-to-end tests that do not require a live NexusNet node.

use std::sync::Arc;

use crate::compile_protocol::{AppRequest, AppResponse, PeerId};
use crate::control::new_control_handle;
use crate::test_support::MockControl;

use tokio::sync::{oneshot, watch};
use tokio::time::{Duration, timeout};

#[cfg(test)]
mod local_remote {
    use super::*;
    use std::collections::HashMap;
    use std::fs;
    use std::path::{Path, PathBuf};

    use crate::compile::TaskSource;
    use crate::compile::bundle::{TaskJob, build_bundle};
    use crate::compile::cache::ResultCache;
    use crate::compile::executor;
    use crate::compile::hash::{hash_bytes, hash_file};
    use crate::compile::manifest::collect_manifest;
    use crate::compile_protocol::{LeaseId, OutputRef, TaskId};
    use crate::config::ServiceConfig;
    use crate::pool::{Consumer, Provider};
    use crate::remote::Coordinator;
    use crate::workspace::Workspace;

    fn project(tag: &str) -> (crate::test_support::TempDir, PathBuf, TaskJob) {
        project_with(tag, "demo")
    }

    fn project_with(tag: &str, name: &str) -> (crate::test_support::TempDir, PathBuf, TaskJob) {
        let root = crate::test_support::TempDir::new(tag);
        let project = root.join("project");
        let out = project.join("out");
        let generated = project.join("generated");
        let cache = root.join("cache");
        fs::create_dir_all(project.join("src")).unwrap();
        fs::create_dir_all(&out).unwrap();
        fs::create_dir_all(&generated).unwrap();
        fs::write(
            project.join("src/lib.rs"),
            format!("pub fn {name}() -> u32 {{ 7 }}\n"),
        )
        .unwrap();

        let args = vec![
            "--crate-name".into(),
            name.into(),
            "--crate-type".into(),
            "lib".into(),
            "--edition".into(),
            "2021".into(),
            "--out-dir".into(),
            out.display().to_string(),
            "--emit=dep-info,metadata,link".into(),
            project.join("src/lib.rs").display().to_string(),
        ];
        let env = HashMap::from([
            ("CARGO_MANIFEST_DIR".into(), project.display().to_string()),
            ("OUT_DIR".into(), generated.display().to_string()),
            ("TARGET".into(), "x86_64-unknown-linux-gnu".into()),
            ("PROFILE".into(), "debug".into()),
            ("CARGO_INCREMENTAL".into(), "0".into()),
        ]);
        let manifest = collect_manifest(&project, Path::new("rustc"), &args, &env).unwrap();
        let task_id = manifest.digest().unwrap();
        let bundle = build_bundle(&project, &args, &env, &manifest).unwrap();
        let bundle_path = root.join("bundle");
        fs::write(&bundle_path, &bundle).unwrap();
        let bundle_hash = hash_bytes(&bundle);
        let (_, bundle_len) = hash_file(&bundle_path).unwrap();
        let job = TaskJob::from_bundle_file(
            &bundle_path,
            task_id,
            bundle_hash,
            bundle_len,
            root.join("work"),
            manifest.toolchain_id.clone(),
            manifest.target.clone(),
        )
        .unwrap();
        (root, cache, job)
    }

    #[test]
    fn local_compile_stores_result() {
        let (_root, cache_dir, job) = project("local");
        let cache = ResultCache::open(&cache_dir, 32 * 1024 * 1024).unwrap();
        let workspace = Workspace::new(&cache_dir);

        let first =
            executor::execute_job(&job, "rustc", &cache, &workspace, TaskSource::Local).unwrap();
        assert_eq!(first.exit_code, 0);
        assert!(!first.cache_hit);
        assert!(job.prepare().unwrap().prepared.out_dir.join("libdemo.rlib").is_file());
        assert!(cache.lookup(&job.task_id).is_some());
    }

    #[tokio::test]
    async fn local_coordinator_uses_provider_lease() {
        let (_root, cache_dir, job) = project("local-coordinator");
        let cache = Arc::new(ResultCache::open(&cache_dir, 32 * 1024 * 1024).unwrap());
        let workspace = Arc::new(Workspace::new(&cache_dir));
        let provider = Arc::new(Provider::new(PeerId::new("local"), 1));
        let task_id = TaskId(job.task_id);

        let coordinator = Coordinator::new(
            provider.clone(),
            Arc::new(Consumer::new()),
            cache.clone(),
            workspace,
            new_control_handle(),
            ServiceConfig::default(),
        );
        let lease = match coordinator.admit_local(&job) {
            crate::remote::LocalAdmission::Reserved(lease) => lease,
            crate::remote::LocalAdmission::Busy => panic!("expected a local slot"),
        };
        let result = coordinator
            .run_local_with_lease(job, "rustc".into(), lease)
            .await
            .unwrap();
        assert_eq!(result.exit_code, 0);
        assert!(!result.cache_hit);
        assert!(cache.lookup(&task_id.0).is_some());
        assert!(provider.task_entry(&task_id).is_none());
    }

    #[tokio::test]
    async fn scheduler_runs_local_origin_from_queue() {
        let (_root, cache_dir, job) = project("scheduler-local");
        let cache = Arc::new(ResultCache::open(&cache_dir, 32 * 1024 * 1024).unwrap());
        let workspace = Arc::new(Workspace::new(&cache_dir));
        let provider = Arc::new(Provider::new(PeerId::new("local"), 1));
        let queue = Arc::new(crate::scheduler::TaskQueue::new());
        let coordinator = Arc::new(Coordinator::new(
            provider.clone(),
            Arc::new(Consumer::new()),
            cache.clone(),
            workspace,
            new_control_handle(),
            ServiceConfig::default(),
        ));
        let scheduler = Arc::new(crate::scheduler::TaskScheduler::new(
            queue.clone(),
            coordinator,
        ));
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let scheduler_task = tokio::spawn(scheduler.run(shutdown_rx));

        let (reply_tx, reply_rx) = oneshot::channel();
        queue.push_back(crate::scheduler::QueuedTask {
            job,
            origin: crate::scheduler::TaskOrigin::Local,
            reply: crate::scheduler::TaskReply::Local(reply_tx),
            rustc: None,
        });

        let result = timeout(Duration::from_secs(30), reply_rx)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(result.exit_code, 0);
        assert!(cache.lookup(&result.action_digest).is_some());

        let _ = shutdown_tx.send(true);
        let _ = scheduler_task.await;
    }

    #[tokio::test]
    async fn remote_cache_hint_hits_when_provider_full() {
        let (_root, cache_dir, job) = project("remote-cache-full");
        let cache = Arc::new(ResultCache::open(&cache_dir, 32 * 1024 * 1024).unwrap());
        let workspace = Arc::new(Workspace::new(&cache_dir));

        let digest = job.task_id;
        let payload = b"remote cached rlib";
        let blob = cache.cas().put_bytes(payload).unwrap();
        let output = OutputRef {
            path: "libdemo.rlib".into(),
            hash: blob.hash,
            size: blob.size,
            mode: 0o644,
        };
        let remote = PeerId::new("provider-full");
        let mock = Arc::new(MockControl::new(vec![
            Ok(AppResponse::Cached {
                task_id: TaskId(digest),
                outputs: vec![output],
                lease_id: LeaseId::new(),
            }),
            Ok(AppResponse::Chunk {
                hash: blob.hash,
                offset: 0,
                data: payload.to_vec(),
            }),
            Ok(AppResponse::Ack),
        ]));
        let control = new_control_handle();
        *control.write().unwrap() = Some(mock.clone());

        let provider = Arc::new(Provider::new(PeerId::new("local"), 1));
        provider.set_capacity(1, 0);
        let consumer = Arc::new(Consumer::new());
        consumer.record_subscribed(remote.clone(), 1);
        consumer.apply_capacity(remote.clone(), 1, 0, 0, false);
        consumer.record_remote_cache(digest, &remote);

        let coordinator = Coordinator::new(
            provider,
            consumer,
            cache.clone(),
            workspace,
            control,
            ServiceConfig::default(),
        );
        let outputs = coordinator
            .import_cached(digest)
            .await
            .expect("remote cache hit");
        assert_eq!(outputs.len(), 1);
        assert!(cache.lookup(&digest).is_some());
        assert_eq!(mock.sent().len(), 3);
        assert!(matches!(mock.sent()[0].1, AppRequest::FetchCached { .. }));
    }

    #[tokio::test]
    async fn stale_remote_cache_hint_is_removed() {
        let (_root, cache_dir, job) = project("remote-cache-stale");
        let cache = Arc::new(ResultCache::open(&cache_dir, 32 * 1024 * 1024).unwrap());
        let workspace = Arc::new(Workspace::new(&cache_dir));
        let digest = job.task_id;
        let remote = PeerId::new("provider-stale");
        let mock = Arc::new(MockControl::new(vec![Ok(AppResponse::Busy {
            retry_after_ms: 1,
        })]));
        let control = new_control_handle();
        *control.write().unwrap() = Some(mock.clone());

        let provider = Arc::new(Provider::new(PeerId::new("local"), 1));
        provider.set_capacity(1, 1);
        let consumer = Arc::new(Consumer::new());
        consumer.record_remote_cache(digest, &remote);

        let coordinator = Coordinator::new(
            provider,
            consumer.clone(),
            cache.clone(),
            workspace,
            control,
            ServiceConfig::default(),
        );
        assert!(coordinator.import_cached(digest).await.is_none());
        assert_eq!(mock.sent().len(), 1);
        assert!(consumer.remote_cache_providers(&digest).is_empty());
    }

    #[tokio::test]
    async fn remote_cached_result_is_imported() {
        let (_root, cache_dir, job) = project("remote");
        let cache = Arc::new(ResultCache::open(&cache_dir, 32 * 1024 * 1024).unwrap());
        let workspace = Arc::new(Workspace::new(&cache_dir));

        let digest = job.task_id;
        let out_dir = job.prepare().unwrap().prepared.out_dir.clone();
        let payload = b"remote rlib bytes";
        let blob = cache.cas().put_bytes(payload).unwrap();
        let output = OutputRef {
            path: "libdemo.rlib".into(),
            hash: blob.hash,
            size: blob.size,
            mode: 0o644,
        };

        let mock = Arc::new(MockControl::new(vec![
            Ok(AppResponse::Cached {
                task_id: TaskId(digest),
                outputs: vec![output],
                lease_id: LeaseId::new(),
            }),
            Ok(AppResponse::Chunk {
                hash: blob.hash,
                offset: 0,
                data: payload.to_vec(),
            }),
            Ok(AppResponse::Ack),
        ]));
        let control = new_control_handle();
        *control.write().unwrap() = Some(mock.clone());

        let provider = Arc::new(Provider::new(PeerId::new("local"), 1));
        provider.set_capacity(1, 0);
        let consumer = Arc::new(Consumer::new());
        let remote = PeerId::new("provider");
        consumer.record_subscribed(remote.clone(), 1);
        consumer.apply_capacity(remote.clone(), 1, 1, 0, true);
        consumer.record_remote_cache(digest, &remote);

        let coordinator = Coordinator::new(
            provider,
            consumer,
            cache.clone(),
            workspace,
            control,
            ServiceConfig::default(),
        );
        let outputs = coordinator
            .import_cached(digest)
            .await
            .expect("remote cache hit");
        assert_eq!(outputs.len(), 1);
        assert!(!out_dir.join("libdemo.rlib").exists());
        assert!(cache.lookup(&digest).is_some());
        assert_eq!(mock.sent().len(), 3);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn slot_admission_does_not_overdraft() {
        let (_root0, cache_dir, job0) = project_with("admit-0", "demo0");
        let (_r1, _, job1) = project_with("admit-1", "demo1");
        let (_r2, _, job2) = project_with("admit-2", "demo2");
        let cache = Arc::new(ResultCache::open(&cache_dir, 32 * 1024 * 1024).unwrap());
        let workspace = Arc::new(Workspace::new(&cache_dir));
        let provider = Arc::new(Provider::new(PeerId::new("local"), 1));
        let queue = Arc::new(crate::scheduler::TaskQueue::new());
        let coordinator = Arc::new(Coordinator::new(
            provider.clone(),
            Arc::new(Consumer::new()),
            cache,
            workspace,
            new_control_handle(),
            ServiceConfig::default(),
        ));
        let scheduler = Arc::new(crate::scheduler::TaskScheduler::new(queue.clone(), coordinator));
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let handle = tokio::spawn(scheduler.run(shutdown_rx));

        let mut replies = Vec::new();
        for job in [job0, job1, job2] {
            let (tx, rx) = oneshot::channel();
            queue.push_back(crate::scheduler::QueuedTask {
                job,
                origin: crate::scheduler::TaskOrigin::Local,
                reply: crate::scheduler::TaskReply::Local(tx),
                rustc: None,
            });
            replies.push(rx);
        }

        for rx in replies {
            let result = timeout(Duration::from_secs(90), rx)
                .await
                .expect("scheduler stalled")
                .expect("scheduler dropped the reply");
            assert!(result.is_ok(), "task must not be rejected: {result:?}");
        }

        let _ = shutdown_tx.send(true);
        let _ = handle.await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn busy_queue_waits_for_capacity() {
        let (_root, cache_dir, job) = project_with("busy-wait", "demob");
        let cache = Arc::new(ResultCache::open(&cache_dir, 32 * 1024 * 1024).unwrap());
        let workspace = Arc::new(Workspace::new(&cache_dir));
        let provider = Arc::new(Provider::new(PeerId::new("local"), 1));
        provider.set_capacity(1, 0);
        let queue = Arc::new(crate::scheduler::TaskQueue::new());
        let coordinator = Arc::new(Coordinator::new(
            provider.clone(),
            Arc::new(Consumer::new()),
            cache,
            workspace,
            new_control_handle(),
            ServiceConfig::default(),
        ));
        let scheduler = Arc::new(crate::scheduler::TaskScheduler::new(queue.clone(), coordinator));
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let handle = tokio::spawn(scheduler.run(shutdown_rx));

        let (tx, rx) = oneshot::channel();
        queue.push_back(crate::scheduler::QueuedTask {
            job,
            origin: crate::scheduler::TaskOrigin::Local,
            reply: crate::scheduler::TaskReply::Local(tx),
            rustc: None,
        });

        tokio::time::sleep(Duration::from_millis(300)).await;
        provider.set_capacity(1, 1);

        let result = timeout(Duration::from_secs(10), rx)
            .await
            .expect("scheduler spun or never retried")
            .expect("reply dropped");
        assert!(
            result.is_ok(),
            "task should run after capacity returns: {result:?}"
        );

        let _ = shutdown_tx.send(true);
        let _ = handle.await;
    }
}
