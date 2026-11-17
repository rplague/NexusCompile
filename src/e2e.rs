//! End-to-end tests that do not require a live NexusNet node.

use std::sync::Arc;

use crate::compile_protocol::{AppResponse, PeerId};
use crate::test_support::MockControl;

#[cfg(test)]
mod local_remote {
    use super::*;
    use std::collections::HashMap;
    use std::fs;
    use std::path::{Path, PathBuf};

    use crate::compile::cache::ResultCache;
    use crate::compile::executor;
    use crate::compile::manifest::collect_manifest;
    use crate::compile_protocol::{LeaseId, OutputRef, TaskId};
    use crate::config::ServiceConfig;
    use crate::local_protocol::{LocalBody, LocalRequest};
    use crate::pool::{Consumer, Provider};
    use crate::remote::Coordinator;
    use crate::workspace::Workspace;

    fn project(tag: &str) -> (crate::test_support::TempDir, PathBuf, LocalRequest) {
        let root = crate::test_support::TempDir::new(tag);
        let project = root.join("project");
        let out = project.join("out");
        let generated = project.join("generated");
        let cache = root.join("cache");
        fs::create_dir_all(project.join("src")).unwrap();
        fs::create_dir_all(&out).unwrap();
        fs::create_dir_all(&generated).unwrap();
        fs::write(project.join("src/lib.rs"), "pub fn demo() -> u32 { 7 }\n").unwrap();

        let args = vec![
            "--crate-name".into(),
            "demo".into(),
            "--crate-type".into(),
            "lib".into(),
            "--edition".into(),
            "2021".into(),
            "--out-dir".into(),
            out.display().to_string(),
            "--emit=dep-info,metadata,link".into(),
            project.join("src/lib.rs").display().to_string(),
        ];
        let env = vec![
            ("CARGO_MANIFEST_DIR".into(), project.display().to_string()),
            ("OUT_DIR".into(), generated.display().to_string()),
            ("TARGET".into(), "x86_64-unknown-linux-gnu".into()),
            ("PROFILE".into(), "debug".into()),
            ("CARGO_INCREMENTAL".into(), "0".into()),
        ];
        let request = LocalRequest::new(project.display().to_string(), "rustc", args, env);
        (root, cache, request)
    }

    #[test]
    fn local_compile_then_cache_hit() {
        let (root, cache_dir, request) = project("local");
        let cache = ResultCache::open(&cache_dir, 32 * 1024 * 1024).unwrap();
        let workspace = Workspace::new(&cache_dir);

        let first = executor::execute_local(&request, &cache, &workspace);
        match first.body {
            LocalBody::Compiled {
                exit_code,
                cache_hit,
                ..
            } => {
                assert_eq!(exit_code, 0);
                assert!(!cache_hit);
            }
            other => panic!("expected compiled, got {other:?}"),
        }
        let out = Path::new(&request.cwd).join("out");
        assert!(out.join("libdemo.rlib").is_file());

        let second = executor::execute_local(&request, &cache, &workspace);
        match second.body {
            LocalBody::Compiled {
                exit_code,
                cache_hit,
                ..
            } => {
                assert_eq!(exit_code, 0);
                assert!(cache_hit);
            }
            other => panic!("expected cached compiled, got {other:?}"),
        }

    }

    #[tokio::test]
    async fn remote_cached_result_is_imported() {
        let (root, cache_dir, request) = project("remote");
        let cache = Arc::new(ResultCache::open(&cache_dir, 32 * 1024 * 1024).unwrap());
        let workspace = Arc::new(Workspace::new(&cache_dir));

        let env: HashMap<String, String> = request.env.iter().cloned().collect();
        let manifest = collect_manifest(
            Path::new(&request.cwd),
            Path::new(&request.rustc),
            &request.args,
            &env,
        )
        .unwrap();
        let digest = manifest.digest().unwrap();

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
        consumer.record_subscribed(PeerId::new("provider"), 1);
        consumer.apply_capacity(PeerId::new("provider"), 1, 1, 0, true);

        let coordinator = Coordinator::new(
            provider,
            consumer,
            cache.clone(),
            workspace,
            control,
            ServiceConfig::default(),
        );
        let response = coordinator.execute(&request).await;
        match response.body {
            LocalBody::Compiled {
                exit_code,
                cache_hit,
                ..
            } => {
                assert_eq!(exit_code, 0);
                assert!(!cache_hit);
            }
            other => panic!("expected remote success, got {other:?}"),
        }
        let out = Path::new(&request.cwd).join("out");
        assert_eq!(fs::read(out.join("libdemo.rlib")).unwrap(), payload);
        assert!(cache.cached_outputs(&digest).is_some());
        assert_eq!(mock.sent().len(), 3);

    }
}
