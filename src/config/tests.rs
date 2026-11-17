use std::fs;

use super::*;
use crate::test_support::TempDir;

#[test]
fn legacy_config_uses_defaults_without_rewriting() {
    let config: ServiceConfig = toml::from_str("port = 5100").unwrap();
    assert_eq!(config.port, 5100);
    assert_eq!(config.service_name, "rustc-pool");
    assert_eq!(config.node.max_usage_percent, 80);
    assert_eq!(config.node.sample_interval_secs, 1);
    assert_eq!(config.subscription.target_slots, 8);
    assert_eq!(config.subscription.update_interval_secs, 30);
    assert_eq!(config.subscription.ttl_secs, 60);
    assert_eq!(config.subscription.target_reevaluate_secs, 5);
    assert_eq!(config.cache.result_cache_mb, 512);
    assert!(config.cache.dir.is_empty());

    let root = TempDir::new("config");
    let path = root.join("config.toml");
    let original = r#"# keep me
port = 6100

[node]
cores_per_slot = 2.0
mem_per_slot_mb = 4096
reserve_cores = 1.0
reserve_mem_mb = 2048
sample_interval_secs = 3
rustc = "rustc-custom"
toolchain = "old-toolchain"
target = "old-target"

[subscription]
target_slots = 12
static_peers = ["peer-a"]

[cache]
dir = "/tmp/cache"
result_cache_mb = 128
"#;
    fs::write(&path, original).unwrap();

    let config = ServiceConfig::try_from_toml_file(&path, true).unwrap();
    assert_eq!(config.port, 6100);
    assert_eq!(config.service_name, "rustc-pool");
    assert_eq!(config.node.max_usage_percent, 80);
    assert_eq!(config.node.sample_interval_secs, 3);
    assert_eq!(config.node.rustc, "rustc-custom");
    assert_eq!(config.subscription.target_slots, 12);
    assert_eq!(config.cache.dir, "/tmp/cache");
    assert_eq!(config.cache.result_cache_mb, 128);
    assert_eq!(fs::read_to_string(&path).unwrap(), original);
    assert!(!root.join("config.toml.bak").exists());
}

#[test]
fn missing_file_is_created_only_when_requested() {
    let root = TempDir::new("config");
    let path = root.join("config.toml");

    let config = ServiceConfig::try_from_toml_file(&path, false).unwrap();
    assert_eq!(config.port, 5100);
    assert!(!path.exists());

    let created = ServiceConfig::try_from_toml_file(&path, true).unwrap();
    assert_eq!(created.port, 5100);
    assert!(path.is_file());
}

#[test]
fn invalid_config_does_not_replace_or_rename() {
    let root = TempDir::new("config-invalid");
    let path = root.join("config.toml");
    let original = "[node\n";
    fs::write(&path, original).unwrap();

    let error = ServiceConfig::try_from_toml_file(&path, true).unwrap_err();
    assert_eq!(error.kind, ConfigErrorKind::Parse, "case: parse error");
    assert_eq!(fs::read_to_string(&path).unwrap(), original);
    assert!(!root.join("config.toml.bak").exists());

    let root = TempDir::new("config-load");
    let path = root.join("config.toml");
    fs::write(&path, "port = 6100\n").unwrap();
    let loaded = ServiceConfig::load_or_create(&path, false).unwrap();
    assert!(!loaded.created);
    assert!(!loaded.missing);
    assert_eq!(loaded.config.port, 6100);

    let missing = ServiceConfig::load_or_create(root.join("missing.toml"), false).unwrap();
    assert!(missing.missing);
    assert!(!missing.created);
    assert_eq!(missing.config.port, 5100);
}

#[test]
fn toolchain_list_roundtrips() {
    let mut config = ServiceConfig::default();
    config.node.toolchains = vec![
        ToolchainEntry {
            channel: "stable".into(),
            rustc: "/opt/stable/bin/rustc".into(),
            target: "x86_64-unknown-linux-gnu".into(),
        },
        ToolchainEntry {
            channel: "1.99.0".into(),
            rustc: "/opt/199/bin/rustc".into(),
            target: String::new(),
        },
    ];

    let text = toml::to_string_pretty(&config).unwrap();
    let parsed: ServiceConfig = toml::from_str(&text).unwrap();
    assert_eq!(parsed.node.toolchains, config.node.toolchains);
}

#[test]
fn warnings_have_stable_codes() {
    let config = ServiceConfig {
        port: 0,
        subscription: SubscriptionConfig {
            ttl_secs: 0,
            ..Default::default()
        },
        ..Default::default()
    };

    let warnings = config.warnings();
    assert!(warnings.iter().any(|warning| warning.code == "port"));
    assert!(warnings.iter().any(|warning| warning.code == "ttl_secs"));
    assert!(warnings.iter().all(|warning| !warning.message.is_empty()));
}
