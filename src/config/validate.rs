use std::fmt;

use super::model::ServiceConfig;

/// One non-fatal configuration warning.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfigWarning {
    pub code: &'static str,
    pub message: String,
}

impl fmt::Display for ConfigWarning {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl ServiceConfig {
    /// Non-fatal configuration warnings. These do not reject the file.
    pub fn warnings(&self) -> Vec<ConfigWarning> {
        let mut warnings = Vec::new();

        if self.port == 0 || self.port > u16::MAX as u32 {
            warnings.push(ConfigWarning {
                code: "port",
                message: format!("port 超出范围 1..={}: {}", u16::MAX, self.port),
            });
        }
        if self.service_name.trim().is_empty() {
            warnings.push(ConfigWarning {
                code: "service_name",
                message: "service_name 为空".into(),
            });
        }
        if self.node.max_usage_percent > 100 {
            warnings.push(ConfigWarning {
                code: "max_usage_percent",
                message: format!(
                    "node.max_usage_percent 大于 100: {}",
                    self.node.max_usage_percent
                ),
            });
        }
        if self.node.sample_interval_secs == 0 {
            warnings.push(ConfigWarning {
                code: "sample_interval_secs",
                message: "node.sample_interval_secs 为 0".into(),
            });
        }
        if self.subscription.update_interval_secs == 0 {
            warnings.push(ConfigWarning {
                code: "update_interval_secs",
                message: "subscription.update_interval_secs 为 0".into(),
            });
        }
        if self.subscription.ttl_secs == 0 {
            warnings.push(ConfigWarning {
                code: "ttl_secs",
                message: "subscription.ttl_secs 为 0".into(),
            });
        }
        if self.subscription.target_reevaluate_secs == 0 {
            warnings.push(ConfigWarning {
                code: "target_reevaluate_secs",
                message: "subscription.target_reevaluate_secs 为 0".into(),
            });
        }
        if self.subscription.ttl_secs <= self.subscription.update_interval_secs {
            warnings.push(ConfigWarning {
                code: "ttl_vs_update",
                message: format!(
                    "subscription.ttl_secs ({}) 应大于 update_interval_secs ({})",
                    self.subscription.ttl_secs, self.subscription.update_interval_secs
                ),
            });
        }
        if self.subscription.target_reevaluate_secs > self.subscription.ttl_secs {
            warnings.push(ConfigWarning {
                code: "reevaluate_vs_ttl",
                message: format!(
                    "subscription.target_reevaluate_secs ({}) 大于 ttl_secs ({})",
                    self.subscription.target_reevaluate_secs, self.subscription.ttl_secs
                ),
            });
        }

        for entry in &self.node.toolchains {
            if entry.rustc.trim().is_empty() {
                warnings.push(ConfigWarning {
                    code: "toolchain_rustc",
                    message: "node.toolchains[].rustc 为空".into(),
                });
            }
        }

        warnings
    }
}
