use anyhow::Context;
use config::Config;
use config::ConfigBuilder;
use config::File;
use config::FileFormat;
use config::Value;
use directories::BaseDirs;
use serde::Deserialize;
use std::path::PathBuf;

const SYSTEM_CONFIG_PATH: &str = "/etc/photohash/config.toml";
const DEFAULT_IGNORE_FILENAME_PATTERNS: &[&str] = &[".picasa.ini", "._*"];

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct AppConfig {
    pub scan: ScanConfig,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ScanConfig {
    pub path_queue_depth: usize,
    pub meta_queue_depth: usize,
    pub ignore_filename_patterns: Vec<String>,
}

impl Default for ScanConfig {
    fn default() -> Self {
        Self {
            path_queue_depth: 10_000,
            meta_queue_depth: 10_000,
            ignore_filename_patterns: DEFAULT_IGNORE_FILENAME_PATTERNS
                .iter()
                .map(|pattern| pattern.to_string())
                .collect(),
        }
    }
}

impl AppConfig {
    pub fn load(overrides: &[String]) -> anyhow::Result<Self> {
        let mut builder = Config::builder()
            .set_default(
                "scan.path_queue_depth",
                Self::default().scan.path_queue_depth as i64,
            )?
            .set_default(
                "scan.meta_queue_depth",
                Self::default().scan.meta_queue_depth as i64,
            )?
            .add_source(File::new(SYSTEM_CONFIG_PATH, FileFormat::Toml).required(false));

        if let Some(user_config_path) = user_config_path() {
            builder = builder.add_source(
                File::from(user_config_path)
                    .format(FileFormat::Toml)
                    .required(false),
            );
        }

        builder = apply_overrides(builder, overrides)?;

        builder
            .build()?
            .try_deserialize()
            .context("failed to deserialize configuration")
    }
}

fn user_config_path() -> Option<PathBuf> {
    BaseDirs::new().map(|dirs| dirs.config_dir().join("photohash.toml"))
}

fn apply_overrides(
    mut builder: ConfigBuilder<config::builder::DefaultState>,
    overrides: &[String],
) -> anyhow::Result<ConfigBuilder<config::builder::DefaultState>> {
    for option in overrides {
        let (key, value) = option.split_once('=').with_context(|| {
            format!("invalid configuration override {option:?}: expected key=value")
        })?;
        validate_key(key)?;
        builder = builder.set_override(key, parse_value(value))?;
    }
    Ok(builder)
}

fn validate_key(key: &str) -> anyhow::Result<()> {
    let mut parts = key.split('.');
    match (parts.next(), parts.next(), parts.next()) {
        (Some(section), Some(name), None) if !section.is_empty() && !name.is_empty() => Ok(()),
        _ => anyhow::bail!("invalid configuration key {key:?}: expected section.key"),
    }
}

fn parse_value(value: &str) -> Value {
    if let Ok(value) = value.parse::<bool>() {
        return Value::from(value);
    }
    if let Ok(value) = value.parse::<i64>() {
        return Value::from(value);
    }
    if let Ok(value) = value.parse::<f64>() {
        return Value::from(value);
    }
    Value::from(value.to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scan_config_defaults_ignore_photo_sidecars() {
        assert_eq!(
            vec![".picasa.ini".to_owned(), "._*".to_owned()],
            ScanConfig::default().ignore_filename_patterns
        );
    }
}
