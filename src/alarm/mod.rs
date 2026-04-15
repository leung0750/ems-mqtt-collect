mod engine;
mod storage;

pub use engine::AlarmConfig;
use storage::{has_unprocessed_alarm, insert_alarm_record, load_alarm_configs_by_device_name};

use std::collections::{HashMap, HashSet};

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlarmConfigData {
    pub id: i64,
    #[serde(default)]
    pub device_id: Option<String>,
    #[serde(default)]
    pub device_name: Option<String>,
    pub alarm_field: String,
    pub operator: String,
    pub threshold: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlarmRecord {
    pub config_id: i64,
    pub device_name: String,
    pub device_id: String,
    pub alarm_desc: String,
    pub snapshot_value: f64,
}

#[derive(Debug, Clone)]
enum AlarmConfigSource {
    Redis(String),
    Database,
}

pub async fn process_device_alarm(
    device_id: &str,
    device_name: &str,
    data_map: &HashMap<String, f64>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let (configs, source) = load_alarm_configs(device_id, device_name).await?;

    if configs.is_empty() {
        println!(
            "no active alarm configs found: device_name={}, device_id={}",
            device_name, device_id
        );
        return Ok(());
    }

    match &source {
        AlarmConfigSource::Redis(cache_key) => {
            println!(
                "alarm configs loaded from redis: device_name={}, device_id={}, key={}, count={}",
                device_name,
                device_id,
                cache_key,
                configs.len()
            );
        }
        AlarmConfigSource::Database => {
            println!(
                "alarm configs loaded from mysql fallback: device_name={}, device_id={}, count={}",
                device_name,
                device_id,
                configs.len()
            );
        }
    }

    for config in configs {
        let Some(&current_value) = data_map.get(&config.alarm_field) else {
            println!(
                "skip alarm config because field is missing: device_name={}, device_id={}, config_id={}, field={}",
                device_name,
                device_id,
                config.id,
                config.alarm_field
            );
            continue;
        };

        let alarm_config = match AlarmConfig::try_from(config.clone()) {
            Ok(config) => config,
            Err(err) => {
                eprintln!(
                    "skip alarm config with invalid operator: device_name={}, device_id={}, config_id={}, operator={}, err={}",
                    device_name,
                    device_id,
                    config.id,
                    config.operator,
                    err
                );
                continue;
            }
        };

        if !alarm_config.is_triggered(current_value) {
            println!(
                "alarm condition not met: device_name={}, device_id={}, config_id={}, field={}, value={:.2}, operator={}, threshold={:.2}",
                device_name,
                device_id,
                config.id,
                config.alarm_field,
                current_value,
                config.operator,
                config.threshold
            );
            continue;
        }

        let field_cn = crate::param_mapping::get_chinese_name(&config.alarm_field).await;
        let operator_desc = alarm_config.operator_desc();
        let alarm_desc = format!(
            "设备[{}]的[{}]当前值为{:.2}, {}阈值{:.2}",
            device_name, field_cn, current_value, operator_desc, config.threshold
        );

        let record = AlarmRecord {
            config_id: config.id,
            device_name: device_name.to_string(),
            device_id: device_id.to_string(),
            alarm_desc,
            snapshot_value: current_value,
        };

        if has_unprocessed_alarm(config.id, device_id).await? {
            println!(
                "skip duplicate unprocessed alarm: device_name={}, device_id={}, config_id={}",
                device_name, device_id, config.id
            );
            continue;
        }

        if let Err(err) = insert_alarm_record(&record).await {
            eprintln!(
                "failed to insert alarm record: device_name={}, device_id={}, config_id={}, err={}",
                device_name, device_id, config.id, err
            );
        } else {
            println!(
                "alarm triggered: device_name={}, device_id={}, config_id={}, field={}, value={:.2}, threshold={:.2}",
                device_name,
                device_id,
                config.id,
                config.alarm_field,
                current_value,
                config.threshold
            );
        }
    }

    Ok(())
}

fn build_alarm_cache_keys(device_id: &str, device_name: &str) -> Vec<String> {
    let mut keys = Vec::new();
    let mut seen = HashSet::new();

    let candidates = [
        format!("alarm::{}", device_id),
        if device_name.is_empty() {
            String::new()
        } else {
            let digest = format!("{:x}", md5::compute(device_name.as_bytes()));
            format!("alarm::d_{}", digest)
        },
        if device_name.is_empty() {
            String::new()
        } else {
            let digest = format!("{:x}", md5::compute(device_name.as_bytes()));
            format!("alarm::{}", digest)
        },
    ];

    for key in candidates {
        if key.is_empty() || !seen.insert(key.clone()) {
            continue;
        }
        keys.push(key);
    }

    keys
}

async fn load_alarm_configs(
    device_id: &str,
    device_name: &str,
) -> Result<(Vec<AlarmConfigData>, AlarmConfigSource), Box<dyn std::error::Error + Send + Sync>> {
    let cache_keys = build_alarm_cache_keys(device_id, device_name);

    for cache_key in &cache_keys {
        match load_alarm_configs_from_cache(cache_key).await {
            Ok(Some(configs)) if !configs.is_empty() => {
                return Ok((configs, AlarmConfigSource::Redis(cache_key.clone())));
            }
            Ok(_) => continue,
            Err(err) => {
                eprintln!(
                    "failed to read alarm cache key={} for device_name={}, device_id={}: {}",
                    cache_key, device_name, device_id, err
                );
            }
        }
    }

    match load_alarm_configs_by_device_name(device_name).await {
        Ok(configs) => Ok((configs, AlarmConfigSource::Database)),
        Err(err) => {
            eprintln!(
                "mysql fallback for alarm configs failed: device_name={}, device_id={}, cache_keys={:?}, err={}",
                device_name, device_id, cache_keys, err
            );
            Err(err)
        }
    }
}

async fn load_alarm_configs_from_cache(
    cache_key: &str,
) -> Result<Option<Vec<AlarmConfigData>>, Box<dyn std::error::Error + Send + Sync>> {
    let Some(config_json) = crate::redis::get_value(cache_key).await? else {
        return Ok(None);
    };

    if config_json.trim().is_empty() {
        eprintln!("alarm cache payload is empty for key={}", cache_key);
        return Ok(None);
    }

    match serde_json::from_str::<Vec<AlarmConfigData>>(&config_json) {
        Ok(configs) => Ok(Some(configs)),
        Err(err) => {
            eprintln!(
                "failed to parse alarm cache json for key={}: {}",
                cache_key, err
            );
            Ok(None)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_alarm_cache_keys_includes_redis_compatible_candidates() {
        let keys = build_alarm_cache_keys("d_123", "device-a");
        assert_eq!(keys[0], "alarm::d_123");
        assert!(keys.iter().any(|key| key.starts_with("alarm::d_")));
        assert!(keys
            .iter()
            .any(|key| key.starts_with("alarm::") && key.len() > "alarm::".len()));
    }

    #[test]
    fn alarm_config_data_accepts_missing_device_name_and_device_id() {
        let json = r#"[{"id":1,"alarm_field":"pw","operator":">","threshold":100.0}]"#;
        let configs: Vec<AlarmConfigData> = serde_json::from_str(json).unwrap();
        assert_eq!(configs.len(), 1);
        assert_eq!(configs[0].device_id, None);
        assert_eq!(configs[0].device_name, None);
        assert_eq!(configs[0].alarm_field, "pw");
    }
}
