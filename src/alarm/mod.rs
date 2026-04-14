mod engine;
mod storage;

pub use engine::AlarmConfig;
use storage::{has_unprocessed_alarm, insert_alarm_record};

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlarmConfigData {
    pub id: i64,
    pub device_id: String,
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

pub async fn process_device_alarm(
    device_id: &str,
    device_name: &str,
    data_map: &std::collections::HashMap<String, f64>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let redis_key = format!("alarm::{}", device_id);
    let config_json = match crate::redis::get_value(&redis_key).await? {
        Some(json) if !json.is_empty() => json,
        _ => return Ok(()),
    };

    let configs: Vec<AlarmConfigData> = match serde_json::from_str(&config_json) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("解析告警配置失败: {}", e);
            return Ok(());
        }
    };

    if configs.is_empty() {
        return Ok(());
    }

    for config in configs {
        if let Some(&current_value) = data_map.get(&config.alarm_field) {
            if let Ok(alarm_config) = AlarmConfig::try_from(config.clone()) {
                if alarm_config.is_triggered(current_value) {
                    let field_cn =
                        crate::param_mapping::get_chinese_name(&config.alarm_field).await;
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

                    if !has_unprocessed_alarm(config.id, device_id).await? {
                        if let Err(e) = insert_alarm_record(&record).await {
                            eprintln!("写入告警记录失败: {}", e);
                        } else {
                            println!("告警触发: {}", record.alarm_desc);
                        }
                    }
                }
            }
        }
    }

    Ok(())
}
