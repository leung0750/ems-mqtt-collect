use crate::alarm::AlarmRecord;
use crate::mysqldb;
use sqlx::Row;

pub async fn insert_alarm_record(
    record: &AlarmRecord,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let pool = mysqldb::get_pool().await?;

    let sql = r#"
        INSERT INTO alarm_record (config_id, device_name, device_id, alarm_desc, snapshot_value, status)
        VALUES (?, ?, ?, ?, ?, 0)
    "#;

    sqlx::query(sql)
        .bind(record.config_id)
        .bind(&record.device_name)
        .bind(&record.device_id)
        .bind(&record.alarm_desc)
        .bind(record.snapshot_value)
        .execute(&pool)
        .await?;

    Ok(())
}

pub async fn has_unprocessed_alarm(
    config_id: i64,
    device_id: &str,
) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
    let pool = mysqldb::get_pool().await?;

    let sql = r#"
        SELECT COUNT(*) as count 
        FROM alarm_record 
        WHERE config_id = ? AND device_id = ? AND status = 0
    "#;

    let result = sqlx::query(sql)
        .bind(config_id)
        .bind(device_id)
        .fetch_one(&pool)
        .await?;

    let count: i64 = result.get("count");

    Ok(count > 0)
}

#[allow(dead_code)]
pub async fn get_alarm_configs_by_device(
    device_id: &str,
) -> Result<Vec<crate::alarm::AlarmConfigData>, Box<dyn std::error::Error + Send + Sync>> {
    let pool = mysqldb::get_pool().await?;

    let sql = r#"
        SELECT id, device_id, alarm_field, operator, threshold
        FROM alarm_config
        WHERE device_id = ?
    "#;

    let rows = sqlx::query(sql).bind(device_id).fetch_all(&pool).await?;

    let configs: Vec<crate::alarm::AlarmConfigData> = rows
        .into_iter()
        .map(|row| crate::alarm::AlarmConfigData {
            id: row.get("id"),
            device_id: row.get("device_id"),
            device_name: None,
            alarm_field: row.get("alarm_field"),
            operator: row.get("operator"),
            threshold: row.get("threshold"),
        })
        .collect();

    Ok(configs)
}

pub async fn load_alarm_configs_by_device_name(
    device_name: &str,
) -> Result<Vec<crate::alarm::AlarmConfigData>, Box<dyn std::error::Error + Send + Sync>> {
    let pool = mysqldb::get_pool().await?;

    let sql = r#"
        SELECT id, device_id, device_name, alarm_field, operator, threshold
        FROM alarm_config
        WHERE device_name = ? AND is_active = 1
        ORDER BY id ASC
    "#;

    let rows = sqlx::query(sql).bind(device_name).fetch_all(&pool).await?;

    let configs: Vec<crate::alarm::AlarmConfigData> = rows
        .into_iter()
        .map(|row| crate::alarm::AlarmConfigData {
            id: row.get("id"),
            device_id: Some(row.get("device_id")),
            device_name: Some(row.get("device_name")),
            alarm_field: row.get("alarm_field"),
            operator: row.get("operator"),
            threshold: row.get("threshold"),
        })
        .collect();

    Ok(configs)
}
