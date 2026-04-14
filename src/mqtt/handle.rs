use std::collections::HashMap;

use anyhow::anyhow;
use chrono::Utc;
use once_cell::sync::OnceCell;
use serde::{Deserialize, Serialize};
use sqlx::Row;
use taos::AsyncQueryable;
use uuid::Uuid;

use crate::mysqldb;
use crate::tdengine;
use crate::tdengine::{get_tdengine_client, reconnect_tdengine, TDENGINE_CONNECTED};

type EnergyType = i32;
const CACHE_TTL_SECONDS: usize = 7 * 24 * 3600;
const CACHE_LIST_KEY: &str = "tdengine:cache:list";
const BATCH_SIZE: usize = 500;
const ENERGY_TYPE_CACHE_KEY_PREFIX: &str = "device::energy_type::";
const DEFAULT_ENERGY_TYPE_CACHE_TTL_SECONDS: usize = 10 * 60;
const DEFAULT_ENERGY_TYPE_MISS_CACHE_TTL_SECONDS: usize = 30;
const GATEWAY_TS_OFFSET_KEY_PREFIX: &str = "gateway::ts_offset::";
const REALTIME_SNAPSHOT_KEY_PREFIX: &str = "rtm::device::";
const REALTIME_SNAPSHOT_SORTED_SET: &str = "rtm::latest";
const REALTIME_SNAPSHOT_ENERGY_SORTED_SET_PREFIX: &str = "rtm::latest::energy::";
static ENERGY_TYPE_CACHE_CONFIG: OnceCell<EnergyTypeCacheConfig> = OnceCell::new();

#[derive(Debug, Deserialize)]
struct MeasurementData {
    m: String,
    v: f64,
}

#[derive(Debug, Deserialize)]
struct Device {
    dev: String,
    d: Vec<MeasurementData>,
}

#[derive(Debug, Deserialize)]
struct MqttMessage {
    devs: Vec<Device>,
    #[allow(dead_code)]
    ver: String,
    #[allow(non_snake_case, dead_code)]
    pKey: String,
    sn: String,
    ts: i64,
}

#[derive(Debug, Serialize, Deserialize)]
struct CachedData {
    id: String,
    device_name: String,
    energy_type: i32,
    gateway_name: String,
    timestamp: i64,
    data_map: HashMap<String, f64>,
    created_at: i64,
}

#[derive(Debug, Serialize, Deserialize)]
struct RealtimeSnapshot {
    device_name: String,
    energy_type: i32,
    timestamp: i64,
    data_map: HashMap<String, f64>,
}

#[derive(Debug, Clone, Deserialize)]
struct EnergyTypeCacheConfig {
    ttl_seconds: Option<usize>,
    miss_ttl_seconds: Option<usize>,
}

impl Default for EnergyTypeCacheConfig {
    fn default() -> Self {
        Self {
            ttl_seconds: Some(DEFAULT_ENERGY_TYPE_CACHE_TTL_SECONDS),
            miss_ttl_seconds: Some(DEFAULT_ENERGY_TYPE_MISS_CACHE_TTL_SECONDS),
        }
    }
}

const REALTIME_TS_FUTURE_TOLERANCE: i64 = 5 * 60;
const REALTIME_TS_PAST_WINDOW: i64 = 2 * 3600;
const TIMEZONE_OFFSET_SECONDS: i64 = 8 * 3600;
const GATEWAY_OFFSET_CACHE_TTL: usize = 30 * 24 * 3600;
const REALTIME_ONLINE_TTL: usize = 30 * 60;

fn get_energy_type_cache_config() -> &'static EnergyTypeCacheConfig {
    ENERGY_TYPE_CACHE_CONFIG.get_or_init(|| {
        let fallback = EnergyTypeCacheConfig::default();
        match crate::conf::load_config::<EnergyTypeCacheConfig>("ENERGY_TYPE_CACHE") {
            Ok(config) => EnergyTypeCacheConfig {
                ttl_seconds: config
                    .ttl_seconds
                    .filter(|value| *value > 0)
                    .or(fallback.ttl_seconds),
                miss_ttl_seconds: config
                    .miss_ttl_seconds
                    .filter(|value| *value > 0)
                    .or(fallback.miss_ttl_seconds),
            },
            Err(err) => {
                eprintln!(
                    "load energy type cache config failed, using defaults: {}",
                    err
                );
                fallback
            }
        }
    })
}

fn energy_type_cache_ttl_seconds() -> usize {
    get_energy_type_cache_config()
        .ttl_seconds
        .unwrap_or(DEFAULT_ENERGY_TYPE_CACHE_TTL_SECONDS)
}

fn energy_type_miss_cache_ttl_seconds() -> usize {
    get_energy_type_cache_config()
        .miss_ttl_seconds
        .unwrap_or(DEFAULT_ENERGY_TYPE_MISS_CACHE_TTL_SECONDS)
}

async fn get_energy_type(device_name: &str) -> EnergyType {
    let redis_key = format!("{}{}", ENERGY_TYPE_CACHE_KEY_PREFIX, device_name);
    let energy_type_str = crate::redis::get_value(&redis_key)
        .await
        .unwrap_or(Some("-1".to_string()));
    let energy_type = energy_type_str
        .unwrap_or_else(|| "-1".to_string())
        .parse::<i32>()
        .unwrap_or(-1);
    if energy_type != -1 {
        return energy_type;
    }

    let pool = match mysqldb::get_pool().await {
        Ok(pool) => pool,
        Err(_) => return -1,
    };

    match load_energy_type_from_db(&pool, device_name).await {
        Ok(Some(db_energy_type)) => {
            if let Err(err) = cache_energy_type(device_name, db_energy_type).await {
                eprintln!(
                    "cache energy type to redis failed for {}: {}",
                    device_name, err
                );
            }
            db_energy_type
        }
        Ok(None) => {
            if let Err(err) = cache_missing_energy_type(device_name).await {
                eprintln!(
                    "cache missing energy type to redis failed for {}: {}",
                    device_name, err
                );
            }
            -1
        }
        Err(err) => {
            eprintln!(
                "load energy type from mysql failed for {}: {}",
                device_name, err
            );
            -1
        }
    }
}

async fn load_energy_type_from_db(
    pool: &sqlx::MySqlPool,
    device_name: &str,
) -> Result<Option<i32>, sqlx::Error> {
    let result = sqlx::query("SELECT energy_type FROM device WHERE name = ?")
        .bind(device_name)
        .fetch_optional(pool)
        .await?;

    Ok(result.map(|row| row.get(0)))
}

async fn cache_energy_type(device_name: &str, energy_type: i32) -> Result<(), redis::RedisError> {
    let redis_key = format!("{}{}", ENERGY_TYPE_CACHE_KEY_PREFIX, device_name);
    crate::redis::set_ex(
        &redis_key,
        &energy_type.to_string(),
        energy_type_cache_ttl_seconds(),
    )
    .await
}

async fn cache_missing_energy_type(device_name: &str) -> Result<(), redis::RedisError> {
    let redis_key = format!("{}{}", ENERGY_TYPE_CACHE_KEY_PREFIX, device_name);
    crate::redis::set_ex(&redis_key, "-1", energy_type_miss_cache_ttl_seconds()).await
}

pub async fn refresh_energy_type_cache() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let pool = mysqldb::get_pool().await?;
    let total_devices: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM device")
        .fetch_one(&pool)
        .await
        .unwrap_or(0);

    let rows = sqlx::query(
        "SELECT name, energy_type FROM device WHERE energy_type IS NOT NULL AND energy_type > 0",
    )
    .fetch_all(&pool)
    .await?;

    let mut refreshed = 0usize;
    let mut sample_names = Vec::new();
    for row in rows {
        let device_name: String = row.get("name");
        let energy_type: i32 = row.get("energy_type");

        cache_energy_type(&device_name, energy_type).await?;
        refreshed += 1;
        if sample_names.len() < 5 {
            sample_names.push(device_name);
        }
    }

    println!(
        "refreshed energy type cache: cached={} total_devices={} sample={:?}",
        refreshed, total_devices, sample_names
    );
    if total_devices == 0 {
        eprintln!("device table is empty in current MySQL database");
    } else if refreshed == 0 {
        eprintln!("device table has data, but no rows with valid energy_type > 0 were found");
    }
    Ok(())
}

pub async fn handle_data(
    topic: &str,
    payload: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    if !topic.contains("/history") && !topic.contains("/rtg") {
        return Ok(());
    }
    let is_realtime_topic = topic.contains("/rtg");

    let message: MqttMessage = match serde_json::from_str(payload) {
        Ok(message) => message,
        Err(err) => {
            eprintln!("parse mqtt payload failed: {}", err);
            return Ok(());
        }
    };

    for device in &message.devs {
        let device_name = &device.dev;
        let energy_type = get_energy_type(device_name).await;
        if energy_type == -1 {
            eprintln!("skip device without energy type: {}", device_name);
            continue;
        }

        let normalized_ts =
            normalize_message_timestamp(topic, &message.sn, device_name, message.ts).await;

        let mut flat_data = Vec::new();
        let mut data_map = HashMap::new();
        for measurement in &device.d {
            if let Some(field) = crate::param_mapping::get_standard_field(&measurement.m).await {
                flat_data.push(format!("{}:{}", field, measurement.v));
                data_map.insert(field, measurement.v);
            }
        }

        if data_map.is_empty() || should_skip_device(energy_type, &data_map) {
            continue;
        }

        if is_realtime_topic {
            let alive_key = format!("alive::{}", device_name);
            crate::redis::set_ex(&alive_key, &normalized_ts.to_string(), REALTIME_ONLINE_TTL)
                .await?;
            cache_realtime_snapshot(device_name, energy_type, normalized_ts, &data_map).await?;
        }

        let gateway_name = &message.sn;
        if let Err(err) = write_realtime_data(
            device_name,
            energy_type,
            gateway_name,
            normalized_ts,
            &data_map,
        )
        .await
        {
            eprintln!("write realtime data failed for {}: {}", device_name, err);
            continue;
        }

        println!(
            "gateway={} device={} energy_type={} ts={} data={}",
            gateway_name,
            device_name,
            energy_type,
            format_timestamp(normalized_ts),
            flat_data.join(" ")
        );

        let device_id = tdengine::get_subtable_name(device_name)?;
        if let Err(err) =
            crate::alarm::process_device_alarm(&device_id, device_name, &data_map).await
        {
            eprintln!("process alarm failed for {}: {}", device_name, err);
        }
    }

    Ok(())
}

async fn cache_realtime_snapshot(
    device_name: &str,
    energy_type: i32,
    timestamp: i64,
    data_map: &HashMap<String, f64>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let payload = serde_json::to_string(&RealtimeSnapshot {
        device_name: device_name.to_string(),
        energy_type,
        timestamp,
        data_map: data_map.clone(),
    })?;
    let snapshot_key = format!("{}{}", REALTIME_SNAPSHOT_KEY_PREFIX, device_name);
    crate::redis::set_ex(&snapshot_key, &payload, REALTIME_ONLINE_TTL).await?;
    crate::redis::zadd(REALTIME_SNAPSHOT_SORTED_SET, timestamp, device_name).await?;
    crate::redis::zadd(
        &format!(
            "{}{}",
            REALTIME_SNAPSHOT_ENERGY_SORTED_SET_PREFIX, energy_type
        ),
        timestamp,
        device_name,
    )
    .await?;
    Ok(())
}

async fn load_gateway_ts_offset(gateway_name: &str) -> Option<i64> {
    let key = format!("{}{}", GATEWAY_TS_OFFSET_KEY_PREFIX, gateway_name);
    let value = crate::redis::get_value(&key).await.ok().flatten()?;
    value.trim().parse::<i64>().ok()
}

async fn store_gateway_ts_offset(gateway_name: &str, offset: i64) {
    let key = format!("{}{}", GATEWAY_TS_OFFSET_KEY_PREFIX, gateway_name);
    if let Err(err) =
        crate::redis::set_ex(&key, &offset.to_string(), GATEWAY_OFFSET_CACHE_TTL).await
    {
        eprintln!(
            "cache gateway timestamp offset failed for {}: {}",
            gateway_name, err
        );
    }
}

async fn normalize_message_timestamp(
    topic: &str,
    gateway_name: &str,
    device_name: &str,
    timestamp: i64,
) -> i64 {
    if timestamp <= 0 {
        return timestamp;
    }

    let now = Utc::now().timestamp();
    let is_realtime = topic.contains("/rtg");
    let cached_offset = load_gateway_ts_offset(gateway_name).await;

    let mut offsets = vec![0, -TIMEZONE_OFFSET_SECONDS, TIMEZONE_OFFSET_SECONDS];
    if let Some(offset) = cached_offset {
        offsets.retain(|item| *item != offset);
        offsets.insert(0, offset);
    }

    let raw_delta = (timestamp - now).abs();
    let mut best_offset = 0;
    let mut best = timestamp;
    let mut best_delta = raw_delta;
    for offset in offsets {
        let candidate = timestamp + offset;
        let delta = (candidate - now).abs();
        if delta < best_delta {
            best = candidate;
            best_delta = delta;
            best_offset = offset;
        }
    }

    if !is_realtime {
        if let Some(offset) = cached_offset {
            let shifted = timestamp + offset;
            if shifted <= now + REALTIME_TS_FUTURE_TOLERANCE {
                println!(
                    "normalize history ts gateway={} device={} raw={} adjusted={} reason=cached_offset",
                    gateway_name,
                    device_name,
                    format_timestamp(timestamp),
                    format_timestamp(shifted)
                );
                return shifted;
            }
        }
        if timestamp > now + REALTIME_TS_FUTURE_TOLERANCE {
            let shifted = timestamp - TIMEZONE_OFFSET_SECONDS;
            if shifted <= now + REALTIME_TS_FUTURE_TOLERANCE {
                store_gateway_ts_offset(gateway_name, -TIMEZONE_OFFSET_SECONDS).await;
                println!(
                    "normalize history ts gateway={} device={} raw={} adjusted={} reason=future_minus_8h",
                    gateway_name,
                    device_name,
                    format_timestamp(timestamp),
                    format_timestamp(shifted)
                );
                return shifted;
            }
        }
        return timestamp;
    }

    let raw_future = timestamp > now + REALTIME_TS_FUTURE_TOLERANCE;
    let raw_stale = timestamp < now - REALTIME_TS_PAST_WINDOW;
    let adjusted = best != timestamp && best <= now + REALTIME_TS_FUTURE_TOLERANCE;
    let significantly_better = best_delta + 300 < raw_delta;

    if adjusted
        && (raw_future
            || raw_stale
            || significantly_better
            || cached_offset.unwrap_or(0) == best_offset)
    {
        store_gateway_ts_offset(gateway_name, best_offset).await;
        println!(
            "normalize realtime ts gateway={} device={} raw={} adjusted={} offset={} topic={}",
            gateway_name,
            device_name,
            format_timestamp(timestamp),
            format_timestamp(best),
            best_offset,
            topic
        );
        return best;
    }

    timestamp
}

fn should_skip_device(energy_type: i32, data_map: &HashMap<String, f64>) -> bool {
    // Only apply the "all key fields are zero" filter to electric devices.
    if energy_type != 1 {
        return false;
    }

    let tracked_fields = [
        "uab", "ubc", "uac", "pw", "ua", "ub", "uc", "tp", "pr", "st", "fr",
    ];
    let sum_val: f64 = tracked_fields
        .iter()
        .map(|key| *data_map.get(*key).unwrap_or(&0.0))
        .sum();
    sum_val == 0.0
}

async fn write_realtime_data(
    device_name: &str,
    energy_type: i32,
    gateway_name: &str,
    timestamp: i64,
    data_map: &HashMap<String, f64>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let sql = build_insert_sql(device_name, energy_type, timestamp, data_map)?;
    let client = get_tdengine_client().ok_or_else(|| anyhow!("tdengine client not initialized"))?;

    match client.query(&sql).await {
        Ok(_) => Ok(()),
        Err(err) => {
            let error_msg = err.to_string();
            eprintln!("tdengine write failed for {}: {}", device_name, error_msg);

            if should_retry_after_reconnect(&error_msg) {
                TDENGINE_CONNECTED.store(false, std::sync::atomic::Ordering::SeqCst);
                match reconnect_tdengine().await {
                    Ok(client) => {
                        if let Err(retry_err) = client.query(&sql).await {
                            eprintln!(
                                "tdengine retry after reconnect failed for {}: {}",
                                device_name, retry_err
                            );
                            cache_data(device_name, energy_type, gateway_name, timestamp, data_map)
                                .await?;
                        }
                    }
                    Err(reconnect_err) => {
                        eprintln!("tdengine reconnect failed: {}", reconnect_err);
                        cache_data(device_name, energy_type, gateway_name, timestamp, data_map)
                            .await?;
                    }
                }
                return Ok(());
            }

            if should_cache_error(&error_msg) {
                cache_data(device_name, energy_type, gateway_name, timestamp, data_map).await?;
                return Ok(());
            }

            Err(Box::new(err))
        }
    }
}

fn should_retry_after_reconnect(error_msg: &str) -> bool {
    error_msg.contains("channel closed")
        || error_msg.contains("0xE003")
        || error_msg.contains("Connection refused")
        || error_msg.contains("Broken pipe")
        || error_msg.contains("not connected")
}

fn should_cache_error(error_msg: &str) -> bool {
    error_msg.contains("Sync leader is restoring")
        || error_msg.contains("0x0914")
        || should_retry_after_reconnect(error_msg)
}

fn build_insert_sql(
    device_name: &str,
    energy_type: i32,
    timestamp: i64,
    data_map: &HashMap<String, f64>,
) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    let table_name = tdengine::get_subtable_name(device_name)?;
    let stable_name = tdengine::get_origin_stable_name()?;

    let mut columns: Vec<&str> = data_map.keys().map(String::as_str).collect();
    columns.sort_unstable();
    let values = columns
        .iter()
        .map(|column| {
            data_map
                .get(*column)
                .copied()
                .unwrap_or_default()
                .to_string()
        })
        .collect::<Vec<_>>();

    Ok(format!(
        "INSERT INTO {} USING {} TAGS ('{}', '{}') (ts, {}) VALUES ({}, {})",
        table_name,
        stable_name,
        device_name,
        energy_type,
        columns.join(", "),
        timestamp * 1000,
        values.join(", ")
    ))
}

fn format_timestamp(timestamp: i64) -> String {
    use chrono::{DateTime, Local, Utc};

    match DateTime::<Utc>::from_timestamp(timestamp, 0) {
        Some(dt) => dt
            .with_timezone(&Local)
            .format("%Y-%m-%d %H:%M:%S")
            .to_string(),
        None => "invalid timestamp".to_string(),
    }
}

async fn cache_data(
    device_name: &str,
    energy_type: i32,
    gateway_name: &str,
    timestamp: i64,
    data_map: &HashMap<String, f64>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let cached_data = CachedData {
        id: Uuid::new_v4().to_string(),
        device_name: device_name.to_string(),
        energy_type,
        gateway_name: gateway_name.to_string(),
        timestamp,
        data_map: data_map.clone(),
        created_at: chrono::Utc::now().timestamp(),
    };

    let cache_key = format!("tdengine:cache:{}:{}", device_name, cached_data.id);
    let payload = serde_json::to_string(&cached_data)?;
    crate::redis::set_ex(&cache_key, &payload, CACHE_TTL_SECONDS).await?;
    crate::redis::lpush(CACHE_LIST_KEY, &cache_key).await?;
    Ok(())
}

async fn check_tdengine_available() -> bool {
    if let Some(client) = get_tdengine_client() {
        return client.query("SELECT server_status()").await.is_ok();
    }
    false
}

async fn check_cache_size() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let cache_keys: Vec<String> = crate::redis::lrange(CACHE_LIST_KEY, 0, -1)
        .await?
        .into_iter()
        .flatten()
        .collect();

    if cache_keys.len() > 1000 {
        eprintln!("tdengine cache backlog is high: {}", cache_keys.len());
    }

    let expired_threshold = chrono::Utc::now().timestamp() - CACHE_TTL_SECONDS as i64;
    for key in cache_keys {
        if let Some(json_data) = crate::redis::get_value(&key).await? {
            if let Ok(cached_data) = serde_json::from_str::<CachedData>(&json_data) {
                if cached_data.created_at < expired_threshold {
                    crate::redis::del(&key).await?;
                    crate::redis::lrem(CACHE_LIST_KEY, 1, &key).await?;
                }
            }
        }
    }

    Ok(())
}

pub async fn retry_cached_data() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    if let Err(err) = check_cache_size().await {
        eprintln!("check tdengine cache size failed: {}", err);
    }

    if !check_tdengine_available().await {
        return Ok(());
    }

    let cache_keys = crate::redis::lrange(CACHE_LIST_KEY, 0, BATCH_SIZE as isize - 1).await?;
    let client = match get_tdengine_client() {
        Some(client) => client,
        None => return Ok(()),
    };

    let mut success_count = 0usize;
    let mut fail_count = 0usize;

    for key in cache_keys.into_iter().flatten() {
        let Some(json_data) = crate::redis::get_value(&key).await? else {
            let _ = crate::redis::lrem(CACHE_LIST_KEY, 1, &key).await;
            continue;
        };

        let cached_data = match serde_json::from_str::<CachedData>(&json_data) {
            Ok(cached_data) => cached_data,
            Err(err) => {
                eprintln!("parse cached tdengine payload failed for {}: {}", key, err);
                let _ = crate::redis::del(&key).await;
                let _ = crate::redis::lrem(CACHE_LIST_KEY, 1, &key).await;
                continue;
            }
        };

        let sql = build_insert_sql(
            &cached_data.device_name,
            cached_data.energy_type,
            cached_data.timestamp,
            &cached_data.data_map,
        )?;

        match client.query(&sql).await {
            Ok(_) => {
                let _ = crate::redis::del(&key).await;
                let _ = crate::redis::lrem(CACHE_LIST_KEY, 1, &key).await;
                success_count += 1;
            }
            Err(err) => {
                fail_count += 1;
                eprintln!(
                    "retry cached tdengine write failed for {}: {}",
                    cached_data.device_name, err
                );
            }
        }
    }

    if success_count > 0 || fail_count > 0 {
        println!(
            "retry cached tdengine data finished: success={}, fail={}",
            success_count, fail_count
        );
    }

    Ok(())
}
