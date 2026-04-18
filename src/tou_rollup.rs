use anyhow::{anyhow, Context};
use chrono::{Duration as ChronoDuration, NaiveDate, NaiveDateTime, NaiveTime};
use futures::TryStreamExt;
use once_cell::sync::Lazy;
use serde::Deserialize;
use sqlx::Row;
use std::collections::{HashMap, HashSet};
use std::time::Duration;
use taos::{AsyncFetchable, AsyncQueryable};
use tokio::sync::Mutex;
use tokio::time::interval;

use crate::{conf, mysqldb, redis, tdengine};

const JOB_BATCH_SIZE: i64 = 10;
const ROLLUP_INSERT_BATCH_SIZE: usize = 200;
const WORKER_INTERVAL_SECS: u64 = 300;
const BILLING_UTC_OFFSET_HOURS: i64 = 8;
const DEFAULT_COMMAND_QUEUE_KEY: &str = "tou_rollup:command_queue";
const DEFAULT_COMMAND_RESULT_KEY: &str = "tou_rollup:last_result";
const DEFAULT_COMMAND_TIMEOUT_SECS: usize = 5;

static RUN_LOCK: Lazy<Mutex<()>> = Lazy::new(|| Mutex::new(()));

#[derive(Debug)]
struct RollupJob {
    id: i64,
    template_id: i64,
    template_version_hash: String,
    energy_type: i32,
    start_date: NaiveDate,
    end_date: NaiveDate,
}

#[derive(Debug, Deserialize)]
struct TemplatePeriod {
    period_key: String,
    start_hour: u32,
    start_minute: u32,
    end_hour: u32,
    end_minute: u32,
}

#[derive(Debug)]
struct RollupUsage {
    device: String,
    day: NaiveDate,
    period_key: String,
    usage: f64,
}

#[derive(Debug, Deserialize, Default)]
struct TouRollupCommandConfig {
    command_queue_key: Option<String>,
    command_result_key: Option<String>,
    command_timeout_seconds: Option<usize>,
}

#[derive(Debug, Deserialize)]
struct TouRollupCommand {
    action: Option<String>,
    request_id: Option<String>,
}

pub fn spawn_worker() {
    tokio::spawn(async {
        let mut ticker = interval(Duration::from_secs(WORKER_INTERVAL_SECS));
        loop {
            ticker.tick().await;
            if let Err(err) = run_once().await {
                eprintln!("tou rollup worker failed: {:#}", err);
            }
        }
    });
}

pub fn spawn_command_listener() {
    tokio::spawn(async {
        let config = load_command_config();
        loop {
            match redis::brpop(&config.command_queue_key, config.command_timeout_seconds).await {
                Ok(Some((_queue, payload))) => {
                    if let Err(err) = handle_command(&config, &payload).await {
                        eprintln!("tou rollup command failed: {:#}", err);
                    }
                }
                Ok(None) => {}
                Err(err) => {
                    eprintln!("tou rollup command listen failed: {}", err);
                    tokio::time::sleep(Duration::from_secs(3)).await;
                }
            }
        }
    });
}

pub async fn run_once() -> anyhow::Result<()> {
    let _guard = RUN_LOCK.lock().await;
    run_once_inner().await
}

async fn run_once_inner() -> anyhow::Result<()> {
    let pool = mysqldb::get_pool().await?;
    let stale_minutes = stale_running_job_minutes();
    let select_sql = format!(
        r#"
        SELECT id, template_id, template_version_hash, energy_type,
               DATE_FORMAT(start_date, '%Y-%m-%d') AS start_date,
               DATE_FORMAT(end_date, '%Y-%m-%d') AS end_date
        FROM tou_rollup_job
        WHERE (
            status = 'PENDING'
            OR (status = 'FAILED' AND (next_retry_at IS NULL OR next_retry_at <= NOW()))
            OR (status = 'RUNNING' AND locked_at < DATE_SUB(NOW(), INTERVAL {} MINUTE))
        )
        ORDER BY id ASC
        LIMIT ?
        "#,
        stale_minutes
    );
    let claim_sql = format!(
        r#"
        UPDATE tou_rollup_job
        SET status = 'RUNNING',
            locked_at = NOW(),
            attempts = attempts + 1,
            progress = 0,
            error_message = NULL
        WHERE id = ?
          AND (
              status = 'PENDING'
              OR (status = 'FAILED' AND (next_retry_at IS NULL OR next_retry_at <= NOW()))
              OR (status = 'RUNNING' AND locked_at < DATE_SUB(NOW(), INTERVAL {} MINUTE))
          )
        "#,
        stale_minutes
    );

    loop {
        let rows = sqlx::query(&select_sql)
            .bind(JOB_BATCH_SIZE)
            .fetch_all(&pool)
            .await?;
        if rows.is_empty() {
            break;
        }

        for row in rows {
            let id: i64 = row.try_get("id")?;
            let affected = sqlx::query(&claim_sql)
                .bind(id)
                .execute(&pool)
                .await?
                .rows_affected();
            if affected == 0 {
                continue;
            }

            let job = RollupJob {
                id,
                template_id: row.try_get("template_id")?,
                template_version_hash: row.try_get("template_version_hash")?,
                energy_type: row.try_get("energy_type")?,
                start_date: parse_date(row.try_get::<String, _>("start_date")?)?,
                end_date: parse_date(row.try_get::<String, _>("end_date")?)?,
            };

            if let Err(err) = process_job(&job).await {
                let message = truncate_error(&format!("{:#}", err));
                sqlx::query(
                    r#"
                    UPDATE tou_rollup_job
                    SET status = CASE WHEN attempts >= ? THEN 'PAUSED' ELSE 'FAILED' END,
                        locked_at = NULL,
                        error_message = ?,
                        next_retry_at = CASE WHEN attempts >= ? THEN NULL ELSE DATE_ADD(NOW(), INTERVAL 10 MINUTE) END
                    WHERE id = ?
                    "#,
                )
                .bind(max_retry_attempts())
                .bind(message)
                .bind(max_retry_attempts())
                .bind(job.id)
                .execute(&pool)
                .await?;
                continue;
            }

            sqlx::query(
                "UPDATE tou_rollup_job SET status = 'DONE', progress = 100, locked_at = NULL, error_message = NULL, next_retry_at = NULL WHERE id = ?",
            )
            .bind(job.id)
            .execute(&pool)
            .await?;
        }
    }

    Ok(())
}

async fn handle_command(
    config: &TouRollupCommandConfigResolved,
    payload: &str,
) -> anyhow::Result<()> {
    let command = parse_command(payload);
    let action = command
        .action
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or("run_once");
    if !action.eq_ignore_ascii_case("run_once") {
        write_command_result(
            config,
            "ignored",
            command.request_id.as_deref(),
            &format!("unsupported action: {}", action),
        )
        .await;
        return Ok(());
    }

    write_command_result(
        config,
        "running",
        command.request_id.as_deref(),
        "TOU rollup started",
    )
    .await;
    match run_once().await {
        Ok(_) => {
            write_command_result(
                config,
                "done",
                command.request_id.as_deref(),
                "TOU rollup finished",
            )
            .await;
            Ok(())
        }
        Err(err) => {
            let message = truncate_error(&format!("{:#}", err));
            write_command_result(config, "failed", command.request_id.as_deref(), &message).await;
            Err(anyhow!(message))
        }
    }
}

#[derive(Debug)]
struct TouRollupCommandConfigResolved {
    command_queue_key: String,
    command_result_key: String,
    command_timeout_seconds: usize,
}

fn load_command_config() -> TouRollupCommandConfigResolved {
    let raw = conf::load_config::<TouRollupCommandConfig>("TOU_ROLLUP").unwrap_or_default();
    TouRollupCommandConfigResolved {
        command_queue_key: raw
            .command_queue_key
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty())
            .unwrap_or_else(|| DEFAULT_COMMAND_QUEUE_KEY.to_string()),
        command_result_key: raw
            .command_result_key
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty())
            .unwrap_or_else(|| DEFAULT_COMMAND_RESULT_KEY.to_string()),
        command_timeout_seconds: raw
            .command_timeout_seconds
            .unwrap_or(DEFAULT_COMMAND_TIMEOUT_SECS),
    }
}

fn parse_command(payload: &str) -> TouRollupCommand {
    serde_json::from_str::<TouRollupCommand>(payload).unwrap_or_else(|_| TouRollupCommand {
        action: Some(payload.trim().to_string()),
        request_id: None,
    })
}

async fn write_command_result(
    config: &TouRollupCommandConfigResolved,
    status: &str,
    request_id: Option<&str>,
    message: &str,
) {
    let payload = serde_json::json!({
        "status": status,
        "request_id": request_id.unwrap_or(""),
        "message": message,
        "updated_at": chrono::Local::now().format("%Y-%m-%d %H:%M:%S").to_string(),
    });
    if let Err(err) = redis::set_value(&config.command_result_key, &payload.to_string()).await {
        eprintln!("write tou rollup command result failed: {}", err);
    }
}

async fn process_job(job: &RollupJob) -> anyhow::Result<()> {
    if job.end_date < job.start_date {
        return Err(anyhow!("invalid job date range"));
    }

    let periods = load_template_periods(job).await?;
    if periods.is_empty() {
        return Err(anyhow!("template version has no TOU periods"));
    }

    let devices: HashSet<String> = load_energy_devices(job.energy_type)
        .await?
        .into_iter()
        .collect();
    if devices.is_empty() {
        println!(
            "tou rollup job {} skipped: no devices for energy_type={}",
            job.id, job.energy_type
        );
        return Ok(());
    }

    let client = tdengine::get_tdengine_client()
        .ok_or_else(|| anyhow!("TDengine client not initialized"))?;
    let hour_stable = tdengine::get_hour_stable_name()?;
    let tou_stable = tdengine::get_tou_daily_stable_name()?;

    let mut current = job.start_date;
    let mut processed_days = 0;
    let mut pending_usages: Vec<RollupUsage> = Vec::with_capacity(ROLLUP_INSERT_BATCH_SIZE);
    while current <= job.end_date {
        let day_usages = query_day_usage_by_period(
            client.as_ref(),
            &hour_stable,
            &devices,
            job.energy_type,
            current,
            &periods,
        )
        .await?;
        for usage in day_usages {
            pending_usages.push(usage);
        }
        if pending_usages.len() >= ROLLUP_INSERT_BATCH_SIZE {
            flush_rollup_usages(client.as_ref(), &tou_stable, job, &mut pending_usages).await?;
        }
        flush_rollup_usages(client.as_ref(), &tou_stable, job, &mut pending_usages).await?;
        processed_days += 1;
        update_progress(job.id, processed_days, total_days(job)).await?;
        current += ChronoDuration::days(1);
    }

    Ok(())
}

async fn load_template_periods(job: &RollupJob) -> anyhow::Result<Vec<TemplatePeriod>> {
    let pool = mysqldb::get_pool().await?;
    let row = sqlx::query(
        "SELECT period_config_json FROM tou_template_version WHERE template_id = ? AND version_hash = ? AND status = 1 LIMIT 1",
    )
    .bind(job.template_id)
    .bind(&job.template_version_hash)
    .fetch_optional(&pool)
    .await?;

    let Some(row) = row else {
        return Err(anyhow!(
            "template version not found: template_id={}, hash={}",
            job.template_id,
            job.template_version_hash
        ));
    };

    let raw: String = row.try_get("period_config_json")?;
    let periods: Vec<TemplatePeriod> =
        serde_json::from_str(&raw).context("parse period_config_json")?;
    Ok(periods)
}

async fn load_energy_devices(energy_type: i32) -> anyhow::Result<Vec<String>> {
    let pool = mysqldb::get_pool().await?;
    let rows = sqlx::query("SELECT name FROM device WHERE energy_type = ?")
        .bind(energy_type)
        .fetch_all(&pool)
        .await?;
    let mut devices = Vec::with_capacity(rows.len());
    for row in rows {
        let name: String = row.try_get("name")?;
        if !name.trim().is_empty() {
            devices.push(name);
        }
    }
    Ok(devices)
}

async fn query_day_usage_by_period(
    client: &taos::Taos,
    hour_stable: &str,
    devices: &HashSet<String>,
    energy_type: i32,
    day: NaiveDate,
    periods: &[TemplatePeriod],
) -> anyhow::Result<Vec<RollupUsage>> {
    let start = day.and_time(NaiveTime::from_hms_opt(0, 0, 0).expect("midnight is valid"));
    let end = start + ChronoDuration::days(1);

    let sql = format!(
        "SELECT CAST(ts AS VARCHAR(64)), device_id, CAST(energy AS VARCHAR(64)) FROM {} WHERE energy_type = {} AND ts >= {} AND ts < {}",
        hour_stable,
        quote_tdengine_string(&energy_type.to_string()),
        quote_tdengine_string(&format_tdengine_query_ts(start)),
        quote_tdengine_string(&format_tdengine_query_ts(end)),
    );
    let mut rows = client.query(sql).await?;
    let records: Vec<(String, String, String)> = rows.deserialize().try_collect().await?;
    let mut usage_by_device_period: HashMap<(String, String), f64> = HashMap::new();
    for (raw_ts, device, raw_usage) in records {
        if !devices.contains(&device) {
            continue;
        }
        let bucket_start = parse_tdengine_ts(&raw_ts)?;
        let usage = raw_usage.trim().parse::<f64>().with_context(|| {
            format!(
                "parse TDengine hourly usage failed: device={}, energy_type={}, day={}, ts={}, raw={}",
                device, energy_type, day, bucket_start, raw_usage
            )
        })?;
        if usage <= 0.0 {
            continue;
        }
        let bucket_end = bucket_start + ChronoDuration::hours(1);
        for period in periods {
            let overlap = period_overlap_seconds(bucket_start, bucket_end, period)?;
            if overlap <= 0 {
                continue;
            }
            let period_usage = usage * overlap as f64 / 3600.0;
            let key = (device.clone(), period.period_key.clone());
            *usage_by_device_period.entry(key).or_insert(0.0) += period_usage;
        }
    }

    let mut usages = Vec::with_capacity(usage_by_device_period.len());
    for ((device, period_key), usage) in usage_by_device_period {
        if usage <= 0.0 {
            continue;
        }
        usages.push(RollupUsage {
            device,
            day,
            period_key,
            usage,
        });
    }
    Ok(usages)
}

fn period_overlap_seconds(
    bucket_start: NaiveDateTime,
    bucket_end: NaiveDateTime,
    period: &TemplatePeriod,
) -> anyhow::Result<i64> {
    let base_day = bucket_start.date() - ChronoDuration::days(1);
    let mut total = 0;
    for offset in 0..=2 {
        let day = base_day + ChronoDuration::days(offset);
        let period_start = day.and_time(
            NaiveTime::from_hms_opt(period.start_hour, period.start_minute, 0)
                .ok_or_else(|| anyhow!("invalid period start time: {}", period.period_key))?,
        );
        let mut period_end = day.and_time(
            NaiveTime::from_hms_opt(period.end_hour, period.end_minute, 0)
                .ok_or_else(|| anyhow!("invalid period end time: {}", period.period_key))?,
        );
        if period_end <= period_start {
            period_end += ChronoDuration::days(1);
        }
        let overlap_start = if bucket_start > period_start {
            bucket_start
        } else {
            period_start
        };
        let overlap_end = if bucket_end < period_end {
            bucket_end
        } else {
            period_end
        };
        if overlap_end > overlap_start {
            total += (overlap_end - overlap_start).num_seconds();
        }
    }
    Ok(total)
}

async fn flush_rollup_usages(
    client: &taos::Taos,
    tou_stable: &str,
    job: &RollupJob,
    usages: &mut Vec<RollupUsage>,
) -> anyhow::Result<()> {
    if usages.is_empty() {
        return Ok(());
    }

    let now = chrono::Local::now().naive_local();
    let mut sql = String::from("INSERT INTO ");
    for item in usages.iter() {
        let table_name = rollup_subtable_name(
            &item.device,
            job.energy_type,
            job.template_id,
            &job.template_version_hash,
            &item.period_key,
        );
        let ts = item
            .day
            .and_time(NaiveTime::from_hms_opt(0, 0, 0).expect("midnight is valid"));
        sql.push_str(&format!(
            "{} USING {} TAGS ({}, {}, {}, {}, {}) (ts, usage, calculated_at, source_updated_at) VALUES ({}, {:.6}, {}, {}) ",
            table_name,
            tou_stable,
            quote_tdengine_string(&item.device),
            quote_tdengine_string(&job.energy_type.to_string()),
            job.template_id,
            quote_tdengine_string(&job.template_version_hash),
            quote_tdengine_string(&item.period_key),
            quote_tdengine_string(&format_ts(ts)),
            item.usage,
            quote_tdengine_string(&format_ts(now)),
            quote_tdengine_string(&format_ts(now)),
        ));
    }
    client.query(sql).await?;
    usages.clear();
    Ok(())
}

async fn update_progress(job_id: i64, processed_days: i64, total_days: i64) -> anyhow::Result<()> {
    if total_days <= 0 {
        return Ok(());
    }
    let progress = ((processed_days * 100) / total_days).clamp(0, 99);
    let pool = mysqldb::get_pool().await?;
    sqlx::query("UPDATE tou_rollup_job SET progress = ? WHERE id = ? AND status = 'RUNNING'")
        .bind(progress)
        .bind(job_id)
        .execute(&pool)
        .await?;
    Ok(())
}

fn parse_date(raw: String) -> anyhow::Result<NaiveDate> {
    NaiveDate::parse_from_str(raw.trim(), "%Y-%m-%d").map_err(|err| err.into())
}

fn total_days(job: &RollupJob) -> i64 {
    (job.end_date - job.start_date).num_days() + 1
}

fn format_ts(ts: NaiveDateTime) -> String {
    ts.format("%Y-%m-%d %H:%M:%S").to_string()
}

fn parse_tdengine_ts(raw: &str) -> anyhow::Result<NaiveDateTime> {
    let trimmed = raw.trim().trim_matches('\'').trim_matches('"');
    if let Some(unix_ts) = parse_unix_timestamp(trimmed) {
        let utc = if unix_ts >= 1_000_000_000_000 {
            chrono::DateTime::from_timestamp_millis(unix_ts)
                .ok_or_else(|| anyhow!("invalid TDengine timestamp millis: raw={}", raw))?
                .naive_utc()
        } else {
            chrono::DateTime::from_timestamp(unix_ts, 0)
                .ok_or_else(|| anyhow!("invalid TDengine timestamp seconds: raw={}", raw))?
                .naive_utc()
        };
        return Ok(utc + ChronoDuration::hours(BILLING_UTC_OFFSET_HOURS));
    }

    let prefix = trimmed.get(0..19).unwrap_or(trimmed);
    NaiveDateTime::parse_from_str(prefix, "%Y-%m-%d %H:%M:%S")
        .with_context(|| format!("parse TDengine timestamp failed: raw={}", raw))
}

fn parse_unix_timestamp(raw: &str) -> Option<i64> {
    let numeric_prefix: String = raw
        .chars()
        .take_while(|ch| ch.is_ascii_digit() || *ch == '.')
        .collect();
    if numeric_prefix.is_empty() {
        return None;
    }

    let integer_part = numeric_prefix.split('.').next()?.trim();
    if integer_part.len() < 10 || !integer_part.chars().all(|ch| ch.is_ascii_digit()) {
        return None;
    }

    integer_part.parse::<i64>().ok()
}

fn format_tdengine_query_ts(local_ts: NaiveDateTime) -> String {
    format_ts(local_ts - ChronoDuration::hours(BILLING_UTC_OFFSET_HOURS))
}

fn quote_tdengine_string(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

fn rollup_subtable_name(
    device: &str,
    energy_type: i32,
    template_id: i64,
    version_hash: &str,
    period_key: &str,
) -> String {
    let raw = format!(
        "{}|{}|{}|{}|{}",
        device, energy_type, template_id, version_hash, period_key
    );
    format!("tou_{:x}", md5::compute(raw.as_bytes()))
}

fn truncate_error(value: &str) -> String {
    const LIMIT: usize = 2000;
    if value.len() <= LIMIT {
        return value.to_string();
    }
    value.chars().take(LIMIT).collect()
}

fn stale_running_job_minutes() -> i64 {
    std::env::var("TOU_ROLLUP_STALE_MINUTES")
        .ok()
        .and_then(|raw| raw.trim().parse::<i64>().ok())
        .filter(|value| (1..=1440).contains(value))
        .unwrap_or(30)
}

fn max_retry_attempts() -> i32 {
    std::env::var("TOU_ROLLUP_MAX_RETRY_ATTEMPTS")
        .ok()
        .and_then(|raw| raw.trim().parse::<i32>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(3)
}
