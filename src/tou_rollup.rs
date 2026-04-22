use anyhow::{anyhow, Context};
use chrono::{Duration as ChronoDuration, NaiveDate, NaiveDateTime, NaiveTime, Timelike};
use futures::TryStreamExt;
use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize};
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
const ROLLUP_VERIFIED_MARKER: &str = "verified:v2";
const ROLLUP_QUERY_CHUNK_DAYS: i64 = 31;
const ROLLUP_JOB_WINDOW_DAYS: i64 = 31;
const ROLLUP_DROP_TABLE_BATCH_SIZE: usize = 200;
const DEFAULT_CLEANUP_HOUR: u32 = 3;
const DEFAULT_CLEANUP_RETENTION_DAYS: i64 = 7;

static RUN_LOCK: Lazy<Mutex<()>> = Lazy::new(|| Mutex::new(()));
static CLEANUP_LOCK: Lazy<Mutex<Option<NaiveDate>>> = Lazy::new(|| Mutex::new(None));
static SYNC_LOCK: Lazy<Mutex<()>> = Lazy::new(|| Mutex::new(()));

#[derive(Debug)]
struct RollupJob {
    id: i64,
    template_id: i64,
    template_version_hash: String,
    rollup_generation: String,
    energy_type: i32,
    start_date: NaiveDate,
    end_date: NaiveDate,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
struct TemplatePeriod {
    period_key: String,
    start_hour: u32,
    start_minute: u32,
    end_hour: u32,
    end_minute: u32,
}

#[derive(Debug)]
struct TemplatePeriodVersionRow {
    template_id: i64,
    start_at: NaiveDateTime,
    end_at: Option<NaiveDateTime>,
    period_key: String,
    start_hour: u32,
    start_minute: u32,
    end_hour: u32,
    end_minute: u32,
    period_order: i32,
}

#[derive(Debug)]
struct TemplateVersionRecord {
    template_id: i64,
    version_hash: String,
    effective_start: NaiveDate,
    effective_end: Option<NaiveDate>,
    period_config_json: String,
}

#[derive(Debug)]
struct RollupJobCandidate {
    template_id: i64,
    template_version_hash: String,
    energy_type: i32,
    start_date: NaiveDate,
    end_date: NaiveDate,
}

#[derive(Debug)]
struct RollupUsage {
    device: String,
    day: NaiveDate,
    period_key: String,
    usage: f64,
}

#[derive(Debug, Default)]
struct RollupStats {
    usage_total: f64,
    row_count: i64,
}

#[derive(Debug)]
struct CleanupJob {
    id: i64,
    template_id: i64,
    template_version_hash: String,
    rollup_generation: String,
    energy_type: i32,
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
            if let Err(err) = run_cleanup_if_due().await {
                eprintln!("tou rollup cleanup failed: {:#}", err);
            }
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
    sync_tou_metadata_and_jobs().await?;

    let pool = mysqldb::get_pool().await?;
    let stale_minutes = stale_running_job_minutes();
    let select_sql = format!(
        r#"
        SELECT id, template_id, template_version_hash, COALESCE(rollup_generation, '') AS rollup_generation, energy_type,
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
            rollup_generation = COALESCE(NULLIF(rollup_generation, ''), ?),
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
            let raw_generation: String = row.try_get("rollup_generation")?;
            let rollup_generation = if raw_generation.trim().is_empty() {
                generate_rollup_generation(id)
            } else {
                raw_generation
            };
            let affected = sqlx::query(&claim_sql)
                .bind(&rollup_generation)
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
                rollup_generation,
                energy_type: row.try_get("energy_type")?,
                start_date: parse_date(row.try_get::<String, _>("start_date")?)?,
                end_date: parse_date(row.try_get::<String, _>("end_date")?)?,
            };

            let stats = match process_job(&job).await {
                Ok(stats) => stats,
                Err(err) => {
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
            };

            sqlx::query(
                "UPDATE tou_rollup_job SET status = 'DONE', progress = 100, locked_at = NULL, error_message = ?, next_retry_at = NULL, verified_at = NOW(), usage_total = ?, row_count = ? WHERE id = ?",
            )
            .bind(ROLLUP_VERIFIED_MARKER)
            .bind(stats.usage_total)
            .bind(stats.row_count)
            .bind(job.id)
            .execute(&pool)
            .await?;
        }
    }

    Ok(())
}

async fn sync_tou_metadata_and_jobs() -> anyhow::Result<()> {
    let _guard = SYNC_LOCK.lock().await;
    let pool = mysqldb::get_pool().await?;

    let synced_versions = sync_template_versions(&pool).await?;
    let (created_jobs, reset_jobs) = sync_rollup_jobs(&pool).await?;

    if synced_versions > 0 || created_jobs > 0 || reset_jobs > 0 {
        println!(
            "tou rollup sync finished: synced_versions={}, created_jobs={}, reset_jobs={}",
            synced_versions, created_jobs, reset_jobs
        );
    }

    Ok(())
}

async fn sync_template_versions(pool: &sqlx::MySqlPool) -> anyhow::Result<usize> {
    let rows = sqlx::query(
        r#"
        SELECT
            tt.id AS template_id,
            DATE_FORMAT(ttp.start_time, '%Y-%m-%d %H:%i:%s') AS version_start,
            COALESCE(DATE_FORMAT(ttp.end_time, '%Y-%m-%d %H:%i:%s'), '') AS version_end,
            ttp.period_key,
            CAST(ttp.start_hour AS UNSIGNED) AS start_hour,
            CAST(ttp.start_minute AS UNSIGNED) AS start_minute,
            CAST(ttp.end_hour AS UNSIGNED) AS end_hour,
            CAST(ttp.end_minute AS UNSIGNED) AS end_minute,
            ttp.period_order
        FROM time_template tt
        INNER JOIN time_template_period ttp ON ttp.template_id = tt.id
        WHERE tt.template_type = 'TOU'
        ORDER BY tt.id ASC, ttp.start_time ASC, ttp.end_time ASC, ttp.period_order ASC, ttp.id ASC
        "#,
    )
    .fetch_all(pool)
    .await?;

    let mut grouped: HashMap<(i64, String, String), Vec<TemplatePeriodVersionRow>> = HashMap::new();
    for row in rows {
        let template_id: i64 = row.try_get("template_id")?;
        let version_start: String = row.try_get("version_start")?;
        let version_end: String = row.try_get("version_end")?;
        let start_at = parse_mysql_naive_datetime(&version_start)?;
        let end_at = if version_end.trim().is_empty() {
            None
        } else {
            Some(parse_mysql_naive_datetime(&version_end)?)
        };

        grouped
            .entry((template_id, version_start, version_end))
            .or_default()
            .push(TemplatePeriodVersionRow {
                template_id,
                start_at,
                end_at,
                period_key: row.try_get("period_key")?,
                start_hour: row.try_get("start_hour")?,
                start_minute: row.try_get("start_minute")?,
                end_hour: row.try_get("end_hour")?,
                end_minute: row.try_get("end_minute")?,
                period_order: row.try_get("period_order")?,
            });
    }

    let mut upserted = 0usize;
    for ((_template_id, _start, _end), mut rows) in grouped {
        rows.sort_by(|left, right| {
            left.period_order
                .cmp(&right.period_order)
                .then_with(|| left.period_key.cmp(&right.period_key))
        });

        let periods: Vec<TemplatePeriod> = rows
            .iter()
            .map(|row| TemplatePeriod {
                period_key: row.period_key.clone(),
                start_hour: row.start_hour,
                start_minute: row.start_minute,
                end_hour: row.end_hour,
                end_minute: row.end_minute,
            })
            .collect();

        if periods.is_empty() {
            continue;
        }

        let first = rows
            .first()
            .ok_or_else(|| anyhow!("template version group is unexpectedly empty"))?;
        let period_config_json = serde_json::to_string(&periods)?;
        let version_hash = format!(
            "{:x}",
            md5::compute(
                format!(
                    "{}|{}|{}|{}",
                    first.template_id,
                    first.start_at.format("%Y-%m-%d %H:%M:%S"),
                    first
                        .end_at
                        .map(|value| value.format("%Y-%m-%d %H:%M:%S").to_string())
                        .unwrap_or_default(),
                    period_config_json
                )
                .as_bytes()
            )
        );

        let record = TemplateVersionRecord {
            template_id: first.template_id,
            version_hash,
            effective_start: first.start_at.date(),
            effective_end: first.end_at.map(resolve_effective_end_date),
            period_config_json,
        };

        sqlx::query(
            r#"
            INSERT INTO tou_template_version (
                template_id,
                version_hash,
                effective_start,
                effective_end,
                period_config_json,
                status
            ) VALUES (?, ?, ?, ?, ?, 1)
            ON DUPLICATE KEY UPDATE
                effective_start = VALUES(effective_start),
                effective_end = VALUES(effective_end),
                period_config_json = VALUES(period_config_json),
                status = 1,
                updated_at = NOW()
            "#,
        )
        .bind(record.template_id)
        .bind(&record.version_hash)
        .bind(record.effective_start.format("%Y-%m-%d").to_string())
        .bind(
            record
                .effective_end
                .map(|value| value.format("%Y-%m-%d").to_string()),
        )
        .bind(&record.period_config_json)
        .execute(pool)
        .await?;
        upserted += 1;
    }

    Ok(upserted)
}

async fn sync_rollup_jobs(pool: &sqlx::MySqlPool) -> anyhow::Result<(usize, usize)> {
    let rows = sqlx::query(
        r#"
        SELECT DISTINCT
            tv.template_id AS template_id,
            tv.version_hash AS template_version_hash,
            ps.energy_type_id AS energy_type,
            DATE_FORMAT(
                LEAST(
                    GREATEST(tv.effective_start, pa.start_date),
                    CURRENT_DATE()
                ),
                '%Y-%m-%d'
            ) AS overlap_start,
            DATE_FORMAT(
                LEAST(
                    COALESCE(tv.effective_end, '2099-12-31'),
                    COALESCE(pa.end_date, '2099-12-31'),
                    CURRENT_DATE()
                ),
                '%Y-%m-%d'
            ) AS overlap_end
        FROM tou_template_version tv
        INNER JOIN price_scheme ps
            ON ps.time_template_id = tv.template_id
           AND ps.billing_mode = 'TOU'
           AND ps.status = 1
        INNER JOIN price_assignment pa
            ON pa.scheme_id = ps.id
           AND pa.status = 1
        WHERE tv.status = 1
          AND LEAST(
                GREATEST(tv.effective_start, pa.start_date),
                CURRENT_DATE()
          ) <= LEAST(
                COALESCE(tv.effective_end, '2099-12-31'),
                COALESCE(pa.end_date, '2099-12-31'),
                CURRENT_DATE()
          )
        "#,
    )
    .fetch_all(pool)
    .await?;

    let mut created = 0usize;
    let mut reset = 0usize;
    for row in rows {
        let candidate = RollupJobCandidate {
            template_id: row.try_get("template_id")?,
            template_version_hash: row.try_get("template_version_hash")?,
            energy_type: row.try_get("energy_type")?,
            start_date: parse_date(row.try_get::<String, _>("overlap_start")?)?,
            end_date: parse_date(row.try_get::<String, _>("overlap_end")?)?,
        };

        if candidate.end_date < candidate.start_date {
            continue;
        }

        let exact_candidate = sqlx::query(
            r#"
            SELECT id, status
            FROM tou_rollup_job
            WHERE template_id = ?
              AND template_version_hash = ?
              AND energy_type = ?
              AND start_date = ?
              AND end_date = ?
            ORDER BY id DESC
            LIMIT 1
            "#,
        )
        .bind(candidate.template_id)
        .bind(&candidate.template_version_hash)
        .bind(candidate.energy_type)
        .bind(candidate.start_date.format("%Y-%m-%d").to_string())
        .bind(candidate.end_date.format("%Y-%m-%d").to_string())
        .fetch_optional(pool)
        .await?;

        if let Some(existing) = exact_candidate {
            let existing_id: i64 = existing.try_get("id")?;
            let normalized_status = existing
                .try_get::<String, _>("status")?
                .trim()
                .to_ascii_uppercase();
            if matches!(normalized_status.as_str(), "FAILED" | "PAUSED" | "CLEANED") {
                sqlx::query(
                    r#"
                    UPDATE tou_rollup_job
                    SET status = 'PENDING',
                        progress = 0,
                        error_message = NULL,
                        next_retry_at = NULL,
                        locked_at = NULL,
                        rollup_generation = NULL,
                        verified_at = NULL,
                        usage_total = NULL,
                        row_count = NULL,
                        updated_at = NOW()
                    WHERE id = ?
                    "#,
                )
                .bind(existing_id)
                .execute(pool)
                .await?;
                reset += 1;
            }
            continue;
        }

        let coverage_row = sqlx::query(
            r#"
            SELECT DATE_FORMAT(MAX(LEAST(end_date, ?)), '%Y-%m-%d') AS latest_covered_end
            FROM tou_rollup_job
            WHERE template_id = ?
              AND template_version_hash = ?
              AND energy_type = ?
              AND start_date <= ?
            "#,
        )
        .bind(candidate.end_date.format("%Y-%m-%d").to_string())
        .bind(candidate.template_id)
        .bind(&candidate.template_version_hash)
        .bind(candidate.energy_type)
        .bind(candidate.end_date.format("%Y-%m-%d").to_string())
        .fetch_one(pool)
        .await?;

        let latest_covered_end = coverage_row
            .try_get::<Option<String>, _>("latest_covered_end")?
            .map(parse_date)
            .transpose()?;

        let mut next_start = latest_covered_end
            .map(|value| std::cmp::max(candidate.start_date, value + ChronoDuration::days(1)))
            .unwrap_or(candidate.start_date);
        while next_start <= candidate.end_date {
            let chunk_end = std::cmp::min(
                candidate.end_date,
                next_start + ChronoDuration::days(ROLLUP_JOB_WINDOW_DAYS - 1),
            );

            let exact_existing = sqlx::query(
                r#"
                SELECT id, status
                FROM tou_rollup_job
                WHERE template_id = ?
                  AND template_version_hash = ?
                  AND energy_type = ?
                  AND start_date = ?
                  AND end_date = ?
                ORDER BY id DESC
                LIMIT 1
                "#,
            )
            .bind(candidate.template_id)
            .bind(&candidate.template_version_hash)
            .bind(candidate.energy_type)
            .bind(next_start.format("%Y-%m-%d").to_string())
            .bind(chunk_end.format("%Y-%m-%d").to_string())
            .fetch_optional(pool)
            .await?;

            if let Some(existing) = exact_existing {
                let existing_id: i64 = existing.try_get("id")?;
                let normalized_status = existing
                    .try_get::<String, _>("status")?
                    .trim()
                    .to_ascii_uppercase();
                if matches!(normalized_status.as_str(), "FAILED" | "PAUSED" | "CLEANED") {
                    sqlx::query(
                        r#"
                        UPDATE tou_rollup_job
                        SET status = 'PENDING',
                            progress = 0,
                            error_message = NULL,
                            next_retry_at = NULL,
                            locked_at = NULL,
                            rollup_generation = NULL,
                            verified_at = NULL,
                            usage_total = NULL,
                            row_count = NULL,
                            updated_at = NOW()
                        WHERE id = ?
                        "#,
                    )
                    .bind(existing_id)
                    .execute(pool)
                    .await?;
                    reset += 1;
                }
            } else {
                let result = sqlx::query(
                    r#"
                    INSERT INTO tou_rollup_job (
                        template_id,
                        template_version_hash,
                        energy_type,
                        start_date,
                        end_date,
                        status,
                        progress,
                        attempts
                    ) VALUES (?, ?, ?, ?, ?, 'PENDING', 0, 0)
                    ON DUPLICATE KEY UPDATE
                        status = CASE
                            WHEN UPPER(TRIM(status)) IN ('FAILED', 'PAUSED', 'CLEANED') THEN 'PENDING'
                            ELSE status
                        END,
                        progress = CASE
                            WHEN UPPER(TRIM(status)) IN ('FAILED', 'PAUSED', 'CLEANED') THEN 0
                            ELSE progress
                        END,
                        error_message = CASE
                            WHEN UPPER(TRIM(status)) IN ('FAILED', 'PAUSED', 'CLEANED') THEN NULL
                            ELSE error_message
                        END,
                        next_retry_at = CASE
                            WHEN UPPER(TRIM(status)) IN ('FAILED', 'PAUSED', 'CLEANED') THEN NULL
                            ELSE next_retry_at
                        END,
                        locked_at = CASE
                            WHEN UPPER(TRIM(status)) IN ('FAILED', 'PAUSED', 'CLEANED') THEN NULL
                            ELSE locked_at
                        END,
                        rollup_generation = CASE
                            WHEN UPPER(TRIM(status)) IN ('FAILED', 'PAUSED', 'CLEANED') THEN NULL
                            ELSE rollup_generation
                        END,
                        verified_at = CASE
                            WHEN UPPER(TRIM(status)) IN ('FAILED', 'PAUSED', 'CLEANED') THEN NULL
                            ELSE verified_at
                        END,
                        usage_total = CASE
                            WHEN UPPER(TRIM(status)) IN ('FAILED', 'PAUSED', 'CLEANED') THEN NULL
                            ELSE usage_total
                        END,
                        row_count = CASE
                            WHEN UPPER(TRIM(status)) IN ('FAILED', 'PAUSED', 'CLEANED') THEN NULL
                            ELSE row_count
                        END,
                        updated_at = NOW()
                    "#,
                )
                .bind(candidate.template_id)
                .bind(&candidate.template_version_hash)
                .bind(candidate.energy_type)
                .bind(next_start.format("%Y-%m-%d").to_string())
                .bind(chunk_end.format("%Y-%m-%d").to_string())
                .execute(pool)
                .await?;
                if result.rows_affected() == 1 {
                    created += 1;
                } else if result.rows_affected() > 1 {
                    reset += 1;
                }
            }

            next_start = chunk_end + ChronoDuration::days(1);
        }
    }

    Ok((created, reset))
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

async fn process_job(job: &RollupJob) -> anyhow::Result<RollupStats> {
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
        return Ok(RollupStats::default());
    }

    let client = tdengine::get_tdengine_client()
        .ok_or_else(|| anyhow!("TDengine client not initialized"))?;
    let hour_stable = tdengine::get_hour_stable_name()?;
    let tou_stable = tdengine::get_tou_daily_stable_name()?;

    let mut stats = RollupStats::default();
    let mut processed_days = 0;
    let mut pending_usages: Vec<RollupUsage> = Vec::with_capacity(ROLLUP_INSERT_BATCH_SIZE);
    let mut chunk_start = job.start_date;
    while chunk_start <= job.end_date {
        let chunk_end = std::cmp::min(
            chunk_start + ChronoDuration::days(ROLLUP_QUERY_CHUNK_DAYS - 1),
            job.end_date,
        );
        let range_usages = query_range_usage_by_period(
            client.as_ref(),
            &hour_stable,
            &devices,
            job.energy_type,
            chunk_start,
            chunk_end,
            &periods,
        )
        .await?;
        for usage in range_usages {
            stats.usage_total += usage.usage;
            stats.row_count += 1;
            pending_usages.push(usage);
            if pending_usages.len() >= ROLLUP_INSERT_BATCH_SIZE {
                flush_rollup_usages(client.as_ref(), &tou_stable, job, &mut pending_usages).await?;
            }
        }
        flush_rollup_usages(client.as_ref(), &tou_stable, job, &mut pending_usages).await?;
        processed_days += (chunk_end - chunk_start).num_days() + 1;
        update_progress(job.id, processed_days, total_days(job)).await?;
        chunk_start = chunk_end + ChronoDuration::days(1);
    }

    Ok(stats)
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

async fn query_range_usage_by_period(
    client: &taos::Taos,
    hour_stable: &str,
    devices: &HashSet<String>,
    energy_type: i32,
    start_day: NaiveDate,
    end_day: NaiveDate,
    periods: &[TemplatePeriod],
) -> anyhow::Result<Vec<RollupUsage>> {
    let start = start_day.and_time(NaiveTime::from_hms_opt(0, 0, 0).expect("midnight is valid"));
    let end = (end_day + ChronoDuration::days(1))
        .and_time(NaiveTime::from_hms_opt(0, 0, 0).expect("midnight is valid"));

    let sql = format!(
		"SELECT CAST(ts AS VARCHAR(64)), device_id, CAST(energy AS VARCHAR(64)) FROM {} WHERE energy_type = {} AND ts >= {} AND ts < {}",
		hour_stable,
        quote_tdengine_string(&energy_type.to_string()),
        quote_tdengine_string(&format_tdengine_query_ts(start)),
        quote_tdengine_string(&format_tdengine_query_ts(end)),
    );
    let mut rows = client.query(sql).await?;
    let records: Vec<(String, String, String)> = rows.deserialize().try_collect().await?;
    let mut usage_by_device_period: HashMap<(String, NaiveDate, String), f64> = HashMap::new();
    for (raw_ts, device, raw_usage) in records {
        if !devices.contains(&device) {
            continue;
        }
        let bucket_start = parse_tdengine_ts(&raw_ts)?;
        let day = bucket_start.date();
        let usage = raw_usage.trim().parse::<f64>().with_context(|| {
            format!(
                "parse TDengine hourly usage failed: device={}, energy_type={}, day={}, ts={}, raw={}",
                device, energy_type, day, bucket_start, raw_usage
            )
        })?;
        if usage <= 0.0 {
            continue;
        }
        if day < start_day || day > end_day {
            continue;
        }
        let bucket_end = bucket_start + ChronoDuration::hours(1);
        for period in periods {
            let overlap = period_overlap_seconds(bucket_start, bucket_end, period)?;
            if overlap <= 0 {
                continue;
            }
            let period_usage = usage * overlap as f64 / 3600.0;
            let key = (device.clone(), day, period.period_key.clone());
            *usage_by_device_period.entry(key).or_insert(0.0) += period_usage;
        }
    }

    let mut usages = Vec::with_capacity(usage_by_device_period.len());
    for ((device, day, period_key), usage) in usage_by_device_period {
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

async fn run_cleanup_if_due() -> anyhow::Result<()> {
    let now = chrono::Local::now();
    if now.hour() != cleanup_hour() {
        return Ok(());
    }

    let today = now.date_naive();
    let mut last_cleanup = CLEANUP_LOCK.lock().await;
    if last_cleanup.as_ref() == Some(&today) {
        return Ok(());
    }
    *last_cleanup = Some(today);
    drop(last_cleanup);

    cleanup_old_rollup_generations().await
}

async fn cleanup_old_rollup_generations() -> anyhow::Result<()> {
    let pool = mysqldb::get_pool().await?;
    let retention_days = cleanup_retention_days();
    let rows = sqlx::query(
        r#"
        SELECT j.id, j.template_id, j.template_version_hash, j.rollup_generation, j.energy_type
        FROM tou_rollup_job j
        WHERE j.status = 'DONE'
          AND j.verified_at IS NOT NULL
          AND j.rollup_generation IS NOT NULL
          AND j.rollup_generation <> ''
          AND j.verified_at < DATE_SUB(NOW(), INTERVAL ? DAY)
          AND EXISTS (
              SELECT 1
              FROM tou_rollup_job newer
              WHERE newer.template_id = j.template_id
                AND newer.template_version_hash = j.template_version_hash
                AND newer.energy_type = j.energy_type
                AND newer.start_date <= j.start_date
                AND newer.end_date >= j.end_date
                AND newer.status = 'DONE'
                AND newer.verified_at IS NOT NULL
                AND newer.rollup_generation IS NOT NULL
                AND newer.rollup_generation <> ''
                AND newer.verified_at > j.verified_at
          )
        ORDER BY j.verified_at ASC
        LIMIT 20
        "#,
    )
    .bind(retention_days)
    .fetch_all(&pool)
    .await?;

    if rows.is_empty() {
        return Ok(());
    }

    let client = tdengine::get_tdengine_client()
        .ok_or_else(|| anyhow!("TDengine client not initialized"))?;
    for row in rows {
        let cleanup_job = CleanupJob {
            id: row.try_get("id")?,
            template_id: row.try_get("template_id")?,
            template_version_hash: row.try_get("template_version_hash")?,
            rollup_generation: row.try_get("rollup_generation")?,
            energy_type: row.try_get("energy_type")?,
        };
        cleanup_rollup_generation(client.as_ref(), &cleanup_job).await?;
        sqlx::query(
            "UPDATE tou_rollup_job SET status = 'CLEANED', error_message = 'cleaned old rollup generation', updated_at = NOW() WHERE id = ?",
        )
        .bind(cleanup_job.id)
        .execute(&pool)
        .await?;
    }

    Ok(())
}

async fn cleanup_rollup_generation(client: &taos::Taos, job: &CleanupJob) -> anyhow::Result<()> {
    let devices: HashSet<String> = load_energy_devices(job.energy_type)
        .await?
        .into_iter()
        .collect();
    let periods = load_template_periods(&RollupJob {
        id: job.id,
        template_id: job.template_id,
        template_version_hash: job.template_version_hash.clone(),
        rollup_generation: job.rollup_generation.clone(),
        energy_type: job.energy_type,
        start_date: NaiveDate::from_ymd_opt(1970, 1, 1).expect("valid date"),
        end_date: NaiveDate::from_ymd_opt(1970, 1, 1).expect("valid date"),
    })
    .await?;

    let mut table_names: Vec<String> = Vec::with_capacity(devices.len() * periods.len());
    for device in &devices {
        for period in &periods {
            table_names.push(rollup_subtable_name(
                device,
                job.energy_type,
                job.template_id,
                &job.template_version_hash,
                &job.rollup_generation,
                &period.period_key,
            ));
        }
    }
    for chunk in table_names.chunks(ROLLUP_DROP_TABLE_BATCH_SIZE) {
        let sql = format!("DROP TABLE IF EXISTS {}", chunk.join(", "));
        if let Err(err) = client.query(sql).await {
            let message = err.to_string().to_ascii_lowercase();
            if !message.contains("does not exist")
                && !message.contains("not exist")
                && !message.contains("unknown table")
            {
                return Err(err.into());
            }
        }
    }
    Ok(())
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
            &job.rollup_generation,
            &item.period_key,
        );
        let ts = item
            .day
            .and_time(NaiveTime::from_hms_opt(0, 0, 0).expect("midnight is valid"));
        sql.push_str(&format!(
            "{} USING {} TAGS ({}, {}, {}, {}, {}, {}) (ts, usage, calculated_at, source_updated_at) VALUES ({}, {:.6}, {}, {}) ",
            table_name,
            tou_stable,
            quote_tdengine_string(&item.device),
            quote_tdengine_string(&job.energy_type.to_string()),
            job.template_id,
            quote_tdengine_string(&job.template_version_hash),
            quote_tdengine_string(&item.period_key),
            quote_tdengine_string(&job.rollup_generation),
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

fn parse_mysql_naive_datetime(raw: &str) -> anyhow::Result<NaiveDateTime> {
    NaiveDateTime::parse_from_str(raw.trim(), "%Y-%m-%d %H:%M:%S").map_err(|err| err.into())
}

fn resolve_effective_end_date(end_at: NaiveDateTime) -> NaiveDate {
    if end_at.time() == NaiveTime::from_hms_opt(0, 0, 0).expect("midnight is valid") {
        (end_at - ChronoDuration::seconds(1)).date()
    } else {
        end_at.date()
    }
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
    rollup_generation: &str,
    period_key: &str,
) -> String {
    let raw = format!(
        "{}|{}|{}|{}|{}|{}",
        device, energy_type, template_id, version_hash, rollup_generation, period_key
    );
    format!("tou_{:x}", md5::compute(raw.as_bytes()))
}

fn generate_rollup_generation(job_id: i64) -> String {
    format!("job_{}_{}", job_id, chrono::Utc::now().timestamp_millis())
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

fn cleanup_hour() -> u32 {
    std::env::var("TOU_ROLLUP_CLEANUP_HOUR")
        .ok()
        .and_then(|raw| raw.trim().parse::<u32>().ok())
        .filter(|value| *value <= 23)
        .unwrap_or(DEFAULT_CLEANUP_HOUR)
}

fn cleanup_retention_days() -> i64 {
    std::env::var("TOU_ROLLUP_CLEANUP_RETENTION_DAYS")
        .ok()
        .and_then(|raw| raw.trim().parse::<i64>().ok())
        .filter(|value| *value >= 1)
        .unwrap_or(DEFAULT_CLEANUP_RETENTION_DAYS)
}
