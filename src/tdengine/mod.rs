use crate::conf;
use anyhow::anyhow;
use once_cell::sync::OnceCell;
use serde::Deserialize;
use std::sync::{Arc, RwLock};
use taos::{self, AsyncQueryable, AsyncTBuilder};

#[derive(Debug, Deserialize)]
pub struct TdengineConfig {
    pub host: String,
    pub port: u16,
    pub username: String,
    pub password: String,
    pub database: String,
    #[serde(default = "default_origin_stable_name", alias = "stable_name")]
    pub origin_stable_name: String,
    #[serde(default = "default_hour_stable_name")]
    pub hour_stable_name: String,
    #[serde(default = "default_day_stable_name")]
    pub day_stable_name: String,
    #[serde(default = "default_month_stable_name")]
    pub month_stable_name: String,
    #[serde(default = "default_manual_hour_override_stable_name")]
    pub manual_hour_override_stable_name: String,
    #[serde(default = "default_tou_daily_stable_name")]
    pub tou_daily_stable_name: String,
    #[serde(default = "default_subtable_prefix")]
    pub subtable_prefix: String,
}

fn default_origin_stable_name() -> String {
    "origin".to_string()
}

fn default_hour_stable_name() -> String {
    "hour".to_string()
}

fn default_day_stable_name() -> String {
    "day".to_string()
}

fn default_month_stable_name() -> String {
    "month".to_string()
}

fn default_manual_hour_override_stable_name() -> String {
    "manual_hour_override".to_string()
}

fn default_tou_daily_stable_name() -> String {
    "tou_daily_usage_rollup".to_string()
}

fn default_subtable_prefix() -> String {
    "d_".to_string()
}

static TDENGINE_CLIENT: OnceCell<RwLock<Option<Arc<taos::Taos>>>> = OnceCell::new();
pub static TDENGINE_CONNECTED: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);
pub static LAST_RECONNECT_TIME: std::sync::atomic::AtomicI64 = std::sync::atomic::AtomicI64::new(0);
pub static TDENGINE_CONFIG: OnceCell<TdengineConfig> = OnceCell::new();

const RECONNECT_COOLDOWN_SECS: i64 = 10;

pub async fn init_tdengine() -> Result<Arc<taos::Taos>, anyhow::Error> {
    let mut config: TdengineConfig = conf::load_config("TDENGINE")?;
    config.database = config.database.trim().to_string();
    if config.database.is_empty() {
        return Err(anyhow!("TDENGINE.database cannot be empty"));
    }
    let _ = TDENGINE_CONFIG.set(config);

    let config = TDENGINE_CONFIG
        .get()
        .ok_or_else(|| anyhow!("TDengine config not initialized"))?;
    let app_client = build_tdengine_client(config).await?;
    ensure_table_schema(&app_client).await?;
    set_tdengine_client(app_client.clone())?;

    TDENGINE_CONNECTED.store(true, std::sync::atomic::Ordering::SeqCst);
    Ok(app_client)
}

pub fn get_tdengine_client() -> Option<Arc<taos::Taos>> {
    let lock = TDENGINE_CLIENT.get()?;
    let guard = lock.read().ok()?;
    guard.clone()
}

pub fn is_tdengine_connected() -> bool {
    TDENGINE_CONNECTED.load(std::sync::atomic::Ordering::SeqCst)
}

pub fn get_origin_stable_name() -> anyhow::Result<String> {
    Ok(TDENGINE_CONFIG
        .get()
        .ok_or_else(|| anyhow!("TDengine config not initialized"))?
        .origin_stable_name
        .clone())
}

pub fn get_hour_stable_name() -> anyhow::Result<String> {
    Ok(TDENGINE_CONFIG
        .get()
        .ok_or_else(|| anyhow!("TDengine config not initialized"))?
        .hour_stable_name
        .clone())
}

pub fn get_tou_daily_stable_name() -> anyhow::Result<String> {
    Ok(TDENGINE_CONFIG
        .get()
        .ok_or_else(|| anyhow!("TDengine config not initialized"))?
        .tou_daily_stable_name
        .clone())
}

pub fn get_subtable_name(device_name: &str) -> anyhow::Result<String> {
    let config = TDENGINE_CONFIG
        .get()
        .ok_or_else(|| anyhow!("TDengine config not initialized"))?;
    let digest = format!("{:x}", md5::compute(device_name.as_bytes()));
    Ok(format!("{}{}", config.subtable_prefix, digest))
}

pub async fn reconnect_tdengine() -> Result<Arc<taos::Taos>, anyhow::Error> {
    let now = chrono::Utc::now().timestamp();
    let last_reconnect = LAST_RECONNECT_TIME.load(std::sync::atomic::Ordering::SeqCst);

    if now - last_reconnect < RECONNECT_COOLDOWN_SECS {
        return Err(anyhow!("Reconnect in cooldown"));
    }

    LAST_RECONNECT_TIME.store(now, std::sync::atomic::Ordering::SeqCst);

    let config = TDENGINE_CONFIG
        .get()
        .ok_or_else(|| anyhow!("TDengine config not initialized"))?;
    let app_client = build_tdengine_client(config).await?;
    ensure_table_schema(&app_client).await?;
    app_client.query("SELECT server_status()").await?;
    set_tdengine_client(app_client.clone())?;

    TDENGINE_CONNECTED.store(true, std::sync::atomic::Ordering::SeqCst);
    println!("TDengine reconnect success");
    Ok(app_client)
}

pub async fn test_connection() -> Result<(), anyhow::Error> {
    let client = get_tdengine_client().ok_or_else(|| anyhow!("TDengine client not initialized"))?;
    client.query("SELECT server_status()").await?;
    Ok(())
}

async fn build_tdengine_client(config: &TdengineConfig) -> Result<Arc<taos::Taos>, anyhow::Error> {
    let database_identifier = quote_identifier(&config.database)?;
    let root_dsn = format!(
        "taos://{}:{}@{}:{}",
        config.username, config.password, config.host, config.port
    );
    let admin_client = taos::TaosBuilder::from_dsn(root_dsn)?.build().await?;
    let create_db_sql = format!(
        "CREATE DATABASE IF NOT EXISTS {} PRECISION 'ms' KEEP 3650",
        database_identifier
    );
    admin_client.query(create_db_sql).await?;

    let app_dsn = format!(
        "taos://{}:{}@{}:{}/{}",
        config.username, config.password, config.host, config.port, config.database
    );
    let app_client = taos::TaosBuilder::from_dsn(app_dsn)?.build().await?;
    Ok(Arc::new(app_client))
}

fn quote_identifier(name: &str) -> anyhow::Result<String> {
    let trimmed = name.trim();
    if trimmed.is_empty() {
        return Err(anyhow!("TDengine identifier cannot be empty"));
    }
    Ok(format!("`{}`", trimmed.replace('`', "``")))
}

fn set_tdengine_client(client: Arc<taos::Taos>) -> Result<(), anyhow::Error> {
    let lock = TDENGINE_CLIENT.get_or_init(|| RwLock::new(None));
    let mut guard = lock
        .write()
        .map_err(|_| anyhow!("TDengine client lock poisoned"))?;
    *guard = Some(client);
    Ok(())
}

pub async fn ensure_table_schema(client: &taos::Taos) -> anyhow::Result<()> {
    let config = TDENGINE_CONFIG
        .get()
        .ok_or_else(|| anyhow!("TDengine config not initialized"))?;

    let origin_sql = format!(
        r#"
CREATE STABLE IF NOT EXISTS {} (
    ts TIMESTAMP,
    ua DOUBLE,
    ub DOUBLE,
    uc DOUBLE,
    avg_u DOUBLE,
    uab DOUBLE,
    ubc DOUBLE,
    uac DOUBLE,
    avg_uu DOUBLE,
    ia DOUBLE,
    ib DOUBLE,
    ic DOUBLE,
    avg_i DOUBLE,
    z_i DOUBLE,
    pa DOUBLE,
    pb DOUBLE,
    pc DOUBLE,
    p DOUBLE,
    q DOUBLE,
    s DOUBLE,
    fa DOUBLE,
    fb DOUBLE,
    fc DOUBLE,
    f DOUBLE,
    hz DOUBLE,
    pw DOUBLE,
    pw_fan DOUBLE,
    qw DOUBLE,
    qw_fan DOUBLE,
    sw DOUBLE,
    peak_plus_energy DOUBLE,
    peak_energy DOUBLE,
    flat_energy DOUBLE,
    valley_energy DOUBLE,
    st DOUBLE,
    st_raw DOUBLE,
    fr DOUBLE,
    sf DOUBLE,
    wf DOUBLE,
    tp DOUBLE,
    pr DOUBLE,
    dx DOUBLE,
    dy DOUBLE,
    dz DOUBLE,
    vx DOUBLE,
    vy DOUBLE,
    vz DOUBLE
) TAGS (
    device_id NCHAR(255),
    energy_type NCHAR(32)
);
"#,
        config.origin_stable_name
    );

    let day_sql = format!(
        r#"
CREATE STABLE IF NOT EXISTS {} (
    ts TIMESTAMP,
    energy DOUBLE,
    start_val DOUBLE,
    end_val DOUBLE
) TAGS (
    device_id NCHAR(255),
    energy_type NCHAR(32)
);
"#,
        config.day_stable_name
    );

    let hour_sql = format!(
        r#"
CREATE STABLE IF NOT EXISTS {} (
    ts TIMESTAMP,
    energy DOUBLE,
    start_val DOUBLE,
    end_val DOUBLE
) TAGS (
    device_id NCHAR(255),
    energy_type NCHAR(32)
);
"#,
        config.hour_stable_name
    );

    let month_sql = format!(
        r#"
CREATE STABLE IF NOT EXISTS {} (
    ts TIMESTAMP,
    energy DOUBLE,
    start_val DOUBLE,
    end_val DOUBLE
) TAGS (
    device_id NCHAR(255),
    energy_type NCHAR(32)
);
"#,
        config.month_stable_name
    );

    let manual_hour_override_sql = format!(
        r#"
CREATE STABLE IF NOT EXISTS {} (
    ts TIMESTAMP,
    energy DOUBLE,
    base_energy DOUBLE,
    updated_at TIMESTAMP
) TAGS (
    device_id NCHAR(255),
    energy_type NCHAR(32)
);
"#,
        config.manual_hour_override_stable_name
    );

    let tou_daily_sql = format!(
        r#"
CREATE STABLE IF NOT EXISTS {} (
    ts TIMESTAMP,
    usage DOUBLE,
    calculated_at TIMESTAMP,
    source_updated_at TIMESTAMP
) TAGS (
    device_id NCHAR(255),
    energy_type NCHAR(32),
    template_id BIGINT,
    template_version_hash NCHAR(64),
    period_key NCHAR(64),
    rollup_generation NCHAR(64)
);
"#,
        config.tou_daily_stable_name
    );

    let hour_ele_stream_sql = format!(
        r#"
CREATE STREAM IF NOT EXISTS hour_stream_ele_v2
INTERVAL(1h) SLIDING(1h)
FROM {} PARTITION BY tbname, device_id, energy_type
STREAM_OPTIONS(FILL_HISTORY_FIRST|PRE_FILTER(energy_type = '1'))
INTO {} (ts, energy, start_val, end_val)
TAGS (
  device_id NCHAR(255) AS CAST(device_id AS NCHAR(255)),
  energy_type NCHAR(32) AS CAST(energy_type AS NCHAR(32))
)
AS
SELECT
  _twstart AS ts,
  SPREAD(pw) AS energy,
  FIRST(pw) AS start_val,
  LAST(pw) AS end_val
FROM %%tbname
WHERE _c0 >= _twstart AND _c0 <= _twend
"#,
        config.origin_stable_name, config.hour_stable_name
    );

    let hour_st_stream_sql = format!(
        r#"
CREATE STREAM IF NOT EXISTS hour_stream_st_v2
INTERVAL(1h) SLIDING(1h)
FROM {} PARTITION BY tbname, device_id, energy_type
STREAM_OPTIONS(FILL_HISTORY_FIRST|PRE_FILTER(st IS NOT NULL))
INTO {} (ts, energy, start_val, end_val)
TAGS (
  device_id NCHAR(255) AS CAST(device_id AS NCHAR(255)),
  energy_type NCHAR(32) AS CAST(energy_type AS NCHAR(32))
)
AS
SELECT
  _twstart AS ts,
  SPREAD(st) AS energy,
  FIRST(st) AS start_val,
  LAST(st) AS end_val
FROM %%tbname
WHERE _c0 >= _twstart AND _c0 <= _twend
"#,
        config.origin_stable_name, config.hour_stable_name
    );

    let day_stream_sql = format!(
        r#"
CREATE STREAM IF NOT EXISTS day_stream_v2
INTERVAL(1d) SLIDING(1d)
FROM {} PARTITION BY tbname, device_id, energy_type
STREAM_OPTIONS(FILL_HISTORY_FIRST)
INTO {} (ts, energy, start_val, end_val)
TAGS (
  device_id NCHAR(255) AS CAST(device_id AS NCHAR(255)),
  energy_type NCHAR(32) AS CAST(energy_type AS NCHAR(32))
)
AS
SELECT
  _twstart AS ts,
  SUM(energy) AS energy,
  FIRST(start_val) AS start_val,
  LAST(end_val) AS end_val
FROM %%tbname
WHERE _c0 >= _twstart AND _c0 < _twend
"#,
        config.hour_stable_name, config.day_stable_name
    );

    let month_stream_sql = format!(
        r#"
CREATE STREAM IF NOT EXISTS month_stream_v2
SLIDING(1d)
FROM {} PARTITION BY tbname, device_id, energy_type
STREAM_OPTIONS(FILL_HISTORY_FIRST)
INTO {} (ts, energy, start_val, end_val)
TAGS (
  device_id NCHAR(255) AS CAST(device_id AS NCHAR(255)),
  energy_type NCHAR(32) AS CAST(energy_type AS NCHAR(32))
)
AS
SELECT
  TO_TIMESTAMP(TO_CHAR(_tcurrent_ts, 'YYYY-MM'), 'YYYY-MM') AS ts,
  SUM(energy) AS energy,
  FIRST(start_val) AS start_val,
  LAST(end_val) AS end_val
FROM %%tbname
WHERE _c0 >= TO_TIMESTAMP(TO_CHAR(_tcurrent_ts, 'YYYY-MM'), 'YYYY-MM')
  AND _c0 < TO_TIMESTAMP(TO_CHAR(_tcurrent_ts + 1n, 'YYYY-MM'), 'YYYY-MM')
"#,
        config.day_stable_name, config.month_stable_name
    );

    upgrade_legacy_streams(client).await?;
    upgrade_current_streams(client, config).await?;

    for (name, sql) in [
        ("origin stable", origin_sql.as_str()),
        ("hour stable", hour_sql.as_str()),
        ("day stable", day_sql.as_str()),
        ("month stable", month_sql.as_str()),
        (
            "manual hour override stable",
            manual_hour_override_sql.as_str(),
        ),
        ("tou daily stable", tou_daily_sql.as_str()),
        ("hour electricity stream", hour_ele_stream_sql.as_str()),
        ("hour st stream", hour_st_stream_sql.as_str()),
        ("day stream", day_stream_sql.as_str()),
        ("month stream", month_stream_sql.as_str()),
    ] {
        if let Err(err) = client.query(sql).await {
            let err_msg = err.to_string().to_ascii_lowercase();
            if name.contains("stream") && err_msg.contains("stream already exists") {
                println!("TDengine {} already exists, skip", name);
                continue;
            }
            eprintln!("Failed to ensure {}: {}", name, err);
            return Err(err.into());
        }
    }
    ensure_streams_started(client).await?;
    ensure_origin_mapping_columns(client, &config.origin_stable_name).await?;

    println!(
        "TDengine schema ensured: origin={}, hour={}, day={}, month={}, manual_hour_override={}, tou_daily={}, subtable_prefix={}",
        config.origin_stable_name,
        config.hour_stable_name,
        config.day_stable_name,
        config.month_stable_name,
        config.manual_hour_override_stable_name,
        config.tou_daily_stable_name,
        config.subtable_prefix
    );
    Ok(())
}

async fn upgrade_legacy_streams(client: &taos::Taos) -> anyhow::Result<()> {
    for sql in [
        "STOP STREAM IF EXISTS hour_stream_ele",
        "STOP STREAM IF EXISTS hour_stream_st",
        "STOP STREAM IF EXISTS day_stream",
        "STOP STREAM IF EXISTS month_stream",
        "STOP STREAM IF EXISTS day_stream_ele",
        "STOP STREAM IF EXISTS day_stream_st",
        "DROP STREAM IF EXISTS hour_stream_ele",
        "DROP STREAM IF EXISTS hour_stream_st",
        "DROP STREAM IF EXISTS day_stream",
        "DROP STREAM IF EXISTS month_stream",
        "DROP STREAM IF EXISTS day_stream_ele",
        "DROP STREAM IF EXISTS day_stream_st",
    ] {
        if let Err(err) = client.query(sql).await {
            let err_msg = err.to_string().to_ascii_lowercase();
            if err_msg.contains("does not exist") || err_msg.contains("not exist") {
                continue;
            }
            eprintln!(
                "Failed to run legacy stream upgrade step `{}`: {}",
                sql, err
            );
            return Err(err.into());
        }
    }
    Ok(())
}

async fn upgrade_current_streams(
    client: &taos::Taos,
    config: &TdengineConfig,
) -> anyhow::Result<()> {
    let checks = [
        (
            "day_stream_v2",
            vec!["where _c0 >= _twstart and _c0 <= _twend"],
        ),
        (
            "month_stream_v2",
            vec![
                "period(1d)",
                "_tlocaltime",
                "create stream if not exists month_stream_v2",
            ],
        ),
    ];

    for (stream_name, legacy_patterns) in checks {
        let sql = format!(
            "SELECT sql FROM information_schema.ins_streams WHERE db_name = '{}' AND stream_name = '{}' LIMIT 1",
            config.database, stream_name
        );
        let existing: Option<(String,)> = AsyncQueryable::query_one(client, sql).await?;
        let Some((stream_sql,)) = existing else {
            continue;
        };

        let normalized = stream_sql.to_ascii_lowercase();
        let should_recreate = legacy_patterns
            .iter()
            .any(|pattern| normalized.contains(pattern));

        if !should_recreate {
            continue;
        }

        for action in [
            format!("STOP STREAM IF EXISTS {}", stream_name),
            format!("DROP STREAM IF EXISTS {}", stream_name),
        ] {
            if let Err(err) = client.query(&action).await {
                let err_msg = err.to_string().to_ascii_lowercase();
                if err_msg.contains("does not exist")
                    || err_msg.contains("not exist")
                    || err_msg.contains("stream was not stopped")
                {
                    continue;
                }
                eprintln!(
                    "Failed to recreate outdated stream {} with step `{}`: {}",
                    stream_name, action, err
                );
                return Err(err.into());
            }
        }

        println!(
            "TDengine outdated stream {} dropped for re-create",
            stream_name
        );
    }

    Ok(())
}

async fn ensure_streams_started(client: &taos::Taos) -> anyhow::Result<()> {
    let streams = [
        "hour_stream_ele_v2",
        "hour_stream_st_v2",
        "day_stream_v2",
        "month_stream_v2",
    ];
    for stream in streams {
        let sql = format!("START STREAM IF EXISTS IGNORE UNTREATED {}", stream);
        if let Err(err) = client.query(sql).await {
            let err_msg = err.to_string().to_ascii_lowercase();
            if err_msg.contains("stream was not stopped") {
                println!("TDengine stream {} already running, skip start", stream);
                continue;
            }
            eprintln!("Failed to start stream {}: {}", stream, err);
            return Err(err.into());
        }
    }
    Ok(())
}

async fn ensure_origin_mapping_columns(
    client: &taos::Taos,
    stable_name: &str,
) -> anyhow::Result<()> {
    for column in ["avg_u", "avg_uu", "avg_i"] {
        let sql = format!("ALTER STABLE {} ADD COLUMN {} DOUBLE", stable_name, column);
        if let Err(err) = client.query(sql).await {
            let err_msg = err.to_string().to_ascii_lowercase();
            if err_msg.contains("exist") || err_msg.contains("duplicate") {
                continue;
            }
            eprintln!(
                "Failed to ensure origin mapping column {}.{}: {}",
                stable_name, column, err
            );
            return Err(err.into());
        }
    }
    Ok(())
}
