use std::time::Duration;

use tokio::time::interval;

mod alarm;
mod conf;
mod mqtt;
mod mysqldb;
mod param_mapping;
mod redis;
mod tdengine;
mod tou_rollup;

#[derive(serde::Deserialize, Default)]
struct TouRollupConfig {
    once: Option<bool>,
}

fn should_run_tou_rollup_once() -> bool {
    if let Ok(value) = std::env::var("TOU_ROLLUP_ONCE") {
        return value == "1" || value.eq_ignore_ascii_case("true");
    }

    conf::load_config::<TouRollupConfig>("TOU_ROLLUP")
        .ok()
        .and_then(|config| config.once)
        .unwrap_or(false)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    println!("Initializing Redis connection...");
    let _ = redis::init_redis().await?;
    println!("Redis connection initialized successfully");

    println!("Initializing param mapping cache...");
    if let Err(err) = param_mapping::init_param_mapping().await {
        eprintln!("init param mapping failed: {}", err);
    }

    println!("Initializing TDengine connection...");
    let _ = tdengine::init_tdengine().await?;
    tdengine::test_connection().await?;
    println!("TDengine connection initialized successfully");

    if should_run_tou_rollup_once() {
        println!("TOU_ROLLUP_ONCE enabled, processing one rollup batch and exiting");
        tou_rollup::run_once().await?;
        return Ok(());
    }

    if let Err(err) = mqtt::handle::refresh_energy_type_cache().await {
        eprintln!("refresh energy type cache on startup failed: {}", err);
    }

    if let Err(err) = mqtt::handle::retry_cached_data().await {
        eprintln!("retry cached TDengine data on startup failed: {}", err);
    }

    tou_rollup::spawn_worker();
    tou_rollup::spawn_command_listener();

    tokio::spawn(async {
        let mut ticker = interval(Duration::from_secs(30));
        loop {
            ticker.tick().await;
            if let Err(err) = mqtt::handle::retry_cached_data().await {
                eprintln!("retry cached TDengine data failed: {}", err);
            }
        }
    });

    tokio::spawn(async {
        let mut ticker = interval(Duration::from_secs(60));
        loop {
            ticker.tick().await;
            if let Err(err) = mqtt::handle::refresh_energy_type_cache().await {
                eprintln!("refresh energy type cache failed: {}", err);
            }
        }
    });

    mqtt::client::start().await?;
    Ok(())
}
