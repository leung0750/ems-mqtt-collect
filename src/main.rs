use std::time::Duration;

use tokio::time::interval;

mod alarm;
mod conf;
mod mqtt;
mod mysqldb;
mod param_mapping;
mod redis;
mod tdengine;

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

    if let Err(err) = mqtt::handle::refresh_energy_type_cache().await {
        eprintln!("refresh energy type cache on startup failed: {}", err);
    }

    if let Err(err) = mqtt::handle::retry_cached_data().await {
        eprintln!("retry cached TDengine data on startup failed: {}", err);
    }

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
