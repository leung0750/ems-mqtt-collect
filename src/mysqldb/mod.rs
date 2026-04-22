use crate::conf;
use once_cell::sync::OnceCell;
use serde::Deserialize;
use sqlx::{
    mysql::{MySqlConnectOptions, MySqlPoolOptions},
    ConnectOptions, Error as SqlxError, MySqlPool,
};
use std::str::FromStr;
use tokio::sync::Mutex;

pub type DbResult<T> = Result<T, SqlxError>;

static DB_POOL: OnceCell<Mutex<Option<MySqlPool>>> = OnceCell::new();

#[derive(Debug, Deserialize)]
pub struct DbConfig {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: String,
    pub database: String,
}

fn load_db_config() -> DbConfig {
    match conf::load_config::<DbConfig>("MYSQL") {
        Ok(conf) => conf,
        Err(e) => {
            eprintln!(
                "Failed to load configuration: {}. Using default MYSQL config.",
                e
            );
            DbConfig {
                host: "127.0.0.1".to_string(),
                port: 3306,
                user: "root".to_string(),
                password: "gdnx@2025".to_string(),
                database: "demo".to_string(),
            }
        }
    }
}

pub fn get_database_name() -> String {
    load_db_config().database
}

async fn connect() -> DbResult<MySqlPool> {
    let conf = load_db_config();

    let database_url = format!(
        "mysql://{user}:{password}@{host}:{port}/{database}",
        user = conf.user,
        password = conf.password,
        host = conf.host,
        port = conf.port,
        database = conf.database
    );

    println!(
        "Database URL: mysql://{}:***@{host}:{port}/{database}",
        conf.user,
        host = conf.host,
        port = conf.port,
        database = conf.database
    );
    println!("Attempting to connect...");

    let options = MySqlConnectOptions::from_str(&database_url)?;
    let pool = MySqlPoolOptions::new()
        .after_connect(|conn, _meta| {
            Box::pin(async move {
                sqlx::query("SET time_zone = '+08:00'")
                    .execute(conn)
                    .await?;
                Ok(())
            })
        })
        .connect_with(options.disable_statement_logging())
        .await?;
    sqlx::query("SELECT 1").execute(&pool).await?;

    println!("Database Pool initialized successfully!");

    let _ = DB_POOL.get_or_init(|| Mutex::new(None));

    if let Some(mutex) = DB_POOL.get() {
        let mut pool_guard = mutex.lock().await;
        *pool_guard = Some(pool.clone());
    }

    Ok(pool)
}

pub async fn get_pool() -> DbResult<MySqlPool> {
    let mutex = DB_POOL.get_or_init(|| Mutex::new(None));

    let pool_guard = mutex.lock().await;

    match &*pool_guard {
        Some(pool) => Ok(pool.clone()),
        None => {
            drop(pool_guard);
            connect().await
        }
    }
}
