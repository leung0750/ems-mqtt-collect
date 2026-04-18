use crate::conf;
use crate::mysqldb;
use once_cell::sync::OnceCell;
use redis::{Client, RedisResult};
use serde::Deserialize;
use std::sync::Arc;

#[derive(Debug, Deserialize)]
pub struct RedisConfig {
    pub url: String,
}

static REDIS_CLIENT: OnceCell<Arc<Client>> = OnceCell::new();
static REDIS_KEY_PREFIX: OnceCell<String> = OnceCell::new();

fn redis_key_prefix() -> &'static str {
    REDIS_KEY_PREFIX
        .get_or_init(|| {
            let database_name = mysqldb::get_database_name();
            let database_name = database_name.trim();
            if database_name.is_empty() {
                String::new()
            } else {
                format!("{}::", database_name)
            }
        })
        .as_str()
}

fn namespace_key(key: &str) -> String {
    let prefix = redis_key_prefix();
    if prefix.is_empty() || key.is_empty() || key.starts_with(prefix) {
        key.to_string()
    } else {
        format!("{}{}", prefix, key)
    }
}

pub async fn init_redis() -> RedisResult<Arc<Client>> {
    let config_result: Result<RedisConfig, _> = conf::load_config("REDIS");
    let config = match config_result {
        Ok(cfg) => cfg,
        Err(_e) => {
            return Err(redis::RedisError::from((
                redis::ErrorKind::InvalidClientConfig,
                "Failed to load Redis config",
            )));
        }
    };

    println!("Redis URL: {}", config.url);
    println!("Redis key prefix: {}", redis_key_prefix());

    let client = Client::open(config.url)?;
    let client = Arc::new(client);

    REDIS_CLIENT.set(client.clone()).map_err(|_| {
        redis::RedisError::from((
            redis::ErrorKind::InvalidClientConfig,
            "Redis client already initialized",
        ))
    })?;

    Ok(client)
}

pub fn get_redis_client() -> Option<Arc<Client>> {
    REDIS_CLIENT.get().cloned()
}

pub async fn set_ex(key: &str, value: &str, seconds: usize) -> redis::RedisResult<()> {
    if let Some(client) = get_redis_client() {
        let mut conn = client.get_multiplexed_async_connection().await?;
        let key = namespace_key(key);
        redis::cmd("SETEX")
            .arg(&key)
            .arg(seconds)
            .arg(value)
            .query_async::<_, ()>(&mut conn)
            .await?;
        Ok(())
    } else {
        Err(redis::RedisError::from((
            redis::ErrorKind::InvalidClientConfig,
            "Redis client not initialized",
        )))
    }
}

pub async fn get_value(key: &str) -> RedisResult<Option<String>> {
    if let Some(client) = get_redis_client() {
        let mut conn = client.get_multiplexed_async_connection().await?;
        let key = namespace_key(key);
        let value: Option<String> = redis::cmd("GET").arg(&key).query_async(&mut conn).await?;
        Ok(value)
    } else {
        Err(redis::RedisError::from((
            redis::ErrorKind::InvalidClientConfig,
            "Redis client not initialized",
        )))
    }
}

pub async fn set_value(key: &str, value: &str) -> RedisResult<()> {
    if let Some(client) = get_redis_client() {
        let mut conn = client.get_multiplexed_async_connection().await?;
        let key = namespace_key(key);
        redis::cmd("SET")
            .arg(&key)
            .arg(value)
            .query_async::<_, ()>(&mut conn)
            .await?;
        Ok(())
    } else {
        Err(redis::RedisError::from((
            redis::ErrorKind::InvalidClientConfig,
            "Redis client not initialized",
        )))
    }
}

pub async fn lpush(key: &str, value: &str) -> RedisResult<()> {
    if let Some(client) = get_redis_client() {
        let mut conn = client.get_multiplexed_async_connection().await?;
        let key = namespace_key(key);
        redis::cmd("LPUSH")
            .arg(&key)
            .arg(value)
            .query_async::<_, ()>(&mut conn)
            .await?;
        Ok(())
    } else {
        Err(redis::RedisError::from((
            redis::ErrorKind::InvalidClientConfig,
            "Redis client not initialized",
        )))
    }
}

pub async fn lrange(key: &str, start: isize, stop: isize) -> RedisResult<Vec<Option<String>>> {
    if let Some(client) = get_redis_client() {
        let mut conn = client.get_multiplexed_async_connection().await?;
        let key = namespace_key(key);
        let values: Vec<Option<String>> = redis::cmd("LRANGE")
            .arg(&key)
            .arg(start)
            .arg(stop)
            .query_async(&mut conn)
            .await?;
        Ok(values)
    } else {
        Err(redis::RedisError::from((
            redis::ErrorKind::InvalidClientConfig,
            "Redis client not initialized",
        )))
    }
}

pub async fn lrem(key: &str, count: i32, value: &str) -> RedisResult<()> {
    if let Some(client) = get_redis_client() {
        let mut conn = client.get_multiplexed_async_connection().await?;
        let key = namespace_key(key);
        redis::cmd("LREM")
            .arg(&key)
            .arg(count)
            .arg(value)
            .query_async::<_, ()>(&mut conn)
            .await?;
        Ok(())
    } else {
        Err(redis::RedisError::from((
            redis::ErrorKind::InvalidClientConfig,
            "Redis client not initialized",
        )))
    }
}

pub async fn del(key: &str) -> RedisResult<()> {
    if let Some(client) = get_redis_client() {
        let mut conn = client.get_multiplexed_async_connection().await?;
        let key = namespace_key(key);
        redis::cmd("DEL")
            .arg(&key)
            .query_async::<_, ()>(&mut conn)
            .await?;
        Ok(())
    } else {
        Err(redis::RedisError::from((
            redis::ErrorKind::InvalidClientConfig,
            "Redis client not initialized",
        )))
    }
}

pub async fn zadd(key: &str, score: i64, member: &str) -> RedisResult<()> {
    if let Some(client) = get_redis_client() {
        let mut conn = client.get_multiplexed_async_connection().await?;
        let key = namespace_key(key);
        redis::cmd("ZADD")
            .arg(&key)
            .arg(score)
            .arg(member)
            .query_async::<_, ()>(&mut conn)
            .await?;
        Ok(())
    } else {
        Err(redis::RedisError::from((
            redis::ErrorKind::InvalidClientConfig,
            "Redis client not initialized",
        )))
    }
}

pub async fn brpop(key: &str, timeout_seconds: usize) -> RedisResult<Option<(String, String)>> {
    if let Some(client) = get_redis_client() {
        let mut conn = client.get_multiplexed_async_connection().await?;
        let key = namespace_key(key);
        let result: Option<[String; 2]> = redis::cmd("BRPOP")
            .arg(&key)
            .arg(timeout_seconds)
            .query_async(&mut conn)
            .await?;
        Ok(result.map(|[queue, value]| (queue, value)))
    } else {
        Err(redis::RedisError::from((
            redis::ErrorKind::InvalidClientConfig,
            "Redis client not initialized",
        )))
    }
}
