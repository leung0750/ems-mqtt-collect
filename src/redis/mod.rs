use crate::conf;
use redis::{Client, RedisResult};
use serde::Deserialize;
use std::sync::Arc;
use once_cell::sync::OnceCell;

#[derive(Debug, Deserialize)]
pub struct RedisConfig {
    pub url: String,
}

static REDIS_CLIENT: OnceCell<Arc<Client>> = OnceCell::new();

pub async fn init_redis() -> RedisResult<Arc<Client>> {
    let config_result: Result<RedisConfig, _> = conf::load_config("REDIS");
    let config = match config_result {
        Ok(cfg) => cfg,
        Err(_e) => {
            return Err(redis::RedisError::from((redis::ErrorKind::InvalidClientConfig, "Failed to load Redis config")));
        }
    };
    
    println!("Redis URL: {}", config.url);
    
    let client = Client::open(config.url)?;
    let client = Arc::new(client);
    
    REDIS_CLIENT.set(client.clone())
        .map_err(|_| redis::RedisError::from((redis::ErrorKind::InvalidClientConfig, "Redis client already initialized")))?;
    
    Ok(client)
}

pub fn get_redis_client() -> Option<Arc<Client>> {
    REDIS_CLIENT.get().cloned()
}

pub async fn set_ex(key: &str, value: &str, seconds: usize) -> redis::RedisResult<()> {
    if let Some(client) = get_redis_client() {
        let mut conn = client.get_multiplexed_async_connection().await?;
        redis::cmd("SETEX").arg(key).arg(seconds).arg(value).query_async::<_, ()>(&mut conn).await?;
        Ok(())
    } else {
        Err(redis::RedisError::from((redis::ErrorKind::InvalidClientConfig, "Redis client not initialized")))
    }
}

pub async fn get_value(key: &str) -> RedisResult<Option<String>> {
    if let Some(client) = get_redis_client() {
        let mut conn = client.get_multiplexed_async_connection().await?;
        let value: Option<String> = redis::cmd("GET").arg(key).query_async(&mut conn).await?;
        Ok(value)
    } else {
        Err(redis::RedisError::from((redis::ErrorKind::InvalidClientConfig, "Redis client not initialized")))
    }
}


pub async fn set_value(key: &str, value: &str) -> RedisResult<()> {
    if let Some(client) = get_redis_client() {
        let mut conn = client.get_multiplexed_async_connection().await?;
        redis::cmd("SET").arg(key).arg(value).query_async::<_, ()>(&mut conn).await?;
        Ok(())
    } else {
        Err(redis::RedisError::from((redis::ErrorKind::InvalidClientConfig, "Redis client not initialized")))
    }
}

pub async fn lpush(key: &str, value: &str) -> RedisResult<()> {
    if let Some(client) = get_redis_client() {
        let mut conn = client.get_multiplexed_async_connection().await?;
        redis::cmd("LPUSH").arg(key).arg(value).query_async::<_, ()>(&mut conn).await?;
        Ok(())
    } else {
        Err(redis::RedisError::from((redis::ErrorKind::InvalidClientConfig, "Redis client not initialized")))
    }
}

pub async fn lrange(key: &str, start: isize, stop: isize) -> RedisResult<Vec<Option<String>>> {
    if let Some(client) = get_redis_client() {
        let mut conn = client.get_multiplexed_async_connection().await?;
        let values: Vec<Option<String>> = redis::cmd("LRANGE").arg(key).arg(start).arg(stop).query_async(&mut conn).await?;
        Ok(values)
    } else {
        Err(redis::RedisError::from((redis::ErrorKind::InvalidClientConfig, "Redis client not initialized")))
    }
}

pub async fn lrem(key: &str, count: i32, value: &str) -> RedisResult<()> {
    if let Some(client) = get_redis_client() {
        let mut conn = client.get_multiplexed_async_connection().await?;
        redis::cmd("LREM").arg(key).arg(count).arg(value).query_async::<_, ()>(&mut conn).await?;
        Ok(())
    } else {
        Err(redis::RedisError::from((redis::ErrorKind::InvalidClientConfig, "Redis client not initialized")))
    }
}

pub async fn del(key: &str) -> RedisResult<()> {
    if let Some(client) = get_redis_client() {
        let mut conn = client.get_multiplexed_async_connection().await?;
        redis::cmd("DEL").arg(key).query_async::<_, ()>(&mut conn).await?;
        Ok(())
    } else {
        Err(redis::RedisError::from((redis::ErrorKind::InvalidClientConfig, "Redis client not initialized")))
    }
}