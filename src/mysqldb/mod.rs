use sqlx::{MySqlPool, Error as SqlxError};
use serde::Deserialize;
use crate::conf;
use once_cell::sync::OnceCell;
use tokio::sync::Mutex;

pub type DbResult<T> = Result<T, SqlxError>;

// 全局数据库连接池实例，使用OnceCell确保只初始化一次
static DB_POOL: OnceCell<Mutex<Option<MySqlPool>>> = OnceCell::new();

// 定义结构体来映射配置文件中的数据库配置
#[derive(Debug, Deserialize)]
pub struct DbConfig {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: String,
    pub database: String,
}

// 初始化数据库连接池并设置为全局单例
async fn connect() -> DbResult<MySqlPool> {
    // 使用更直接的环境变量方式，避免配置文件中的引号问题
    let conf = match conf::load_config::<DbConfig>("MYSQL") {
        Ok(conf) => conf,
        Err(e) => {
            eprintln!("Failed to load configuration: {}. Using default MYSQL config.", e);
            DbConfig { 
                host: "127.0.0.1".to_string(), 
                port: 3306, 
                user: "root".to_string(), 
                password: "gdnx@2025".to_string(),
                database: "demo".to_string() 
            }
        }
    };
    
    // 构造数据库连接 URL 字符串
    let database_url = format!(
        "mysql://{user}:{password}@{host}:{port}/{database}",
        user = conf.user,
        password = conf.password,
        host = conf.host,
        port = conf.port,
        database = conf.database
    );
    
    println!("Database URL: mysql://{}:***@{host}:{port}/{database}", conf.user, host = conf.host, port = conf.port, database = conf.database);
    println!("Attempting to connect...");

    // 使用构造的 URL 建立连接池
    let pool = MySqlPool::connect(&database_url).await?;

    // 验证连接是否有效
    sqlx::query("SELECT 1").execute(&pool).await?;

    println!("✅ Database Pool initialized successfully!");
    
    // 初始化全局数据库连接池实例
    let _ = DB_POOL.get_or_init(|| Mutex::new(None));
    
    // 获取互斥锁并设置连接池
    if let Some(mutex) = DB_POOL.get() {
        let mut pool_guard = mutex.lock().await;
        *pool_guard = Some(pool.clone());
    }
    
    Ok(pool)
}

// 获取全局数据库连接池的单例函数
pub async fn get_pool() -> DbResult<MySqlPool> {
    // 确保全局实例已初始化
    let mutex = DB_POOL.get_or_init(|| Mutex::new(None));
    
    let pool_guard = mutex.lock().await;
    
    match &*pool_guard {
        Some(pool) => Ok(pool.clone()),
        None => {
            // 如果连接池还未初始化，则调用connect函数初始化
            drop(pool_guard); // 释放锁，避免在connect中获取锁时造成死锁
            connect().await
        }
    }
}

