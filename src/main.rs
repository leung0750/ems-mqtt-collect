
use tokio;

// 添加conf模块声明
mod conf;
// 修改导入路径，指向正确的模块位置
mod mqtt;
mod mysqldb;
mod tdengine;
mod redis;

#[tokio::main]
async fn main()-> Result<(), Box<dyn std::error::Error + Send + Sync >> {
    // 初始化redis连接
    println!("Initializing Redis connection...");
    let _ = redis::init_redis().await?;
    println!("Redis connection initialized successfully");
    
    // 测试Redis连接
    println!("Testing Redis connection...");
    let _redis_client = redis::get_redis_client().unwrap();
    println!("Redis connection test passed");
    
    // 初始化Tdengine连接
    println!("Initializing Tdengine connection...");
    let _ = tdengine::init_tdengine().await?;
    println!("Tdengine connection initialized successfully");
    
    // 测试Tdengine连接
    println!("Testing Tdengine connection...");
    tdengine::test_connection().await?;
    println!("Tdengine connection test passed");
    
    // 使用正确的模块路径调用client函数
    // let pool = mysqldb::get_pool().await?;
    // println!("MySQL pool connected: {:?}", pool);
    // pool.close().await;
    // 移除close调用，保持连接池打开

    mqtt::client::start().await?;
    Ok(())
}