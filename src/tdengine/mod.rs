use crate::conf;
use serde::Deserialize;
use taos;
use taos::{AsyncQueryable,AsyncTBuilder};
use std::sync::Arc;
use once_cell::sync::OnceCell;


#[derive(Debug, Deserialize)]
pub struct TdengineConfig {
    pub host: String,
    pub port: u16,
    pub username: String,
    pub password: String,
    pub database: String,
    pub stable_name: String,
}

static TDENGINE_CLIENT: OnceCell<Arc<taos::Taos>> = OnceCell::new();
pub static TDENGINE_CONFIG: OnceCell<TdengineConfig> = OnceCell::new();

pub async fn init_tdengine() -> Result<Arc<taos::Taos>, anyhow::Error> {
    // 1. 加载并设置全局配置
    let config: TdengineConfig = conf::load_config("TDENGINE")?;
    TDENGINE_CONFIG.set(config)
        .map_err(|_| anyhow::anyhow!("Tdengine config already initialized"))?;
    
    // 获取配置引用，方便后续使用
    let config = TDENGINE_CONFIG.get().unwrap();
    let db_name = &config.database;

    // ==========================================
    // 第一步：Admin 连接 (无 DB) -> 负责创建数据库
    // ==========================================
    let root_dsn = format!("taos://{}:{}@{}:{}", 
        config.username, config.password, config.host, config.port
    );
    println!("Tdengine Admin DSN: {}", root_dsn);
    
    let admin_client = taos::TaosBuilder::from_dsn(root_dsn)?.build().await?;

    // 核心修改：直接尝试建库 (利用 IF NOT EXISTS)，不再依赖 USE 报错来判断
    // 这样代码更简洁，也不容易出错
    println!("Ensuring database '{}' exists...", db_name);
    let create_db_sql = format!("CREATE DATABASE IF NOT EXISTS {} PRECISION 'ms' KEEP 3650", db_name);
    admin_client.query(create_db_sql).await?;
    
    // ==========================================
    // 第二步：App 连接 (有 DB) -> 负责建表和业务
    // ==========================================
    // 核心修改：这里必须用 config.database，绝对不能用 stable_name
    let app_dsn = format!(
        "taos://{}:{}@{}:{}/{}", 
        config.username, config.password, config.host, config.port, 
        config.database // <--- 修正点：这里是数据库名
    );
    println!("Tdengine App DSN: {}", app_dsn);

    let app_client = taos::TaosBuilder::from_dsn(app_dsn)?.build().await?;
    
    // ==========================================
    // 第三步：执行 stable.sql (无论库是否存在都要执行)
    // ==========================================
    // 使用 app_client 执行，因为它已经连接到了 db_name，无需 USE
    ensure_table_schema(&app_client).await?;

    // ==========================================
    // 第四步：注册全局客户端
    // ==========================================
    let app_client = Arc::new(app_client);
    TDENGINE_CLIENT.set(app_client.clone())
        .map_err(|_| anyhow::anyhow!("Tdengine client already initialized"))?;
    
    Ok(app_client)
}

pub fn get_tdengine_client() -> Option<Arc<taos::Taos>> {
    TDENGINE_CLIENT.get().cloned()
}

pub async fn test_connection() -> Result<(), anyhow::Error> {
    let client = get_tdengine_client()
        .ok_or_else(|| anyhow::anyhow!("Tdengine client not initialized"))?;
    
    let result = client.query("SELECT server_status()").await?;
    println!("Tdengine connection test successful: {:?}", result);
    
    Ok(())
}

/// 在代码中定义表结构并执行创建
pub async fn ensure_table_schema(client: &taos::Taos) -> anyhow::Result<()> {
    // 使用 r#""# 语法包裹原始 SQL，保持格式清晰
    // 我已修正了原 SQL 中的拼写错误 (ennergy_type -> energy_type)
    let sql = r#"
    CREATE STABLE IF NOT EXISTS collect_energy (
        /* === 基础字段 === */
        ts TIMESTAMP,                  -- [时间戳] 主键

        /* === A. 电力：电压 (FLOAT) === */
        ua FLOAT, ub FLOAT, uc FLOAT,  -- 相电压
        uab FLOAT, ubc FLOAT, uac FLOAT, -- 线电压

        /* === B. 电力：电流 (FLOAT) === */
        ia FLOAT, ib FLOAT, ic FLOAT,  -- 相电流
        z_i FLOAT,                     -- 零序电流

        /* === C. 电力：功率 (FLOAT) === */
        pa FLOAT, pb FLOAT, pc FLOAT,  -- 有功功率
        p  FLOAT,                      -- 总有功
        q  FLOAT,                      -- 总无功
        s  FLOAT,                      -- 总视在

        /* === D. 电力：因数与频率 (FLOAT) === */
        fa FLOAT, fb FLOAT, fc FLOAT,  -- 功率因数
        f  FLOAT,                      -- 总功率因数
        hz FLOAT,                      -- 频率

        /* === E. 电力：电能抄表 (DOUBLE - 核心计费) === */
        pw DOUBLE,                     -- 正向有功总电能 (kWh)
        pw_fan DOUBLE,                 -- 反向有功总电能
        qw DOUBLE,                     -- 正向无功总电能
        qw_fan DOUBLE,                 -- 反向无功总电能
        sw DOUBLE,                     -- 总视在电能

        /* === F. 流体：水/气/热 (兼容设计) === */
        st DOUBLE,                     -- 结算累计量
        st_raw DOUBLE,                 -- 原始累计量
        fr FLOAT,                      -- 瞬时流速
        sf FLOAT,                      -- 标况流量
        wf FLOAT,                      -- 工况流量
        tp FLOAT,                      -- 流体温度
        pr FLOAT,                      -- 流体压力

        /* === G. 机械振动 (FLOAT) === */
        dx FLOAT, dy FLOAT, dz FLOAT,  -- 位移
        vx FLOAT, vy FLOAT, vz FLOAT   -- 速度

    ) TAGS (
        /* === 标签 (Tags) === */
        device_id BINARY(50),      -- 设备编号
        energy_type BINARY(20)     -- 设备类型 (修正了拼写)
    );
    "#;

    println!("Ensuring stable 'collect_energy' schema...");
    
    // 执行 SQL
    match client.query(sql).await {
        Ok(_) => {
            println!("✅ Schema check passed (CREATE STABLE IF NOT EXISTS executed).");
            Ok(())
        },
        Err(e) => {
            eprintln!("❌ Failed to create stable 'collect_energy'.");
            eprintln!("Error details: {}", e);
            Err(e.into())
        }
    }
}