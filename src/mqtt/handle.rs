use std::collections::HashMap;
use once_cell::sync::OnceCell;
use serde::{Deserialize, Serialize};
use sqlx::Row;

use crate::mysqldb;
use crate::tdengine::{get_tdengine_client};
use taos::AsyncQueryable;
use md5;
use crate::tdengine;
use uuid::Uuid;
use anyhow;



// 定义全局的参数映射表，使用OnceCell确保只初始化一次
static PARAM_MAPPING: OnceCell<HashMap<&'static str, &'static str>> = OnceCell::new();

// static DEVICE_CACHE: OnceCell<Arc<RwLock<DeviceMap>>> = OnceCell::new();
// static CACHE_INITIALIZED: AtomicBool = AtomicBool::new(false);

// 获取参数映射表的辅助函数
fn get_param_mapping() -> &'static HashMap<&'static str, &'static str> {
    PARAM_MAPPING.get_or_init(|| {
        HashMap::from([
            // ==========================
            // 1. 电压 (Voltage)
            // ==========================
            ("ua", "ua"), ("UA", "ua"), ("A相电压", "ua"), ("Ua", "ua"),
            ("ub", "ub"), ("UB", "ub"), ("B相电压", "ub"), ("Ub", "ub"),
            ("uc", "uc"), ("UC", "uc"), ("C相电压", "uc"), ("Uc", "uc"),
            
            ("uab", "uab"), ("Uab", "uab"), ("AB线电压", "uab"), ("AB相线电压", "uab"),
            ("ubc", "ubc"), ("Ubc", "ubc"), ("BC线电压", "ubc"), ("BC相线电压", "ubc"),
            ("uac", "uac"), ("Uac", "uac"), ("AC线电压", "uac"), ("CA线电压", "uac"), ("AC(CA)线电压", "uac"),

            // ==========================
            // 2. 电流 (Current)
            // ==========================
            ("ia", "ia"), ("IA", "ia"), ("A相电流", "ia"),
            ("ib", "ib"), ("IB", "ib"), ("B相电流", "ib"),
            ("ic", "ic"), ("IC", "ic"), ("C相电流", "ic"),
            ("z_i", "z_i"), ("零序电流", "z_i"),

            // ==========================
            // 3. 功率 (Power)
            // ==========================
            ("pa", "pa"), ("PA", "pa"), ("A相有功功率", "pa"),
            ("pb", "pb"), ("PB", "pb"), ("B相有功功率", "pb"),
            ("pc", "pc"), ("PC", "pc"), ("C相有功功率", "pc"),
            
            // 总有功 (兼容最多别名)
            ("p", "p"), ("P", "p"), ("总有功功率", "p"), ("有功功率", "p"), ("三相有功功率", "p"), ("输出功率", "p"),
            
            // 总无功
            ("q", "q"), ("Q", "q"), ("总无功功率", "q"), ("三相无功功率", "q"), ("三相无功", "q"),
            
            // 总视在
            ("s", "s"), ("S", "s"), ("总视在功率", "s"), ("三相视在功率", "s"), ("三相视在", "s"),

            // ==========================
            // 4. 功率因数与频率 (PF & Hz)
            // ==========================
            ("fa", "fa"), ("PFA", "fa"), ("A相功率因数", "fa"),
            ("fb", "fb"), ("PFB", "fb"), ("B相功率因数", "fb"),
            ("fc", "fc"), ("PFC", "fc"), ("C相功率因数", "fc"),
            ("f", "f"), ("PFS", "f"), ("总功率因数", "f"),
            
            ("hz", "hz"), ("HZ", "hz"), ("F", "hz"), ("频率", "hz"),

            // ==========================
            // 5. 电能/累计量 (Energy - DOUBLE类型)
            // ==========================
            // 正向有功
            ("pw", "pw"), ("PW", "pw"), ("EP", "pw"), 
            ("正向有功电能", "pw"), ("总有功电能", "pw"), ("总发电量", "pw"), ("总正向有功电能", "pw"),
            
            // 反向有功
            ("pw_fan", "pw_fan"), ("反向有功电能", "pw_fan"), ("总反向有功电能", "pw_fan"),
            
            // 无功
            ("qw", "qw"), ("正向无功电能", "qw"), ("总无功电能", "qw"), ("总正向无功电能", "qw"),
            ("qw_fan", "qw_fan"), ("反向无功电能", "qw_fan"),
            
            // 视在电能
            ("sw", "sw"), ("总视在电能", "sw"), ("视在电能", "sw"),

            // ==========================
            // 6. 流体与环境 (Fluids)
            // ==========================
            // 累计量
            ("st", "st"), ("ST", "st"), 
            ("累计流量", "st"), ("用水量", "st"), ("标况总量", "st"), ("标况体积总量", "st"),
            
            // 瞬时量
            ("fr", "fr"), ("FR", "fr"),
            ("瞬时流量", "fr"), ("风速", "fr"), ("流速", "fr"), ("瞬时质量流量", "fr"),
            
            // 专用流量
            ("sf", "sf"), ("标况流量", "sf"),
            ("wf", "wf"), ("工况流量", "wf"), ("补偿后流量", "wf"),
            
            // 温压
            ("tp", "tp"), ("温度", "tp"),
            ("pr", "pr"), ("压力", "pr"),

            // ==========================
            // 7. 振动 (Vibration)
            // ==========================
            ("dx", "dx"), ("DX", "dx"), ("X轴位移", "dx"),
            ("dy", "dy"), ("DY", "dy"), ("Y轴位移", "dy"),
            ("dz", "dz",), ("DZ", "dz"), ("Z轴位移", "dz"),
            
            ("vx", "vx"), ("VX", "vx"), ("X轴速度", "vx"),
            ("vy", "vy"), ("VY", "vy"), ("Y轴速度", "vy"),
            ("vz", "vz"), ("VZ", "vz"), ("Z轴速度", "vz"),
            
             // ==========================
            // 8. 其他标识
            // ==========================
            ("energy_type", "energy_type"), ("能源类型标识", "energy_type"),

            // 尖峰平谷预留字段
            ("peak", "peak"), ("尖", "peak"), ("尖峰", "peak"),
            ("flat", "flat"), ("平", "flat"), ("峰平", "flat"),
            ("valley", "valley"), ("谷", "valley"), ("谷段", "valley"),
            ("shoulder", "shoulder"), ("峰", "shoulder"), ("峰段", "shoulder"),
        ])
    })
}

// 定义能源类型
type EnergyType = i32;




async fn get_energy_type(device_name: &str) -> EnergyType {
    // 从Redis查询设备的能源类型
    let redis_key = format!("device::energy_type::{}", device_name);
    let energy_type_str = crate::redis::get_value(&redis_key).await.unwrap_or(Some("-1".to_string()));
    let energy_type: i32 = energy_type_str.unwrap_or("-1".to_string()).parse().unwrap_or(-1);
    if energy_type != -1 {
        return energy_type;
    }

    // 从MySQL查询设备的能源类型
    let pool = match mysqldb::get_pool().await {
        Ok(p) => p,
        Err(_) => return -1,
    };
    let result = sqlx::query("SELECT energy_type FROM device WHERE name = ?")
        .bind(device_name)
        .fetch_one(&pool)
        .await;
    
    if let Ok(row) = result {
        let db_energy_type: i32 = row.get(0);
        // 在Redis中存储能源类型
        let redis_key = format!("device::energy_type::{}", device_name);
        println!("缓存能源类型到Redis: {} -> {}", redis_key, db_energy_type);
        if let Err(e) = crate::redis::set_value(&redis_key, &db_energy_type.to_string()).await {
            eprintln!("缓存能源类型到Redis失败: {}", e);
        }
        // 更新energy_type变量
        return db_energy_type;
    }
    -1
}




// 数据结构定义
#[derive(Debug, Deserialize)]
struct MeasurementData {
    m: String,  // 参数名
    v: f64,     // 参数值
}

#[derive(Debug, Deserialize)]
struct Device {
    dev: String,               // 设备名
    d: Vec<MeasurementData>,   // 设备数据
}

#[derive(Debug, Deserialize)]
struct MqttMessage {
    devs: Vec<Device>,         // 设备列表
    ver: String,               // 版本号
    pKey: String,              // 主键
    sn: String,                // 设备序列号
    ts: i64,                   // 时间戳
}

// 缓存数据结构
#[derive(Debug, Serialize, Deserialize)]
struct CachedData {
    id: String,                // 唯一标识
    device_name: String,       // 设备名
    energy_type: i32,          // 能源类型
    gateway_name: String,       // 网关名
    timestamp: i64,            // 时间戳
    data_map: HashMap<String, f64>,  // 数据映射
    created_at: i64,           // 缓存时间
}





pub async fn handle_data(_topic: &str, payload: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    if !_topic.contains("/history") && !_topic.contains("/rtg") {
        return Ok(());
    }

    
    // 获取全局的参数映射表
    let param_mapping = get_param_mapping();
    
    // 尝试解析JSON数据
    match serde_json::from_str::<MqttMessage>(payload) {
        Ok(message) => {
            // 验证并处理每个设备的数据
            for device in &message.devs {
                let device_name = &device.dev;
                let energy_type = get_energy_type(device_name).await;
                if energy_type == -1 {
                    println!("未找到设备 {} 的能源类型", device_name);
                    continue; // 跳过当前设备处理
                }
                // 使用 redis 模块提供的 set_ex 函数
                let key_name = format!("alive::{}", device_name);
                // if !_topic.contains("/history") {
                //     continue; // 跳过当前设备处理
                // }
                crate::redis::set_ex(&key_name, "1", 7200).await?; // 2小时过期
                let gateway_name = &message.sn;

                
                // 将嵌套的设备数据转换为扁平化键值对格式
                let mut flat_data = Vec::new();
                let mut data_map = std::collections::HashMap::new();
                
                for measurement in &device.d {
                    if let Some(letter_param) = param_mapping.get(measurement.m.as_str()) {
                        flat_data.push(format!("{}:{}", letter_param, measurement.v));
                        data_map.insert(letter_param.to_string(), measurement.v);
                    }
                }

                // 计算指定参数的总和
                let sum_val: f64 = [
                    "uab", "ubc", "uac", "pw", "ua", "ub", "uc", 
                    "tp", "pr", "st", "fr"
                ].iter()
                    .map(|&key| *data_map.get(key).unwrap_or(&0.0))
                    .sum();

                if sum_val == 0.0 {
                    continue; // 跳过当前设备处理
                }
                // 输出扁平化的数据字符串
                if !flat_data.is_empty() { 
                    let table_suffix = format!("{:x}", md5::compute(device_name.as_bytes()));
                    let table_name = format!("d_{}", table_suffix);
                    let stable_name = tdengine::TDENGINE_CONFIG.get().unwrap().stable_name.clone();
                    let columns: Vec<&str> = data_map.keys().map(|k| k.as_str()).collect();
                    let values: Vec<String> = data_map.values().map(|v| v.to_string()).collect();
                    let sql = format!(
                        "INSERT INTO {} USING {} TAGS ('{}', '{}') (ts, {}) VALUES ({}, {})",
                        table_name,             // 子表名
                        stable_name,            // 超级表名
                        device_name,            // Tag1: device_id
                        energy_type,            // Tag2: energy_type
                        columns.join(", "),     // 列名列表: ua, ub, p...
                        message.ts*1000,             // 时间戳 (毫秒)
                        values.join(", ")       // 值列表: 220.5, 221.0, 15.5...
                    );
                    let client = get_tdengine_client()
                        .ok_or_else(|| anyhow::anyhow!("Tdengine client not initialized"))?;

                    if let Err(e) = client.query(&sql).await {
                        let error_msg = format!("{}", e);
                        eprintln!("写入 TDengine 失败: {}", error_msg);
                        
                        // 检查是否是 "Sync leader is restoring" 错误
                        if error_msg.contains("Sync leader is restoring") || error_msg.contains("0x0914") {
                            println!("检测到 TDengine 初始化中，缓存数据...");
                            // 缓存数据到 Redis
                            if let Err(cache_err) = cache_data(device_name, energy_type, gateway_name, message.ts, &data_map).await {
                                eprintln!("缓存数据失败: {}", cache_err);
                            }
                        }
                        continue; // 跳过当前设备处理
                    }

                    let flat_data_str = flat_data.join(" ");
                    
                    // 格式化时间戳示例
                    let formatted_time = format_timestamp(message.ts);
                    println!("网关 {} 设备 {} {} 能源类型:{} 时间戳: {}", gateway_name, device_name, flat_data_str, energy_type, formatted_time);
                    
                }
            }
        },
        Err(e) => {
            // 保留错误日志，便于调试
            eprintln!("解析JSON数据失败: {}", e);
        }
    }
    
    Ok(())
}

// 将Unix时间戳转换为格式化字符串
fn format_timestamp(timestamp: i64) -> String {
    use chrono::{DateTime, Utc, Local};
    
    match DateTime::<Utc>::from_timestamp(timestamp, 0) {
        Some(dt) => {
            // 转换为本地时间并格式化为 "YYYY-MM-DD HH:MM:SS" 格式
            dt.with_timezone(&Local)
                .format("%Y-%m-%d %H:%M:%S")
                .to_string()
        },
        None => "无效时间戳".to_string(),
    }
}

// 缓存数据到 Redis
async fn cache_data(device_name: &str, energy_type: i32, gateway_name: &str, timestamp: i64, data_map: &HashMap<String, f64>) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let cached_data = CachedData {
        id: Uuid::new_v4().to_string(),
        device_name: device_name.to_string(),
        energy_type,
        gateway_name: gateway_name.to_string(),
        timestamp,
        data_map: data_map.clone(),
        created_at: chrono::Utc::now().timestamp(),
    };
    
    let json_data = serde_json::to_string(&cached_data)?;
    let cache_key = format!("tdengine:cache:{}:{}", device_name, cached_data.id);
    
    // 设置缓存，过期时间为 7 天
    crate::redis::set_ex(&cache_key, &json_data, 7 * 24 * 3600).await?;
    
    // 添加到缓存列表，便于批量处理
    let list_key = "tdengine:cache:list";
    crate::redis::lpush(&list_key, &cache_key).await?;
    
    println!("数据已缓存到Redis: {} (设备: {})", cached_data.id, device_name);
    
    Ok(())
}

// 从 Redis 获取所有缓存数据
async fn get_all_cached_data() -> Result<Vec<CachedData>, Box<dyn std::error::Error + Send + Sync>> {
    let list_key = "tdengine:cache:list";
    let cache_keys: Vec<String> = crate::redis::lrange(&list_key, 0, -1).await?
        .into_iter()
        .filter_map(|k| k)
        .collect();
    
    let mut cached_data_list = Vec::new();
    
    for key in cache_keys {
        if let Some(json_data) = crate::redis::get_value(&key).await? {
            if let Ok(cached_data) = serde_json::from_str::<CachedData>(&json_data) {
                cached_data_list.push(cached_data);
            }
        }
    }
    
    Ok(cached_data_list)
}

// 从 Redis 删除缓存数据
async fn remove_cached_data(cache_id: &str, device_name: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let cache_key = format!("tdengine:cache:{}:{}", device_name, cache_id);
    let list_key = "tdengine:cache:list";
    
    // 从列表中移除
    crate::redis::lrem(&list_key, 1, &cache_key).await?;
    // 删除缓存
    crate::redis::del(&cache_key).await?;
    
    Ok(())
}

// 检查 TDengine 可用性
async fn check_tdengine_available() -> bool {
    if let Some(client) = get_tdengine_client() {
        match client.query("SELECT server_status()").await {
            Ok(_) => return true,
            Err(e) => {
                eprintln!("TDengine 检查失败: {}", e);
                return false;
            }
        }
    }
    false
}

// 检查缓存数据数量并触发告警
async fn check_cache_size() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let list_key = "tdengine:cache:list";
    let cache_keys: Vec<String> = crate::redis::lrange(&list_key, 0, -1).await?
        .into_iter()
        .filter_map(|k| k)
        .collect();
    
    let cache_count = cache_keys.len();
    
    // 当缓存数据超过阈值时触发告警
    if cache_count > 1000 {
        eprintln!("⚠️  缓存数据数量超过阈值: {} 条，请检查 TDengine 状态", cache_count);
        // 这里可以添加更复杂的告警逻辑，比如发送邮件或短信
    }
    
    // 检查缓存数据是否过期（超过7天）
    let now = chrono::Utc::now().timestamp();
    let expired_threshold = now - 7 * 24 * 3600;
    
    for key in cache_keys {
        if let Some(json_data) = crate::redis::get_value(&key).await? {
            if let Ok(cached_data) = serde_json::from_str::<CachedData>(&json_data) {
                if cached_data.created_at < expired_threshold {
                    eprintln!("删除过期缓存数据: {} (设备: {})", cached_data.id, cached_data.device_name);
                    crate::redis::del(&key).await?;
                    crate::redis::lrem(&list_key, 1, &key).await?;
                }
            }
        }
    }
    
    Ok(())
}

// 重试写入缓存数据
pub async fn retry_cached_data() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // 检查缓存大小并触发告警
    if let Err(e) = check_cache_size().await {
        eprintln!("检查缓存大小失败: {}", e);
    }
    
    // 检查 TDengine 是否可用
    if !check_tdengine_available().await {
        return Ok(());
    }
    
    // 获取所有缓存数据
    let cached_data_list = get_all_cached_data().await?;
    if cached_data_list.is_empty() {
        return Ok(());
    }
    
    println!("开始重试写入 {} 条缓存数据...", cached_data_list.len());
    
    let client = get_tdengine_client()
        .ok_or_else(|| anyhow::anyhow!("Tdengine client not initialized"))?;
    
    let mut success_count = 0;
    let mut fail_count = 0;
    
    for cached_data in cached_data_list {
        let table_suffix = format!("{:x}", md5::compute(cached_data.device_name.as_bytes()));
        let table_name = format!("d_{}", table_suffix);
        let stable_name = tdengine::TDENGINE_CONFIG.get().unwrap().stable_name.clone();
        let columns: Vec<&str> = cached_data.data_map.keys().map(|k| k.as_str()).collect();
        let values: Vec<String> = cached_data.data_map.values().map(|v| v.to_string()).collect();
        
        let sql = format!(
            "INSERT INTO {} USING {} TAGS ('{}', '{}') (ts, {}) VALUES ({}, {})",
            table_name,
            stable_name,
            cached_data.device_name,
            cached_data.energy_type,
            columns.join(", "),
            cached_data.timestamp * 1000,
            values.join(", ")
        );
        
        if let Err(e) = client.query(&sql).await {
            eprintln!("重试写入失败: {} (设备: {})", e, cached_data.device_name);
            fail_count += 1;
        } else {
            // 写入成功，删除缓存
            remove_cached_data(&cached_data.id, &cached_data.device_name).await?;
            success_count += 1;
        }
    }
    
    println!("重试完成: 成功 {}, 失败 {}", success_count, fail_count);
    
    Ok(())
}