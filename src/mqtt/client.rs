use crate::conf;
use rumqttc::{AsyncClient, EventLoop, MqttOptions};
use std::time::Duration;

use tokio;
use super::handle::handle_data;




#[derive(serde::Deserialize)]
pub struct MqttConfig {
    pub host: String,
    pub port: u16,
    pub username: String,
    pub password: String,
}

async fn connect() -> Result<(AsyncClient, EventLoop), Box<dyn std::error::Error + Send + Sync>> {
    let conf = match conf::load_config::<MqttConfig>("MQTT") {
        Ok(conf) => conf,
        Err(e) => {
            eprintln!("Failed to load configuration: {}. Using default MQTT config.", e);
            MqttConfig { 
                host: "http://127.0.0.1/".to_string(), 
                port: 11883, 
                username: "admin".to_string(), 
                password: "admin".to_string() 
            }
        }
    };
    
    // 创建MQTT连接选项
    // 清理host中的http://或https://前缀
    let host = conf.host.replace("http://", "").replace("https://", "");
    // 使用host的引用而不是移动它
    let mut mqtt_options = MqttOptions::new("mqtt_collect_node", &host, conf.port);
    mqtt_options.set_credentials(conf.username, conf.password);
    // 增加keep-alive时间，避免频繁ping请求
    mqtt_options.set_keep_alive(Duration::from_secs(30));
    mqtt_options.set_clean_session(true); // 设置为true以确保会话状态被清除
    let (client, mut eventloop) = AsyncClient::new(mqtt_options, 10);
    
    // 等待连接事件
    tokio::select! {
        // 处理连接事件
        result = eventloop.poll() => {
            match result {
                Ok(event) => {
                    println!("MQTT event received: {:?}", event);
                    match event {
                        rumqttc::Event::Incoming(rumqttc::Packet::ConnAck(_)) => {
                            println!("Successfully connected to MQTT broker!");
                        },
                        rumqttc::Event::Incoming(rumqttc::Packet::Disconnect) => {
                            eprintln!("Disconnected from MQTT broker");
                            return Err("Disconnected from MQTT broker".into());
                        },
                        _ => {}
                    }
                },
                Err(e) => {
                    eprintln!("Failed to connect to MQTT broker: {}", e);
                    return Err(e.into());
                }
            }
        },
        
        // 设置连接超时
        _ = tokio::time::sleep(Duration::from_secs(15)) => {
            eprintln!("Connection to MQTT broker timed out");
            return Err("Connection timeout".into());
        }
    }
    
    // 给一点时间让事件循环处理
    tokio::time::sleep(Duration::from_secs(1)).await;
    
    Ok((client, eventloop))
}

pub async fn start() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    println!("Started receiving messages. Press Ctrl+C to disconnect...");
    
    // 持续运行，包括重连逻辑
    loop {
        // 建立连接
        let (client, mut eventloop) = match connect().await {
            Ok((client, eventloop)) => {
                println!("MQTT client connected: {:?}", client);
                
                // 订阅主题
                println!("Subscribing to topic: #");
                if let Err(e) = client.subscribe("#", rumqttc::QoS::AtLeastOnce).await {
                    eprintln!("Failed to subscribe to topic: {}", e);
                    // 等待一段时间后重试
                    tokio::time::sleep(Duration::from_secs(5)).await;
                    continue;
                }
                println!("Successfully subscribed to topic");
                (client, eventloop)
            },
            Err(e) => {
                eprintln!("Failed to connect to MQTT broker: {}", e);
                // 等待一段时间后重试
                tokio::time::sleep(Duration::from_secs(5)).await;
                continue;
            }
        };
        
        // 消息处理循环
        let mut should_reconnect = false;

        loop {
            // 使用select来同时处理消息和信号
            tokio::select! {
                // 轮询接收消息
                result = eventloop.poll() => {
                    match result {
                        Ok(event) => {
                            match event {
                                rumqttc::Event::Incoming(rumqttc::Packet::Publish(packet)) => {
                                    // 处理收到的消息
                                    let topic = packet.topic;
                                    let payload = String::from_utf8_lossy(&packet.payload);
                                    // println!("Received message on topic {}: {}", topic, payload);
                                    // 使用新的数据结构解析JSON消息
                                    if let Err(e) = handle_data(&topic, &payload).await {
                                        eprintln!("Error handling data: {}", e);
                                    }
                                },
                                rumqttc::Event::Incoming(rumqttc::Packet::SubAck(_)) => {
                                    println!("Subscription acknowledged by broker");
                                },
                                rumqttc::Event::Incoming(rumqttc::Packet::Disconnect) => {
                                    println!("Received disconnect packet from broker");
                                    should_reconnect = true;
                                    break;
                                },
                                rumqttc::Event::Outgoing(rumqttc::Outgoing::PingReq) => {
                                    // 记录ping请求发送
                                    // println!("Ping request sent to broker");
                                },
                                rumqttc::Event::Incoming(rumqttc::Packet::PingResp) => {
                                    // 收到ping响应
                                    // println!("Ping response received from broker");
                                },
                                _ => {}
                            }
                        },
                        Err(e) => {
                            // 特别处理ping相关的错误
                            let error_msg = e.to_string();
                            if error_msg.contains("Last pingreq isn't acked") {
                                eprintln!("Warning: Ping request not acknowledged, connection may be unstable");
                                // 继续尝试，但不立即断开连接
                                should_reconnect = true;
                                break;
                                // continue;
                            } else {
                                eprintln!("Error polling MQTT events: {}", e);
                                // 标记需要重连
                                should_reconnect = true;
                                break;
                            }
                        }
                    }
                },
                // 监听Ctrl+C信号
                _ = tokio::signal::ctrl_c() => {
                    println!("\nReceived Ctrl+C, disconnecting...");
                    // 先尝试优雅地断开连接
                    if let Err(e) = client.disconnect().await {
                        println!("Warning: Failed to disconnect client gracefully: {}", e);
                    }
                    
                    // 显式关闭事件循环
                    drop(eventloop);
                    
                    println!("Disconnected from MQTT broker");
                    return Ok(());
                },
                // 短暂的延时，避免CPU占用过高
                _ = tokio::time::sleep(Duration::from_millis(100)) => {
                    // 继续循环
                }
            }
        }
        
        // 如果需要重连，等待一段时间后重试
        if should_reconnect {
            eprintln!("Attempting to reconnect in 5 seconds...");
            tokio::time::sleep(Duration::from_secs(5)).await;
        }
    }
}

