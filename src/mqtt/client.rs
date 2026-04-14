use crate::conf;
use rumqttc::{AsyncClient, EventLoop, MqttOptions};
use std::time::Duration;

use super::handle::handle_data;
use tokio;

#[derive(serde::Deserialize)]
pub struct MqttConfig {
    pub host: String,
    pub port: u16,
    pub username: String,
    pub password: String,
    #[serde(default)]
    pub topic_filter: String,
}

fn load_mqtt_config() -> MqttConfig {
    match conf::load_config::<MqttConfig>("MQTT") {
        Ok(conf) => conf,
        Err(e) => {
            eprintln!(
                "Failed to load configuration: {}. Using default MQTT config.",
                e
            );
            MqttConfig {
                host: "http://127.0.0.1/".to_string(),
                port: 11883,
                username: "admin".to_string(),
                password: "admin".to_string(),
                topic_filter: String::new(),
            }
        }
    }
}

fn parse_topic_filters(topic_filter: &str) -> Vec<String> {
    topic_filter
        .split(|c| matches!(c, ',' | ';' | '\n' | '\r' | '\u{FF0C}' | '\u{FF1B}'))
        .map(str::trim)
        .filter(|item| !item.is_empty())
        .map(str::to_string)
        .collect()
}

fn should_process_topic(topic: &str, topic_filters: &[String]) -> bool {
    topic_filters.is_empty() || topic_filters.iter().any(|filter| topic.contains(filter))
}

async fn connect() -> Result<(AsyncClient, EventLoop), Box<dyn std::error::Error + Send + Sync>> {
    let conf = load_mqtt_config();

    let host = conf.host.replace("http://", "").replace("https://", "");
    let mut mqtt_options = MqttOptions::new("mqtt_collect_node", &host, conf.port);
    mqtt_options.set_credentials(conf.username, conf.password);
    mqtt_options.set_keep_alive(Duration::from_secs(30));
    mqtt_options.set_clean_session(true);
    let (client, mut eventloop) = AsyncClient::new(mqtt_options, 10);

    tokio::select! {
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

        _ = tokio::time::sleep(Duration::from_secs(15)) => {
            eprintln!("Connection to MQTT broker timed out");
            return Err("Connection timeout".into());
        }
    }

    tokio::time::sleep(Duration::from_secs(1)).await;

    Ok((client, eventloop))
}

pub async fn start() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    println!("Started receiving messages. Press Ctrl+C to disconnect...");
    let topic_filters = parse_topic_filters(&load_mqtt_config().topic_filter);
    if topic_filters.is_empty() {
        println!("MQTT topic filter disabled, processing all topics");
    } else {
        println!(
            "MQTT topic filter enabled, processing topics containing any of: {}",
            topic_filters.join(", ")
        );
    }

    loop {
        let (client, mut eventloop) = match connect().await {
            Ok((client, eventloop)) => {
                println!("MQTT client connected: {:?}", client);

                println!("Subscribing to topic: #");
                if let Err(e) = client.subscribe("#", rumqttc::QoS::AtLeastOnce).await {
                    eprintln!("Failed to subscribe to topic: {}", e);
                    tokio::time::sleep(Duration::from_secs(5)).await;
                    continue;
                }
                println!("Successfully subscribed to topic");
                (client, eventloop)
            }
            Err(e) => {
                eprintln!("Failed to connect to MQTT broker: {}", e);
                tokio::time::sleep(Duration::from_secs(5)).await;
                continue;
            }
        };

        let should_reconnect = loop {
            tokio::select! {
                result = eventloop.poll() => {
                    match result {
                        Ok(event) => {
                            match event {
                                rumqttc::Event::Incoming(rumqttc::Packet::Publish(packet)) => {
                                    let topic = packet.topic;
                                    if !should_process_topic(&topic, &topic_filters) {
                                        continue;
                                    }

                                    let payload = String::from_utf8_lossy(&packet.payload);
                                    if let Err(e) = handle_data(&topic, &payload).await {
                                        eprintln!("Error handling data: {}", e);
                                    }
                                },
                                rumqttc::Event::Incoming(rumqttc::Packet::SubAck(_)) => {
                                    println!("Subscription acknowledged by broker");
                                },
                                rumqttc::Event::Incoming(rumqttc::Packet::Disconnect) => {
                                    println!("Received disconnect packet from broker");
                                    break true;
                                },
                                rumqttc::Event::Outgoing(rumqttc::Outgoing::PingReq) => {},
                                rumqttc::Event::Incoming(rumqttc::Packet::PingResp) => {},
                                _ => {}
                            }
                        },
                        Err(e) => {
                            let error_msg = e.to_string();
                            if error_msg.contains("Last pingreq isn't acked") {
                                eprintln!("Warning: Ping request not acknowledged, connection may be unstable");
                                break true;
                            } else {
                                eprintln!("Error polling MQTT events: {}", e);
                                break true;
                            }
                        }
                    }
                },
                _ = tokio::signal::ctrl_c() => {
                    println!("\nReceived Ctrl+C, disconnecting...");
                    if let Err(e) = client.disconnect().await {
                        println!("Warning: Failed to disconnect client gracefully: {}", e);
                    }

                    drop(eventloop);

                    println!("Disconnected from MQTT broker");
                    return Ok(());
                },
                _ = tokio::time::sleep(Duration::from_millis(100)) => {}
            }
        };

        if should_reconnect {
            eprintln!("Attempting to reconnect in 5 seconds...");
            tokio::time::sleep(Duration::from_secs(5)).await;
        }
    }
}
