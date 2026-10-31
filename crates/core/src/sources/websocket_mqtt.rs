use crate::error::Result;
use crate::impl_source_component;
use crate::pipeline::{Message, Sender, Source};
use rumqttc::{AsyncClient, MqttOptions, QoS};
use std::time::Duration;
use tracing::{debug, error};

/// A pipeline source that subscribes to an MQTT topic over WebSockets.
#[derive(Clone, Debug)]
pub struct WebSocketMqttSource {
    url: String,
    topic: String,
    client_id: String,
}

impl Default for WebSocketMqttSource {
    fn default() -> Self {
        Self {
            url: "ws://localhost:8883".to_string(),
            topic: "test".to_string(),
            client_id: "test_client".to_string(),
        }
    }
}

impl WebSocketMqttSource {
    pub fn new(
        url: impl Into<String>,
        topic: impl Into<String>,
        client_id: impl Into<String>,
    ) -> Self {
        Self {
            url: url.into(),
            topic: topic.into(),
            client_id: client_id.into(),
        }
    }
}

impl Source for WebSocketMqttSource {
    type Output = String;

    async fn run(&self, output: Sender<Self::Output>) -> Result<()> {
        debug!("WebSocketMqttSource starting");

        let url_with_client = format!("{}?client_id={}", self.url, self.client_id);
        let mut mqtt_options = MqttOptions::parse_url(&url_with_client)?;
        mqtt_options.set_keep_alive(Duration::from_secs(20));
        debug!("MQTT Options: {:?}", mqtt_options);

        let (client, mut eventloop) = AsyncClient::new(mqtt_options, 10);
        client.subscribe(&self.topic, QoS::AtLeastOnce).await?;

        debug!(
            "Connected to MQTT broker and subscribed to topic: {}",
            self.topic
        );

        loop {
            match eventloop.poll().await {
                Ok(rumqttc::Event::Incoming(rumqttc::Packet::Publish(msg))) => {
                    if let Ok(payload) = String::from_utf8(msg.payload.to_vec()) {
                        if output.send(Message::new(payload)).await.is_err() {
                            break;
                        }
                    }
                }
                Ok(_) => {}
                Err(e) => {
                    error!("MQTT connection error: {}", e);
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            }
        }

        if let Err(e) = client.disconnect().await {
            error!("Error disconnecting from MQTT broker: {}", e);
        }

        debug!("WebSocketMqttSource completed");
        Ok(())
    }
}

impl_source_component!(WebSocketMqttSource, String);
