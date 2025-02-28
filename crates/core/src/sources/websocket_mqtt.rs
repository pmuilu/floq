use crate::pipeline::{PipelineComponent, ComponentContext, Message};
use crate::pipeline::channel::{Sender, Receiver};
use std::sync::Arc;
use tracing::{debug, error};
use rumqttc::{MqttOptions, AsyncClient, QoS};
use std::time::Duration;

pub struct WebSocketMqttSource {
    url: String,
    topic: String,
    client_id: String,
}

impl WebSocketMqttSource {
    pub fn new(url: String, topic: String, client_id: String) -> Self {
        WebSocketMqttSource { 
            url,
            topic,
            client_id,
        }
    }
}

impl PipelineComponent for WebSocketMqttSource {
    type Input = ();
    type Output = String;

    fn new() -> Self {
        WebSocketMqttSource { 
            url: "ws://localhost:8883".to_string(),
            topic: "test".to_string(),
            client_id: "test_client".to_string(),
        }
    }

    async fn run(&self, _input: Receiver<Self::Input>, output: Sender<Self::Output>, _context: Arc<ComponentContext<Self::Input, Self::Output>>) {
        debug!("WebSocketMqttSource starting");

        // Parse URL with client_id
        let url_with_client = format!("{}?client_id={}", self.url, self.client_id);
        let mut mqtt_options = match MqttOptions::parse_url(&url_with_client) {
            Ok(options) => options,
            Err(e) => {
                error!("Failed to parse MQTT URL: {}", e);
                return;
            }
        };

        println!("mqtt_options: {:?}", mqtt_options);

        mqtt_options.set_keep_alive(Duration::from_secs(20));
        debug!("MQTT Options: {:?}", mqtt_options);

        let (client, mut eventloop) = AsyncClient::new(mqtt_options, 10);

        // Subscribe to topic
        if let Err(e) = client.subscribe(&self.topic, QoS::AtLeastOnce).await {
            error!("Failed to subscribe to topic: {}", e);
            return;
        }

        debug!("Connected to MQTT broker and subscribed to topic: {}", self.topic);

        // Process incoming messages
        loop {
            match eventloop.poll().await {
                Ok(notification) => {
                    if let rumqttc::Event::Incoming(rumqttc::Packet::Publish(msg)) = notification {
                        if let Ok(payload) = String::from_utf8(msg.payload.to_vec()) {
                            if let Err(e) = output.send(Message::new(payload)) {
                                error!("Failed to send message to output: {}", e);
                                break;
                            }
                        }
                    }
                }
                Err(e) => {
                    error!("MQTT connection error: {}", e);
                    // Add a small delay before retrying
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            }
        }

        // Cleanup
        if let Err(e) = client.disconnect().await {
            error!("Error disconnecting from MQTT broker: {}", e);
        }

        debug!("WebSocketMqttSource completed");
    }
}
