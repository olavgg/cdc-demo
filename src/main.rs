
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::message::Message;
use rdkafka::error::KafkaResult;
use serde_json::{Value};
use chrono::{DateTime, Utc};
use chrono::serde::ts_milliseconds;
use serde::{Deserialize, Serialize};

fn default_assets() -> Vec<Asset> {
    Vec::new()
}
#[derive(Debug, Serialize, Deserialize, Clone)]
struct WorkPermit {
    id: i64,
    description: String,
    status: String,
    #[serde(rename = "type")]
    r#type: Option<String>,
    responsible_person: String,
    #[serde(with = "ts_milliseconds")]
    valid_from: DateTime<Utc>,
    #[serde(with = "ts_milliseconds")]
    valid_to: DateTime<Utc>,
    authorized_by: String,
    location: String,
    #[serde(default = "default_assets")]
    #[serde(skip_serializing_if = "Vec::is_empty")]
    assets: Vec<Asset>,
    permit_number: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct Datapoint {
    id: i64,
    #[serde(with = "chrono::serde::ts_nanoseconds")]
    timestamp: DateTime<Utc>,
    value: f64,
    asset_id: i64,
}

#[derive(Debug, Serialize, Deserialize)]
struct PermitAsset {
    permit_id: i64,
    asset_id: i64,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct Asset {
    id: i64,
    tag: String,
    name: String,
    description: String,
    status: String,
    #[serde(with = "ts_milliseconds")]
    date_created: DateTime<Utc>,
    #[serde(with = "ts_milliseconds")]
    last_updated: DateTime<Utc>,
}

// Define the topics to subscribe to
const TOPICS: &[&str] = &[
    "datapoints",
    "work-permit",
    "asset",
    "permit-asset",
];

#[tokio::main]
async fn main() -> KafkaResult<()> {

    let mut assets: Vec<Asset> = vec![];
    let mut work_permits: Vec<WorkPermit> = vec![];

    // 1. Configure the consumer
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", "localhost:9092") // Your Kafka Broker
        .set("group.id", "rust_consumer_group")     // Consumer Group ID
        .set("enable.auto.commit", "false")         // Manual offset control
        .set("auto.offset.reset", "earliest")       // Start from the beginning if no offset exists
        .create()
        .expect("Consumer creation failed");

    // 2. Subscribe to the topics
    consumer.subscribe(TOPICS)
        .expect("Can't subscribe to specified topics");

    println!("Consumer started and subscribed to: {:?}", TOPICS);

    // 3. Start the consumption loop
    loop {
        match consumer.recv().await {
            Ok(message) => {
                let topic = message.topic();

                let payload_str = match message.payload() {
                    Some(payload) => String::from_utf8_lossy(payload),
                    None => "None".into(),
                };

                let formatted_data = match serde_json::from_str::<Value>(&payload_str) {
                    Ok(mut root_value) => {
                        // 1. Try to extract the 'after' object via optional chaining
                        root_value
                            .get_mut("payload") // Get the outer 'payload' object (Option<&mut Value>)
                            .and_then(|payload|
                                payload.get_mut("after")
                            ).cloned() // Get the 'after' object (Option<&mut Value>)
                    }
                    Err(_) => {
                        // Failure: The payload is not valid JSON
                        eprintln!("Error: Payload is not valid JSON or is unparseable.");
                        None
                    }
                };

                match topic {
                    "datapoints" => {
                        if let Some(data) = formatted_data {
                            match serde_json::from_value::<Datapoint>(data.clone()) {
                                Ok(data) => {
                                    println!("✅ DATAPOINT Received: {:#?}", data);
                                    work_permits.iter_mut().for_each(| permit| {
                                        permit.assets.iter().for_each( | asset | {
                                           if asset.id == data.asset_id {
                                               println!("✅✅✅ Work permit: {:#?} is connected to \
                                               this data point.", permit.permit_number);
                                           }
                                        });
                                    });
                                },
                                Err(e) => eprintln!("❌ DATAPOINT Deserialization Error: {} \n{}", e, data),
                            }
                        } else {
                            eprintln!("❌ DATAPOINT: No valid data found in message");
                        }
                    }
                    "work-permit" => {
                        if let Some(data) = formatted_data {
                            match serde_json::from_value::<WorkPermit>(data.clone()) {
                                Ok(data) => {
                                    work_permits.push(data.clone());
                                    println!("✅ WORK PERMIT Received: {:#?}", data)
                                },
                                Err(e) => eprintln!("❌ WORK PERMIT Deserialization Error: {} \n{}", e, data),
                            }
                        } else {
                            eprintln!("❌ WORK PERMIT: No valid data found in message");
                        }
                    }
                    "permit-asset" => {
                        if let Some(data) = formatted_data {
                            match serde_json::from_value::<PermitAsset>(data.clone()) {
                                Ok(data) => {
                                    work_permits.iter_mut().for_each( | permit| {
                                        if permit.id == data.permit_id {
                                            permit.assets.push(
                                                assets.iter()
                                                    .find(|asset| asset.id == data.asset_id)
                                                    .unwrap()
                                                    .clone());
                                            println!("✅ Successfully added asset id: {:#?} to permit id: {:#?}",
                                                     data.asset_id, data.permit_id);
                                        }
                                    });
                                    println!("✅ PermitAsset Received: {:#?}", data)
                                },
                                Err(e) => eprintln!("❌ PermitAsset Deserialization Error: {} \n{}", e, data),
                            }
                        } else {
                            eprintln!("❌ PermitAsset: No valid data found in message");
                        }
                    }
                    "asset" => {
                        if let Some(data) = formatted_data {
                            match serde_json::from_value::<Asset>(data.clone()) {
                                Ok(data) => {
                                    assets.push(data.clone());
                                    println!("✅ ASSET Received: {:#?}", data)
                                },
                                Err(e) => eprintln!("❌ ASSET Deserialization Error: {} \n{}", e, data),
                            }
                        } else {
                            eprintln!("❌ ASSET: No valid data found in message");
                        }
                    }
                    _ => {
                        println!("Ignoring message from unknown topic: {}", topic);
                    }
                }

                // Manually commit the offset after processing (optional but recommended)
                consumer.commit_message(&message, rdkafka::consumer::CommitMode::Async)
                    .expect("TODO: panic message");
            }
            Err(e) => {
                eprintln!("Kafka error: {}", e);
            }
        }
    }

}
