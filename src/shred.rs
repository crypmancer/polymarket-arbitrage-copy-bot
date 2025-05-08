use jito_protos::shredstream::{
    shredstream_proxy_client::ShredstreamProxyClient, SubscribeEntriesRequest,
};
use dotenvy::dotenv;
use std::time::{SystemTime, UNIX_EPOCH};
use std::collections::HashSet;


#[tokio::main]
async fn main() -> Result<(), std::io::Error> {
    dotenv().ok();
    let url = std::env::var("SHRED_URL").expect("SHRED_URL must be set");   
    let mut client = ShredstreamProxyClient::connect(url)
        .await
        .unwrap();
    let mut stream = client
        .subscribe_entries(SubscribeEntriesRequest {})
        .await
        .unwrap()
        .into_inner();

    let mut processed_slots = HashSet::new();
    while let Some(slot_entry) = stream.message().await.unwrap() {
        let _entries =
            match bincode::deserialize::<Vec<solana_entry::entry::Entry>>(&slot_entry.entries) {
                Ok(e) => e,
                Err(e) => {
                    println!("Deserialization failed with err: {e}");
                    continue;
                }
            };
        
        if !processed_slots.contains(&slot_entry.slot) {
            processed_slots.insert(slot_entry.slot);
            let timestamp = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis();
            println!(
                "Slot: {}, Timestamp: {}",
                slot_entry.slot,
                timestamp
            );
        }
    }
    Ok(())
}