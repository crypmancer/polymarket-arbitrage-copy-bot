use std::time::{SystemTime, UNIX_EPOCH};
use std::collections::HashMap;
use tokio::sync::mpsc;
use tokio::time::{Duration, Instant};
use futures_util::StreamExt;
use futures_util::SinkExt;
use dotenvy::dotenv;
use chrono::Local;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

struct Stats {
    grpc_first: AtomicU64,
    shred_first: AtomicU64,
    grpc_delay_sum: AtomicU64,
    shred_delay_sum: AtomicU64,
    grpc_delay_count: AtomicU64,
    shred_delay_count: AtomicU64,
}

impl Stats {
    fn new() -> Self {
        Self {
            grpc_first: AtomicU64::new(0),
            shred_first: AtomicU64::new(0),
            grpc_delay_sum: AtomicU64::new(0),
            shred_delay_sum: AtomicU64::new(0),
            grpc_delay_count: AtomicU64::new(0),
            shred_delay_count: AtomicU64::new(0),
        }
    }

    fn print_stats(&self) {
        let total = self.grpc_first.load(Ordering::Relaxed) + self.shred_first.load(Ordering::Relaxed);
        let grpc_first_percent = (self.grpc_first.load(Ordering::Relaxed) as f64 / total as f64) * 100.0;
        let shred_first_percent = (self.shred_first.load(Ordering::Relaxed) as f64 / total as f64) * 100.0;
        
        let grpc_avg_delay = if self.grpc_delay_count.load(Ordering::Relaxed) > 0 {
            self.grpc_delay_sum.load(Ordering::Relaxed) as f64 / self.grpc_delay_count.load(Ordering::Relaxed) as f64
        } else { 0.0 };
        
        let shred_avg_delay = if self.shred_delay_count.load(Ordering::Relaxed) > 0 {
            self.shred_delay_sum.load(Ordering::Relaxed) as f64 / self.shred_delay_count.load(Ordering::Relaxed) as f64
        } else { 0.0 };

        let overall_grpc_avg = (self.grpc_delay_sum.load(Ordering::Relaxed) as f64) / total as f64;
        let overall_shred_avg = (self.shred_delay_sum.load(Ordering::Relaxed) as f64) / total as f64;

        println!("[{}] INFO: ===== Endpoint Performance Comparison =====", Local::now().format("%H:%M:%S%.3f"));
        println!("[{}] INFO: GRPC   : First received {:6.2}%, avg delay when behind {:6.2}ms, overall avg delay {:6.2}ms", 
            Local::now().format("%H:%M:%S%.3f"),
            grpc_first_percent,
            grpc_avg_delay,
            overall_grpc_avg
        );
        println!("[{}] INFO: SHRED  : First received {:6.2}%, avg delay when behind {:6.2}ms, overall avg delay {:6.2}ms", 
            Local::now().format("%H:%M:%S%.3f"),
            shred_first_percent,
            shred_avg_delay,
            overall_shred_avg
        );
    }
}

async fn run_grpc_client(tx: mpsc::Sender<(u64, u128)>) {
    dotenv().ok();
    let url = std::env::var("GRPC_URL").expect("GRPC_URL must be set");
    let mut client = yellowstone_grpc_client::GeyserGrpcClient::build_from_shared(url)
        .unwrap()
        .tls_config(yellowstone_grpc_client::ClientTlsConfig::new().with_native_roots())
        .unwrap()
        .connect()
        .await
        .unwrap();

    let subscribe_request = yellowstone_grpc_proto::geyser::SubscribeRequest {
        transactions: std::collections::HashMap::from([(
            "client".to_string(),
            yellowstone_grpc_proto::geyser::SubscribeRequestFilterTransactions {
                vote: Some(false),
                failed: Some(false),
                signature: None,
                account_include: vec![],
                account_exclude: vec![],
                account_required: vec![],
            },
        )]),
        commitment: Some(yellowstone_grpc_proto::geyser::CommitmentLevel::Processed.into()),
        ..Default::default()
    };

    let (mut subscribe_tx, mut stream) = client
        .subscribe_with_request(Some(subscribe_request))
        .await
        .unwrap();

    let mut last_slot = 0;
    while let Some(message) = stream.next().await {
        match message {
            Ok(msg) => {
                match msg.update_oneof {
                    Some(yellowstone_grpc_proto::geyser::subscribe_update::UpdateOneof::Transaction(sut)) => {
                        if sut.slot != last_slot {
                            last_slot = sut.slot;
                            let timestamp = SystemTime::now()
                                .duration_since(UNIX_EPOCH)
                                .unwrap()
                                .as_millis();
                            let _ = tx.send((sut.slot, timestamp)).await;
                        }
                    }
                    Some(yellowstone_grpc_proto::geyser::subscribe_update::UpdateOneof::Ping(_)) => {
                        let _ = subscribe_tx
                            .send(yellowstone_grpc_proto::geyser::SubscribeRequest {
                                ping: Some(yellowstone_grpc_proto::geyser::SubscribeRequestPing { id: 1 }),
                                ..Default::default()
                            })
                            .await;
                    }
                    _ => {}
                }
            }
            Err(_) => break,
        }
    }
}

async fn run_shred_client(tx: mpsc::Sender<(u64, u128)>) {
    dotenv().ok();
    let url = std::env::var("SHRED_URL").expect("SHRED_URL must be set");
    let mut client = jito_protos::shredstream::shredstream_proxy_client::ShredstreamProxyClient::connect(url)
        .await
        .unwrap();
    let mut stream = client
        .subscribe_entries(jito_protos::shredstream::SubscribeEntriesRequest {})
        .await
        .unwrap()
        .into_inner();

    let mut processed_slots = std::collections::HashSet::new();
    while let Some(slot_entry) = stream.message().await.unwrap() {
        let _entries = match bincode::deserialize::<Vec<solana_entry::entry::Entry>>(&slot_entry.entries) {
            Ok(e) => e,
            Err(_) => continue,
        };

        if !processed_slots.contains(&slot_entry.slot) {
            processed_slots.insert(slot_entry.slot);
            let timestamp = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis();
            let _ = tx.send((slot_entry.slot, timestamp)).await;
        }
    }
}

#[tokio::main]
async fn main() {
    println!("[{}] INFO: Starting GRPC and SHRED service performance comparison...", Local::now().format("%H:%M:%S%.3f"));
    println!("[{}] INFO: Test duration: 30 seconds", Local::now().format("%H:%M:%S%.3f"));
    println!("[{}] INFO: Test endpoints: GRPC, SHRED", Local::now().format("%H:%M:%S%.3f"));

    let (grpc_tx, mut grpc_rx) = mpsc::channel::<(u64, u128)>(100);
    let (shred_tx, mut shred_rx) = mpsc::channel::<(u64, u128)>(100);

    tokio::spawn(run_grpc_client(grpc_tx));
    tokio::spawn(run_shred_client(shred_tx));

    let mut grpc_slots = HashMap::new();
    let mut shred_slots = HashMap::new();
    let stats = Arc::new(Stats::new());
    let stats_clone = stats.clone();

    let start_time = SystemTime::now();
    let mut first_slot_received = false;

    loop {
        tokio::select! {
            Some((slot, timestamp)) = grpc_rx.recv() => {
                grpc_slots.insert(slot, timestamp);
                if let Some(shred_ts) = shred_slots.get(&slot) {
                    let diff = timestamp as i128 - *shred_ts as i128;
                    if !first_slot_received {
                        println!("[{}] INFO: All endpoints have received the first slot, starting official statistics...", 
                            Local::now().format("%H:%M:%S%.3f"));
                        first_slot_received = true;
                    }
                    if diff < 0 {
                        stats.grpc_first.fetch_add(1, Ordering::Relaxed);
                        stats.shred_delay_sum.fetch_add((-diff) as u64, Ordering::Relaxed);
                        stats.shred_delay_count.fetch_add(1, Ordering::Relaxed);
                    } else {
                        stats.shred_first.fetch_add(1, Ordering::Relaxed);
                        stats.grpc_delay_sum.fetch_add(diff as u64, Ordering::Relaxed);
                        stats.grpc_delay_count.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
            Some((slot, timestamp)) = shred_rx.recv() => {
                shred_slots.insert(slot, timestamp);
                if let Some(grpc_ts) = grpc_slots.get(&slot) {
                    let diff = timestamp as i128 - *grpc_ts as i128;
                    if !first_slot_received {
                        println!("[{}] INFO: All endpoints have received the first slot, starting official statistics...", 
                            Local::now().format("%H:%M:%S%.3f"));
                        first_slot_received = true;
                    }
                    if diff < 0 {
                        stats.shred_first.fetch_add(1, Ordering::Relaxed);
                        stats.grpc_delay_sum.fetch_add((-diff) as u64, Ordering::Relaxed);
                        stats.grpc_delay_count.fetch_add(1, Ordering::Relaxed);
                    } else {
                        stats.grpc_first.fetch_add(1, Ordering::Relaxed);
                        stats.shred_delay_sum.fetch_add(diff as u64, Ordering::Relaxed);
                        stats.shred_delay_count.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
        }

        if let Ok(duration) = start_time.elapsed() {
            if duration.as_secs() >= 30 {
                stats_clone.print_stats();
                break;
            }
        }
    }
}
