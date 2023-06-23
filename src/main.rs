#![allow(dead_code)]

use log::LevelFilter as LogLevel;
use std::io::{Result, Write};
use std::sync::{Arc, Mutex};
use strategy::Strategy;

use chrono::Local as LocalDateTime;
use env_logger::Target as LogTarget;
use env_logger::{fmt, Builder as LogBuilder};
use tokio::sync::mpsc;

use broker::{Balance, Broker};
use data::config::DataConfig;
use data::provider::{DataProvider, SymbolsProvider};
use data::sources::{EventSource, EventSourceOptions, StoreEventSource};
use data::store::DataStore;
use data::{AssetCategory, Symbol};
use extensions::datetime;
use signals::ema_signals::EmaCrossSignal;
use signals::SignalGenerator;

mod broker;
mod data;
mod events;
mod extensions;
mod signals;
mod strategy;
mod ta;

const DEFAULT_CHANNEL_SIZE: usize = 100;

#[tokio::main]
async fn main() {
    // let (sender, receiver): (Sender<String>, Receiver<String>) = channel();

    let log_handler = LogHandler::new();
    let log_target = fmt::Target::Pipe(Box::new(log_handler));
    setup_logger(log_target, LogLevel::Info);

    // all_symbols().await;
    // sync_test().await;
    // event_test().await;
}

async fn all_symbols() {
    let config = DataConfig::new(AssetCategory::Usdm);
    let provider = SymbolsProvider::new(config, AssetCategory::Usdm);
    provider.get().await.unwrap();
}

// async fn sync_test() {
//     let config = DataConfig::new(AssetCategory::Usdm);

//     // let symbols_provider = SymbolsProvider::new(config.clone(), AssetCategory::Usdm);
//     // let symbols = symbols_provider.get().await.unwrap();

//     let symbols = vec![Symbol {
//         name: "BTCUSDT".to_string(),
//         initdate: datetime::create_utc(2020, 1, 1),
//     }];

//     let sync_task = tokio::task::spawn_blocking(move || {
//         let data_store = DataStore::new_arc(config);
//         data_store.sync(symbols);
//     });

//     sync_task.await.unwrap();
// }

async fn event_test() {
    let symbol = "BTCUSDT".to_string();
    let timeframe = Some("5m".to_string());

    let (event_sender, event_receiver) = mpsc::channel(DEFAULT_CHANNEL_SIZE);
    let (order_sender, order_receiver) = mpsc::channel(DEFAULT_CHANNEL_SIZE);

    let config = DataConfig::new(AssetCategory::Usdm);
    let options = EventSourceOptions { symbol, timeframe };

    let signal_generator = EmaCrossSignal::new(10, 20);
    let lookback = signal_generator.get_threshold();

    let balance = Arc::new(Mutex::new(Balance::new(1000.0)));

    let mut broker = Broker::new(Arc::clone(&balance), order_receiver);
    tokio::spawn(async move {
        broker.start().await;
    });

    let source = StoreEventSource::new(config, options, event_sender.clone());
    tokio::spawn(async move {
        match source.start(lookback).await {
            Ok(_) => println!("Done"),
            Err(e) => log::error!("Error: {}", e),
        }
    });

    let mut strategy = Strategy::new(
        event_receiver,
        order_sender,
        signal_generator,
        Arc::clone(&balance),
    );
    strategy.run().await;
}

fn setup_logger(target: LogTarget, level: LogLevel) {
    LogBuilder::new()
        .target(target)
        .format(|buffer, record| {
            writeln!(
                buffer,
                "{} - [{}] - {}",
                LocalDateTime::now().format("%Y-%m-%d %H:%M:%S"),
                record.level(),
                record.args()
            )
        })
        .filter(None, level)
        .init();
}

#[derive(Default)]
pub struct LogHandler {}

impl LogHandler {
    pub fn new() -> LogHandler {
        LogHandler {}
    }
}

impl Write for LogHandler {
    fn write(&mut self, buf: &[u8]) -> Result<usize> {
        let msg = String::from_utf8_lossy(buf);
        print!("Received: {}", msg);
        Ok(buf.len())
    }

    fn flush(&mut self) -> Result<()> {
        Ok(())
    }
}
