#![allow(dead_code)]

use log::LevelFilter as LogLevel;
use std::io::{Result, Write};

use chrono::Local as LocalDateTime;
use env_logger::Target as LogTarget;
use env_logger::{fmt, Builder as LogBuilder};
use tokio::sync::mpsc;

use data::config::DataConfig;
use data::provider::{DataProvider, SymbolsProvider};
use data::store::DataStore;
use data::{AssetCategory, Symbol};
use event::handler::EventHandler;
use event::sources::{EventSource, EventSourceOptions, StoreEventSource};
use extensions::datetime;

mod data;
mod event;
mod extensions;
mod signals;
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

async fn sync_test() {
    let config = DataConfig::new(AssetCategory::Usdm);

    // let symbols_provider = SymbolsProvider::new(config.clone(), AssetCategory::Usdm);
    // let symbols = symbols_provider.get().await.unwrap();

    let symbols = vec![Symbol {
        name: "BTCUSDT".to_string(),
        initdate: datetime::create_utc(2020, 1, 1),
    }];

    let sync_task = tokio::task::spawn_blocking(move || {
        let data_store = DataStore::new_arc(config);
        data_store.sync(symbols);
    });

    sync_task.await.unwrap();
}

// async fn event_test() {
//     let symbol = "BTCUSDT".to_string();
//     let timeframe = Some("5m".to_string());

//     let (sender, receiver) = mpsc::channel(DEFAULT_CHANNEL_SIZE);

//     let config = DataConfig::new(AssetCategory::Usdm);
//     let options = EventSourceOptions { symbol, timeframe };

//     let source = StoreEventSource::new(config, options, sender.clone());

//     let strat = EmaCrossStrat::new(10, 20);
//     let lookback = strat.get_threshold() * 5;
//     let mut handler = EventHandler::new(strat, receiver);

//     tokio::spawn(async move {
//         match source.start(lookback).await {
//             Ok(_) => println!("Done"),
//             Err(e) => log::error!("Error: {}", e),
//         }
//     });

//     handler.listen().await;
// }

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
