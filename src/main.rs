#![allow(dead_code)]

use log::LevelFilter as LogLevel;
use std::io::{Result, Write};

use chrono::Local as LocalDateTime;
use env_logger::Target as LogTarget;
use env_logger::{fmt, Builder as LogBuilder};

use data::config::DataConfig;
use data::provider::{DataProvider, SymbolsProvider};
use data::store::DataStore;
use data::{AssetCategory, Symbol};
use extensions::datetime;

mod data;
mod extensions;

#[tokio::main]
async fn main() {
    // let (sender, receiver): (Sender<String>, Receiver<String>) = channel();

    let log_handler = LogHandler::new();
    let log_target = fmt::Target::Pipe(Box::new(log_handler));
    setup_logger(log_target, LogLevel::Info);

    // all_symbols().await;
    sync_test().await;
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
        let data_store = DataStore::new_arc(config.clone(), AssetCategory::Usdm);
        data_store.sync(symbols);
    });

    sync_task.await.unwrap();
}

// async fn sync_test() {
//     let config = DataConfig::new();
//     let provider = DataProvider::new(config, AssetCategory::Usdm);
//     let sync_result = provider
//         .sync("BNBBUSD", &datetime::create_utc(2020, 1, 1))
//         .await;
//     match sync_result {
//         Ok(_) => info!("Sync completed successfully"),
//         Err(e) => error!("Sync failed: {}", e),
//     }
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
