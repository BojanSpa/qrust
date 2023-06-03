use log::LevelFilter as LogLevel;
use log::{error, info};
use std::io::Write;

use chrono::Local as LocalDateTime;
use env_logger::Builder as LogBuilder;
use env_logger::Target as LogTarget;

use data::config::DataConfig;
use data::provider::{DataProvider, SymbolsProvider};
use data::AssetCategory;
use extensions::datetime;

mod data;
mod extensions;

#[tokio::main]
async fn main() {
    setup_logger();

    // all_symbols().await;
    sync_test().await;
}

async fn all_symbols() {
    let config = DataConfig::new();
    let provider = SymbolsProvider::new(config, AssetCategory::Usdm);
    provider.get().await.unwrap();
}

async fn sync_test() {
    let config = DataConfig::new();
    let provider = DataProvider::new(config, AssetCategory::Usdm);
    let sync_result = provider
        .sync("BNBBUSD", &datetime::create_utc(2020, 1, 1))
        .await;
    match sync_result {
        Ok(_) => info!("Sync completed successfully"),
        Err(e) => error!("Sync failed: {}", e),
    }
}

fn setup_logger() {
    LogBuilder::new()
        .target(LogTarget::Stdout)
        .format(|buffer, record| {
            writeln!(
                buffer,
                "{} - [{}] - {}",
                LocalDateTime::now().format("%Y-%m-%d %H:%M:%S"),
                record.level(),
                record.args()
            )
        })
        .filter(None, LogLevel::Info)
        .init();
}
