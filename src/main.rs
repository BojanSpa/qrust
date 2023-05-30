use log::LevelFilter as LogLevel;
use log::{error, info};
use std::io::Write;

use chrono::Local as LocalDateTime;
use env_logger::Builder as LogBuilder;
use env_logger::Target as LogTarget;

use data::config::DataConfig;
use data::provider::DataProvider;
use data::AssetCategory;
use extensions::datetime;

mod data;
mod extensions;

fn main() {
    setup_logger();

    let config = DataConfig::new();
    let provider = DataProvider::new(config, AssetCategory::Usdm);
    let sync_result = provider.sync("BNBBUSD", &datetime::create_utc(2020, 1, 1));
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
