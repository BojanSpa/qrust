use chrono::Local as LocalDateTime;
use env_logger::Builder as LogBuilder;
use env_logger::Target as LogTarget;
use log::LevelFilter as LogLevel;
use std::io::Write;

mod data;

fn main() {
    setup_logger();

    // let cat = AssetCategory::by_str("um");
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
