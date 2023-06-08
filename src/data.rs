use chrono::{DateTime, Utc};

pub mod config;
pub mod provider;
pub mod sanitizer;
pub mod store;

#[derive(Clone)]
pub enum AssetCategory {
    Spot,
    Usdm,
    Coinm,
}
impl AssetCategory {
    pub fn as_str(&self) -> &'static str {
        match self {
            AssetCategory::Spot => "spot",
            AssetCategory::Usdm => "um",
            AssetCategory::Coinm => "cm",
        }
    }

    pub fn as_value(value: &str) -> AssetCategory {
        match value {
            "spot" => AssetCategory::Spot,
            "um" => AssetCategory::Usdm,
            "cm" => AssetCategory::Coinm,
            _ => panic!("Invalid asset category: {}", value),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Symbol {
    pub name: String,
    pub initdate: DateTime<Utc>,
}

pub struct Column;
impl Column {
    pub const OPEN_TIME: &'static str = "open_time";
    pub const OPEN: &'static str = "open";
    pub const HIGH: &'static str = "high";
    pub const LOW: &'static str = "low";
    pub const CLOSE: &'static str = "close";
    pub const VOLUME: &'static str = "volume";
    pub const QUOTE_VOLUME: &'static str = "quote_volume";
    pub const COUNT: &'static str = "count";
    pub const TAKER_BUY_VOLUME: &'static str = "taker_buy_volume";
    pub const TAKER_BUY_QUOTE_VOLUME: &'static str = "taker_buy_quote_volume";
    pub const LOG_RETURNS: &'static str = "log_returns";
    pub const CUM_RETURNS: &'static str = "cum_returns";
}
