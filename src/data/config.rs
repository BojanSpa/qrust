use serde::Deserialize;
use std::fs;

#[derive(Deserialize)]
pub struct DataConfig {
    pub base_raw_dir: String,
    pub base_store_dir: String,

    pub spot_info_uri: String,
    pub usdm_info_uri: String,
    pub coinm_info_uri: String,

    pub spot_klines_uri: String,
    pub usdm_klines_uri: String,
    pub coinm_klines_uri: String,

    pub spot_hist_klines_monthly_uri: String,
    pub usdm_hist_klines_monthly_uri: String,
    pub coinm_hist_klines_monthly_uri: String,

    pub spot_hist_klines_daily_uri: String,
    pub usdm_hist_klines_daily_uri: String,
    pub coinm_hist_klines_daily_uri: String,

    pub download_file_format: String,
    pub date_format_monthly: String,
    pub date_format_daily: String,
}

impl DataConfig {
    pub fn new() -> DataConfig {
        let content = fs::read_to_string("Config.toml").unwrap();
        toml::from_str(&content).unwrap()
    }
}
