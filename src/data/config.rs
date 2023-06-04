use std::fs;

use serde::Deserialize;

use crate::data::AssetCategory;

#[derive(Deserialize)]
struct RawDataConfig {
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

impl RawDataConfig {
    pub fn new() -> RawDataConfig {
        let content = fs::read_to_string("Config.toml").unwrap();
        toml::from_str(&content).unwrap()
    }
}

#[derive(Clone)]
pub struct DataConfig {
    pub base_raw_dir: String,
    pub base_store_dir: String,

    pub info_uri: String,
    pub klines_uri: String,
    pub hist_klines_monthly_uri: String,
    pub hist_klines_daily_uri: String,

    pub download_file_format: String,
    pub date_format_monthly: String,
    pub date_format_daily: String,
}

impl DataConfig {
    pub fn new(asset_cat: AssetCategory) -> DataConfig {
        let rawc = RawDataConfig::new();
        let (info_uri, klines_uri, hist_monthly_klines_uri, hist_daily_klines_uri) = match asset_cat
        {
            AssetCategory::Spot => (
                rawc.spot_info_uri,
                rawc.spot_klines_uri,
                rawc.spot_hist_klines_monthly_uri,
                rawc.spot_hist_klines_daily_uri,
            ),
            AssetCategory::Usdm => (
                rawc.usdm_info_uri,
                rawc.usdm_klines_uri,
                rawc.usdm_hist_klines_monthly_uri,
                rawc.usdm_hist_klines_daily_uri,
            ),
            AssetCategory::Coinm => (
                rawc.coinm_info_uri,
                rawc.coinm_klines_uri,
                rawc.coinm_hist_klines_monthly_uri,
                rawc.coinm_hist_klines_daily_uri,
            ),
        };

        DataConfig {
            base_raw_dir: rawc.base_raw_dir.clone(),
            base_store_dir: rawc.base_store_dir.clone(),
            info_uri: info_uri.clone(),
            klines_uri: klines_uri.clone(),
            hist_klines_monthly_uri: hist_monthly_klines_uri.clone(),
            hist_klines_daily_uri: hist_daily_klines_uri.clone(),
            download_file_format: rawc.download_file_format.clone(),
            date_format_monthly: rawc.date_format_monthly.clone(),
            date_format_daily: rawc.date_format_daily.clone(),
        }
    }
}
