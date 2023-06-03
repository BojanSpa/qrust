use serde::Deserialize;
use std::fs;

use crate::data::AssetCategory;

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

    pub fn info_uri_for(&self, asset_cat: &AssetCategory) -> &str {
        match asset_cat {
            AssetCategory::Spot => &self.spot_info_uri,
            AssetCategory::Usdm => &self.usdm_info_uri,
            AssetCategory::Coinm => &self.coinm_info_uri,
        }
    }

    pub fn daily_hist_klines_uri_for(&self, asset_cat: &AssetCategory) -> &str {
        match asset_cat {
            AssetCategory::Spot => &self.spot_hist_klines_daily_uri,
            AssetCategory::Usdm => &self.usdm_hist_klines_daily_uri,
            AssetCategory::Coinm => &self.coinm_hist_klines_daily_uri,
        }
    }

    pub fn monthly_hist_klines_uri_for(&self, asset_cat: &AssetCategory) -> &str {
        match asset_cat {
            AssetCategory::Spot => &self.spot_hist_klines_monthly_uri,
            AssetCategory::Usdm => &self.usdm_hist_klines_monthly_uri,
            AssetCategory::Coinm => &self.coinm_hist_klines_monthly_uri,
        }
    }
}
