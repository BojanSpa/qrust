// use polars::frame::DataFrame;
// use polars::prelude::PolarsError;

use crate::data::config::DataConfig;
use crate::data::AssetCategory;

pub struct DataStore {
    config: DataConfig,
    asset_cat: AssetCategory,
}
impl DataStore {
    pub fn new(config: DataConfig, asset_cat: AssetCategory) -> DataStore {
        DataStore { config, asset_cat }
    }

    // pub fn load(&self, symbol: String, tf: Option<String>) -> Result<DataFrame, PolarsError> {
    //     DataFrame::new(vec![])
    // }
}
