use std::thread;

use crate::data::Symbol;
use crate::AssetCategory;
use crate::DataConfig;
use crate::DataProvider;

pub struct DataStore {
    config: DataConfig,
    asset_cat: AssetCategory,
    provider: DataProvider,
}
impl DataStore {
    pub fn new(config: DataConfig, asset_cat: AssetCategory) -> DataStore {
        let provider = DataProvider::new(config.clone(), asset_cat.clone());

        DataStore {
            config,
            asset_cat,
            provider,
        }
    }

    pub fn sync(&self, symbols: Vec<Symbol>) {
        let mut threads = vec![];

        for symbol in symbols {
            threads.push(thread::spawn(move || {
                Self::sync_internal(symbol);
            }));
        }

        threads.into_iter().for_each(|t| t.join().unwrap());
    }

    fn sync_internal(symbol: Symbol) {}
}
