use log::{info, warn};
use std::fs::File;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{anyhow, Result};
use polars::io::parquet::ParquetReader;
use polars::prelude::*;
use rayon::prelude::*;

use crate::data::Symbol;
use crate::AssetCategory;
use crate::DataConfig;
use crate::DataProvider;

pub struct DataStore {
    config: DataConfig,
    asset_cat: AssetCategory,
}

impl DataStore {
    pub fn new(config: DataConfig, asset_cat: AssetCategory) -> DataStore {
        DataStore { config, asset_cat }
    }

    // TODO: Date range
    pub fn load(&self, symbol: &str, timeframe: &Option<String>) -> Option<DataFrame> {
        let store_path = self.store_path_for(symbol, timeframe);
        if !store_path.exists() {
            warn!("Store file not found: {:?}", store_path);
            return None;
        }

        let mut store_file = File::open(&store_path).ok()?;
        let df = ParquetReader::new(&mut store_file).finish().ok()?;
        Some(df)
    }

    fn store_path_for(&self, symbol: &str, timeframe: &Option<String>) -> PathBuf {
        let store_name = self.store_name_for(symbol, timeframe);
        let mut store_path = PathBuf::new();
        store_path.push(&self.config.base_store_dir);
        store_path.push(store_name);
        store_path
    }

    fn store_name_for(&self, symbol: &str, timeframe: &Option<String>) -> String {
        let tf = match timeframe {
            Some(tf) => format!("-{}", tf),
            None => String::new(),
        };
        format!("{}{}.parquet", symbol, tf)
    }
}

impl DataStore {
    pub fn new_arc(config: DataConfig, asset_cat: AssetCategory) -> Arc<Self> {
        Arc::new(DataStore::new(config, asset_cat))
    }

    pub fn sync(self: Arc<Self>, mut symbols: Vec<Symbol>) {
        symbols.sort_by(|a, b| a.name.cmp(&b.name));

        symbols.into_par_iter().for_each(|symbol| {
            let this = Arc::clone(&self);
            this.sync_internal(symbol);
        });
    }

    fn sync_internal(self: &Arc<Self>, symbol: Symbol) -> Result<()> {
        let df = self.load(&symbol.name, &None);
        match df {
            Some(df) => {
                // TODO: Check if df is up to date
            }
            None => {
                let provider = DataProvider::new(self.config.clone(), self.asset_cat.clone());
                provider.sync(&symbol.name, &symbol.initdate)?;

                // let dfs = provider.load_all(&symbol.name);
                // if dfs.is_empty() {
                //     return Err(anyhow!("No data found for symbol: {}", symbol.name));
                // }

                // let store = self.create(&symbol.name, dfs);

                // store.unique_stable()
            }
        }

        Ok(())
    }

    fn create(self: &Arc<Self>, symbol: &str, mut dfs: Vec<DataFrame>) -> Option<DataFrame> {
        let store = dfs.pop().unwrap();
        for df in dfs {
            store.vstack(&df).ok()?;
        }
        print!("{}", store.sample_n(50, false, false, None).unwrap());

        None
    }
}
