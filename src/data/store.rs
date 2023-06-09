use std::fs::File;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{anyhow, Result};
use polars::io::parquet::ParquetReader;
use polars::lazy::dsl::*;
use polars::prelude::*;
use rayon::prelude::*;

use crate::data::{Column, Symbol};
use crate::DataConfig;
use crate::DataProvider;

pub struct DataStore {
    config: DataConfig,
}

impl DataStore {
    pub fn new(config: DataConfig) -> DataStore {
        DataStore { config }
    }

    // TODO: Date range
    pub fn load(&self, symbol: &str, timeframe: &Option<&str>) -> Option<DataFrame> {
        let store_path = self.store_path_for(symbol, timeframe);
        if !store_path.exists() {
            log::warn!("Store file not found: {:?}", store_path);
            return None;
        }

        let mut store_file = File::open(&store_path).ok()?;
        let df = ParquetReader::new(&mut store_file).finish().ok()?;
        Some(df)
    }

    fn store_path_for(&self, symbol: &str, timeframe: &Option<&str>) -> PathBuf {
        let store_name = self.store_name_for(symbol, timeframe);
        let mut store_path = PathBuf::new();
        store_path.push(&self.config.base_store_dir);
        store_path.push(self.config.asset_cat.as_str());
        store_path.push(symbol);

        if !store_path.exists() {
            std::fs::create_dir_all(&store_path).unwrap();
        }

        store_path.push(store_name);
        store_path
    }

    fn store_name_for(&self, symbol: &str, timeframe: &Option<&str>) -> String {
        let tf = match timeframe {
            Some(tf) => format!("-{}", tf),
            None => String::new(),
        };
        format!("{}{}.parquet", symbol, tf)
    }
}

impl DataStore {
    pub fn new_arc(config: DataConfig) -> Arc<Self> {
        Arc::new(DataStore::new(config))
    }

    pub fn sync(self: Arc<Self>, mut symbols: Vec<Symbol>) {
        symbols.sort_by(|a, b| a.name.cmp(&b.name));

        symbols.into_par_iter().for_each(|symbol| {
            let this = Arc::clone(&self);

            match this.sync_internal(&symbol) {
                Ok(_) => log::info!("Synced: {}", symbol.name),
                Err(e) => {
                    log::error!("Failed to sync: {}", symbol.name);
                    log::debug!("Error: {}", e);
                }
            };
        });
    }

    fn sync_internal(self: &Arc<Self>, symbol: &Symbol) -> Result<()> {
        let store = self.load(&symbol.name, &None);
        match store {
            Some(store) => {
                // TODO: Check if df is up to date
                // println!("{}", store.sample_n(50, false, false, None).unwrap());

                println!("{}", store.sort([Column::OPEN_TIME], false)?);
                // println!("{}", store);
            }
            None => {
                let provider =
                    DataProvider::new(self.config.clone(), self.config.asset_cat.clone());
                provider.sync(&symbol.name, &symbol.initdate)?;

                let store = self.create(&provider, &symbol.name)?.lazy();

                for tf in self.config.default_timeframes.iter() {
                    self.resample(&symbol.name, tf, store.clone())?;
                }
            }
        }

        Ok(())
    }

    fn create(self: &Arc<Self>, provider: &DataProvider, symbol: &str) -> Result<DataFrame> {
        let mut dfs = provider.load_all(symbol)?;
        if dfs.is_empty() {
            return Err(anyhow!("No data found for symbol: {}", symbol));
        }

        let mut store = dfs.pop().unwrap();
        for df in dfs {
            store = store.vstack(&df)?;
        }

        // TODO: Check for gaps in data
        // Sometimes monthly data has daily gaps, so we need to fill them from daily data

        store = store.drop_nulls::<String>(None)?;

        store.calc_log_returns()?;
        store.calc_cum_returns()?;

        let store_path = self.store_path_for(symbol, &None);
        let mut store_file = File::create(store_path)?;
        ParquetWriter::new(&mut store_file).finish(&mut store)?;

        log::info!(
            "Created store for: {} - {}",
            self.config.asset_cat.as_str(),
            symbol
        );
        Ok(store)
    }

    fn resample(&self, symbol: &str, timeframe: &str, store: LazyFrame) -> Result<()> {
        let duration = Duration::parse(timeframe);
        let offset = Duration::parse("0s");
        let mut resampled_store = store
            .sort(Column::OPEN_TIME, SortOptions::default())
            .groupby_dynamic(
                col(Column::OPEN_TIME),
                [],
                DynamicGroupOptions {
                    index_column: Column::OPEN_TIME.into(),
                    every: duration,
                    period: duration,
                    offset,
                    truncate: false,
                    include_boundaries: false,
                    closed_window: ClosedWindow::Left,
                    start_by: Default::default(),
                    check_sorted: false,
                },
            )
            .agg([
                col(Column::OPEN).first(),
                col(Column::HIGH).max(),
                col(Column::LOW).min(),
                col(Column::CLOSE).last(),
                col(Column::VOLUME).sum(),
                col(Column::QUOTE_VOLUME).sum(),
                col(Column::COUNT).sum(),
                col(Column::TAKER_BUY_VOLUME).sum(),
                col(Column::TAKER_BUY_QUOTE_VOLUME).sum(),
                col(Column::LOG_RETURNS).sum(),
                col(Column::CUM_RETURNS).last(),
            ])
            .collect()?;

        let store_path = self.store_path_for(symbol, &Some(timeframe));
        let mut store_file = File::create(store_path)?;
        ParquetWriter::new(&mut store_file).finish(&mut resampled_store)?;

        log::info!(
            "Resampled: {} - {} - {}",
            self.config.asset_cat.as_str(),
            symbol,
            timeframe
        );
        Ok(())
    }
}

trait StoreCalcs {
    fn calc_log_returns(&mut self) -> Result<()>;
    fn calc_cum_returns(&mut self) -> Result<()>;
}

impl StoreCalcs for DataFrame {
    fn calc_log_returns(&mut self) -> Result<()> {
        let close = self.column(Column::CLOSE)?.f64()?.clone();
        let shifted_close = close.shift_and_fill(1, Some(1.0));

        let log_returns = Series::new(
            Column::LOG_RETURNS,
            (close / shifted_close).apply(|diff| diff.ln()),
        );

        self.hstack_mut(&[log_returns])?;
        Ok(())
    }

    fn calc_cum_returns(&mut self) -> Result<()> {
        let cum_returns = Series::new(
            Column::CUM_RETURNS,
            self.column(Column::LOG_RETURNS)?.cumsum(false),
        );

        self.hstack_mut(&[cum_returns])?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_calc_log_returns() {
        let mut df = df!(
            Column::CLOSE => &[1.0, 2.0, 1.5, 3.5, 1.5]
        )
        .unwrap();

        df.calc_log_returns().unwrap();
        df.calc_cum_returns().unwrap();

        println!("{}", df);
    }

    #[test]
    fn test_calc_cum_returns() {
        let mut df = df!(
            Column::LOG_RETURNS => &[-1.0, 2.0, -3.0, 4.0, -5.0]
        )
        .unwrap();

        df.calc_cum_returns().unwrap();

        let expected_cum_returns = Series::new(Column::CUM_RETURNS, &[-1.0, 1.0, -2.0, 2.0, -3.0]);

        let x = df
            .column(Column::CUM_RETURNS)
            .unwrap()
            .iter()
            .zip(expected_cum_returns.iter());

        for (a, b) in x {
            assert_eq!(a, b);
        }
    }
}
