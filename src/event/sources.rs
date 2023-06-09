use anyhow::{anyhow, Result};

use crate::data::config::DataConfig;
use crate::data::store::DataStore;
use crate::event::handler::EventHandler;
use crate::event::DataEvent;

pub trait EventSource {
    fn start(&self, handler: &EventHandler, lookback: usize) -> Result<()>;
}

pub struct StoreEventSource<'a> {
    store: DataStore,
    symbol: &'a str,
    timeframe: Option<&'a str>,
}

impl<'a> StoreEventSource<'a> {
    pub fn new(config: DataConfig, symbol: &'a str, timeframe: Option<&'a str>) -> Self {
        Self {
            store: DataStore::new(config),
            symbol,
            timeframe,
        }
    }
}

impl<'a> EventSource for StoreEventSource<'a> {
    fn start(&self, handler: &EventHandler, lookback: usize) -> Result<()> {
        let data = self
            .store
            .load(self.symbol, &self.timeframe)
            .ok_or(anyhow!("No data found for symbol {}", self.symbol))?;

        for i in 0..data.height() {
            if i < lookback {
                continue;
            }

            let offset = (i - lookback) as i64;
            let event = DataEvent::new(data.slice(offset, lookback));
            handler.on_data(event);
        }

        Ok(())
    }
}

pub struct ExchangeEventSource<'a> {
    symbol: &'a str,
    timeframe: Option<String>,
}

impl<'a> ExchangeEventSource<'a> {
    pub fn new(symbol: &'a str, timeframe: Option<String>) -> Self {
        Self { symbol, timeframe }
    }
}

impl EventSource for ExchangeEventSource<'_> {
    fn start(&self, _handler: &EventHandler, _lookback: usize) -> Result<()> {
        todo!()
    }
}
