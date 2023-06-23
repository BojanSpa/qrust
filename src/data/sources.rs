use anyhow::{anyhow, Result};
use async_trait::async_trait;
use tokio::sync::mpsc;

use crate::data::config::DataConfig;
use crate::data::store::DataStore;
use crate::events::Event;

#[async_trait]
pub trait EventSource {
    async fn start(&self, lookback: usize) -> Result<()>;
}

pub struct EventSourceOptions {
    pub symbol: String,
    pub timeframe: Option<String>,
}

pub struct StoreEventSource {
    store: DataStore,
    options: EventSourceOptions,
    sender: mpsc::Sender<Event>,
}

impl StoreEventSource {
    pub fn new(
        config: DataConfig,
        options: EventSourceOptions,
        sender: mpsc::Sender<Event>,
    ) -> Self {
        Self {
            store: DataStore::new(config),
            options,
            sender,
        }
    }
}

#[async_trait]
impl EventSource for StoreEventSource {
    async fn start(&self, lookback: usize) -> Result<()> {
        let symbol = self.options.symbol.clone();
        let timeframe = self.options.timeframe.clone();

        let data = self
            .store
            .load(&symbol, &timeframe)
            .ok_or(anyhow!("No data found for symbol {}", symbol))?;

        for i in 0..data.height() {
            if i < lookback {
                continue;
            }

            let offset = (i - lookback) as i64;
            let event = Event::Data(data.slice(offset, lookback));
            self.sender.send(event).await?;
        }

        self.sender.send(Event::Stop).await?;

        Ok(())
    }
}

// pub struct ExchangeEventSource<'a> {
//     symbol: &'a str,
//     timeframe: Option<String>,
// }

// impl<'a> ExchangeEventSource<'a> {
//     pub fn new(symbol: &'a str, timeframe: Option<String>) -> Self {
//         Self { symbol, timeframe }
//     }
// }

// impl EventSource for ExchangeEventSource<'_> {
//     fn start(&self, _handler: &EventHandler, _lookback: usize) -> Result<()> {
//         todo!()
//     }
// }
