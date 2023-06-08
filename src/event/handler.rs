use anyhow::Result;

use crate::event::sources::EventSource;
use crate::event::DataEvent;

pub trait HandlesData {
    fn on_data(&self, event: DataEvent);
}

pub struct EventHandler {
    source: Box<dyn EventSource>,
}

impl EventHandler {
    pub fn new(source: Box<dyn EventSource>) -> Self {
        Self { source }
    }

    pub fn start(&self, lookback: usize) -> Result<()> {
        self.source.start(self, lookback)?;
        Ok(())
    }
}

impl HandlesData for EventHandler {
    fn on_data(&self, event: DataEvent) {
        println!("Received data event: {:?}", event.data.collect().unwrap());
    }
}
