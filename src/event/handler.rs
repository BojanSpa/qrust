use anyhow::Result;

use crate::event::sources::EventSource;
use crate::event::DataEvent;
use crate::strats::EventStrat;

pub struct EventHandler {
    source: Box<dyn EventSource>,
    strat: Box<dyn EventStrat>,
}

impl EventHandler {
    pub fn new(source: Box<dyn EventSource>, strat: Box<dyn EventStrat>) -> Self {
        Self { source, strat }
    }

    pub fn on_data(&self, event: DataEvent) {
        let signal = self.strat.run(event.data);

        // println!("Received signal: {:?}", signal);
    }

    pub fn start(&self, lookback: usize) -> Result<()> {
        self.source.start(self, lookback)?;
        Ok(())
    }
}
