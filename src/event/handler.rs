use tokio::sync::mpsc;

use crate::event::DataEvent;
use crate::strats::EventStrat;

pub struct EventHandler<S: EventStrat> {
    strat: S,
    receiver: mpsc::Receiver<Option<DataEvent>>,
}

impl<S: EventStrat> EventHandler<S> {
    pub fn new(strat: S, receiver: mpsc::Receiver<Option<DataEvent>>) -> Self {
        Self { strat, receiver }
    }

    pub async fn listen(&mut self) {
        while let Some(event_opt) = self.receiver.recv().await {
            if let Some(event) = event_opt {
                self.on_data(event);
            } else {
                println!("All items have been received");
                break;
            }
        }
    }

    fn on_data(&self, event: DataEvent) {
        let signal = self.strat.run(event.data);
        // println!("Received signal: {:?}", signal);
    }
}
