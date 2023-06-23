use std::sync::Mutex;

use polars::prelude::*;
use tokio::sync::mpsc;

use crate::broker::{Balance, Order};
use crate::events::Event;
use crate::signals::SignalGenerator;

pub struct Strategy<S: SignalGenerator> {
    event_receiver: mpsc::Receiver<Event>,
    order_sender: mpsc::Sender<Order>,
    signal_generator: S,
    balance: Arc<Mutex<Balance>>,
}

impl<S: SignalGenerator> Strategy<S> {
    pub fn new(
        event_receiver: mpsc::Receiver<Event>,
        order_sender: mpsc::Sender<Order>,
        signal_generator: S,
        balance: Arc<Mutex<Balance>>,
    ) -> Self {
        Self {
            event_receiver,
            order_sender,
            signal_generator,
            balance,
        }
    }

    pub async fn run(&mut self) {
        while let Some(event) = self.event_receiver.recv().await {
            match event {
                Event::Data(data) => self.on_data(data),
                Event::Stop => self.on_stop(),
                Event::None => break,
            }
        }
    }

    fn on_data(&self, data: DataFrame) {
        let signal = self.signal_generator.process(data);
    }

    fn on_stop(&self) {}
}
