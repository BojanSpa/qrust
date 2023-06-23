use std::sync::{Arc, Mutex};

use tokio::sync::mpsc;

pub enum Order {
    Buy(f64),
    Sell(f64),
    None,
}

pub struct Position {
    size: f64,
}

pub struct Balance {
    initial: f64,
    current: f64,
    positions: Vec<Position>,
}

impl Balance {
    pub fn new(initial: f64) -> Self {
        Self {
            initial,
            current: initial,
            positions: Vec::new(),
        }
    }
}

pub struct Broker {
    balance: Arc<Mutex<Balance>>,
    order_receiver: mpsc::Receiver<Order>,
}

impl Broker {
    pub fn new(balance: Arc<Mutex<Balance>>, order_receiver: mpsc::Receiver<Order>) -> Self {
        Self {
            balance,
            order_receiver,
        }
    }

    pub async fn start(&mut self) {
        while let Some(order) = self.order_receiver.recv().await {
            match order {
                Order::Buy(price) => self.on_buy(price),
                Order::Sell(price) => self.on_sell(price),
                Order::None => break,
            }
        }
    }

    fn on_buy(&self, _price: f64) {}
    fn on_sell(&self, _price: f64) {}
}
