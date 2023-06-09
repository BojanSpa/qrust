use std::cell::RefCell;

use polars::prelude::*;
use ta::indicators::ExponentialMovingAverage as Ema;

use crate::data::Column;
use crate::ta::Last;

#[derive(Debug)]
pub enum Signal {
    Buy,
    Sell,
    Hold,
}

pub trait Strat {}

pub trait EventStrat {
    fn run(&self, data: DataFrame) -> Signal;
}

pub trait VectorStrat {}

pub struct EmaCrossStrat {
    ema_fast: RefCell<Ema>,
    ema_slow: RefCell<Ema>,
    threshold: usize,
}

impl EmaCrossStrat {
    pub fn new(fast: usize, slow: usize) -> Self {
        let ema_fast = RefCell::new(Ema::new(fast).unwrap());
        let ema_slow = RefCell::new(Ema::new(slow).unwrap());

        Self {
            ema_fast,
            ema_slow,
            threshold: slow,
        }
    }
}

impl EventStrat for EmaCrossStrat {
    fn run(&self, data: DataFrame) -> Signal {
        if data.height() < self.threshold {
            return Signal::Hold;
        }

        let close = data.column(Column::CLOSE).unwrap().f64().unwrap();
        let fast = self.ema_fast.borrow_mut().last(close);
        let slow = self.ema_slow.borrow_mut().last(close);

        if fast > slow {
            return Signal::Buy;
        }

        if fast < slow {
            return Signal::Sell;
        }

        Signal::Hold
    }
}
