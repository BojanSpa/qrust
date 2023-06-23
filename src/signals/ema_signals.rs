use std::cell::RefCell;

use polars::prelude::DataFrame;
use ta::indicators::ExponentialMovingAverage as Ema;
use ta::Period;

use crate::data::Column;
use crate::signals::{Signal, SignalGenerator};
use crate::ta::Last;

pub struct EmaCrossSignal {
    ema_fast: RefCell<Ema>,
    ema_slow: RefCell<Ema>,
}

impl EmaCrossSignal {
    pub fn new(fast: usize, slow: usize) -> Self {
        let ema_fast = RefCell::new(Ema::new(fast).unwrap());
        let ema_slow = RefCell::new(Ema::new(slow).unwrap());

        Self { ema_fast, ema_slow }
    }
}

impl SignalGenerator for EmaCrossSignal {
    fn process(&self, data: DataFrame) -> Signal {
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

    fn get_threshold(&self) -> usize {
        self.ema_slow.borrow().period()
    }
}
