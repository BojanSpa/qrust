pub mod ema_signals;

use polars::prelude::DataFrame;

#[derive(Debug)]
pub enum Signal {
    Buy,
    Sell,
    Hold,
}

pub trait SignalProcessor {
    fn proc(&self, data: &DataFrame) -> Signal;
    fn get_threshold(&self) -> usize;
}
