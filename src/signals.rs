pub mod ema_signals;

use polars::prelude::DataFrame;

#[derive(Debug)]
pub enum Signal {
    Buy,
    Sell,
    Hold,
}

pub trait SignalGenerator {
    fn process(&self, data: DataFrame) -> Signal;
    fn get_threshold(&self) -> usize;
}
