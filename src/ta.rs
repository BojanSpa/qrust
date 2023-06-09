use polars::prelude::*;
use ta::indicators::ExponentialMovingAverage as Ema;
use ta::Next;

pub trait Last {
    fn last(&mut self, input: &ChunkedArray<Float64Type>) -> f64;
}

impl Last for Ema {
    fn last(&mut self, inputs: &ChunkedArray<Float64Type>) -> f64 {
        let mut last = 0.0;
        for inp in inputs.into_iter() {
            last = self.next(inp.unwrap());
        }
        last
    }
}
