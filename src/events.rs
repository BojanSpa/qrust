use polars::prelude::*;

#[derive(Debug)]
pub enum Event {
    Data(DataFrame),
    Stop,
    None,
}
