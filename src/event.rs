pub mod handler;
pub mod sources;

use polars::lazy::prelude::*;

pub struct DataEvent {
    pub data: LazyFrame,
}
impl DataEvent {
    pub fn new(data: LazyFrame) -> Self {
        Self { data }
    }
}
