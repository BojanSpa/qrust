pub mod handler;
pub mod sources;

use polars::prelude::*;

pub struct DataEvent {
    pub data: DataFrame,
}
impl DataEvent {
    pub fn new(data: DataFrame) -> Self {
        Self { data }
    }
}
