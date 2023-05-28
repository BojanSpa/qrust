use crate::data::config::DataConfig;
use crate::data::sanitizer::CsvSanitizer;
use crate::data::AssetCategory;
use chrono::{DateTime, Datelike, Duration, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use polars::frame::DataFrame;
use polars::io::csv::CsvReader;
use polars::prelude::PolarsError;
use std::io::Error;
use std::path::{Path, PathBuf};

#[derive(PartialEq)]
pub enum Timeperiod {
    Monthly,
    Daily,
}

pub struct DataProvider<'a> {
    config: DataConfig<'a>,
    asset_cat: AssetCategory,
    sanitizer: CsvSanitizer,
}

impl<'a> DataProvider<'a> {
    pub fn new(config: DataConfig, asset_cat: AssetCategory) -> DataProvider {
        DataProvider {
            config,
            asset_cat,
            sanitizer: CsvSanitizer::new(),
        }
    }

    pub fn sync(&self, symbol: &str, init_date: &DateTime<Utc>) -> Result<(), Error> {
        self.sync_internal(symbol, init_date, Timeperiod::Monthly)?;
        self.sync_internal(symbol, init_date, Timeperiod::Daily)?;
        Ok(())
    }

    fn sync_internal(
        &self,
        symbol: &str,
        init_date: &DateTime<Utc>,
        timeperiod: Timeperiod,
    ) -> Result<(), Error> {
        let now = Utc::now();
        let fromdate = self.fromdate_for(init_date, &timeperiod);
        let todate = Utc::now() - Duration::days(1);
        let dates = self.dates_reversed_for(&fromdate, &todate, &timeperiod);

        let base_path = self.base_path_for(symbol);
        if !base_path.exists() {
            std::fs::create_dir_all(base_path)?;
        }

        // dateparts.iter().for_each(|item| self.fetch())

        // for datepart in dateparts {
        //     self.fetch();
        // }

        Ok(())
    }

    // pub fn load(&self, symbol: &str) -> Result<DataFrame, PolarsError> {
    //     let base_path = self.base_path_for(symbol);

    //     if !base_path.exists() {
    //         return Err(PolarsError::NotFound);
    //     }

    // }

    fn base_path_for(&self, symbol: &str) -> &Path {
        let mut pathBuf = PathBuf::new();
        pathBuf.push(self.config.base_raw_dir);
        pathBuf.push(self.asset_cat.to_str());
        pathBuf.push(symbol.to_uppercase());

        pathBuf.as_path()
    }

    fn file_name_for(&self, symbol: &str) -> String {
        format!("{}.csv", symbol.to_uppercase())
    }

    fn fromdate_for(&self, init_date: &DateTime<Utc>, timeperiod: &Timeperiod) -> DateTime<Utc> {
        if *timeperiod == Timeperiod::Monthly {
            return init_date.clone();
        }

        return DateTime::from_utc(
            NaiveDateTime::new(
                NaiveDate::from_ymd_opt(init_date.year(), init_date.month(), 1).unwrap(),
                NaiveTime::from_hms_opt(0, 0, 0).unwrap(),
            ),
            Utc,
        );
    }

    fn dates_reversed_for(
        &self,
        fromdate: &DateTime<Utc>,
        todate: &DateTime<Utc>,
        timeperiod: &Timeperiod,
    ) -> Vec<u32> {
        match timeperiod {
            Timeperiod::Monthly => {
                let years = todate.year() as u32 - fromdate.year() as u32;
                let months = years * 12 + (todate.month() - fromdate.month());
                let mut dateparts: Vec<u32> = (1..=months).collect();
                dateparts.reverse();
                return dateparts;
            }
            Timeperiod::Daily => {
                let days = (*todate - *fromdate).num_days() as u32;
                let mut dateparts: Vec<u32> = (1..=days).collect();
                dateparts.reverse();
                return dateparts;
            }
            Duration::months()
        }
    }
}
