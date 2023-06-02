use log::{debug, error, info, warn};
use std::fs;
use std::fs::File;
use std::io::Error as IoError;
use std::io::Write;
use std::path::PathBuf;

use chrono::{DateTime, Datelike, Days, Duration, Months, Utc};
use reqwest::Url;
use url;
use zip::ZipArchive;

use crate::data::config::DataConfig;
use crate::data::sanitizer::CsvSanitizer;
use crate::data::AssetCategory;
use crate::extensions::datetime;

const DEFAULT_TIMEFRAME: &str = "1m";

#[derive(PartialEq)]
pub enum Timeperiod {
    Monthly,
    Daily,
}

pub struct DataProvider {
    config: DataConfig,
    asset_cat: AssetCategory,
    sanitizer: CsvSanitizer,
}

impl DataProvider {
    pub fn new(config: DataConfig, asset_cat: AssetCategory) -> DataProvider {
        DataProvider {
            config,
            asset_cat,
            sanitizer: CsvSanitizer::new(),
        }
    }

    pub fn sync(&self, symbol: &str, init_date: &DateTime<Utc>) -> Result<(), IoError> {
        self.sync_internal(symbol, init_date, Timeperiod::Monthly)?;
        self.sync_internal(symbol, init_date, Timeperiod::Daily)?;
        Ok(())
    }

    fn sync_internal(
        &self,
        symbol: &str,
        init_date: &DateTime<Utc>,
        timeperiod: Timeperiod,
    ) -> Result<(), IoError> {
        let fromdate = self.fromdate_for(init_date, &timeperiod);
        let todate = Utc::now() - Duration::days(1);
        let dates = self.dates_for(&fromdate, &todate, &timeperiod);

        let base_path = self.base_path_for(symbol);
        if !base_path.exists() {
            std::fs::create_dir_all(base_path)?;
        }

        dates
            .iter()
            .for_each(|date| self.fetch(symbol, &timeperiod, date));

        Ok(())
    }

    fn fetch(&self, symbol: &str, timeperiod: &Timeperiod, date: &DateTime<Utc>) {
        let basepath = self.base_path_for(symbol);
        let baseuri = self.base_uri_for(timeperiod);
        let dateformat = self.date_format_for(timeperiod);
        let zipname = self.file_name_for(symbol, date, &dateformat);
        let csvname = zipname.replace(".zip", ".csv");

        let csvpath = basepath.join(&csvname);
        if csvpath.exists() {
            debug!("{} already exists", csvpath.to_str().unwrap());
            return;
        }

        let fileuri = self.uri_for(&baseuri, symbol, &zipname);
        let response = match reqwest::blocking::get(fileuri) {
            Ok(response) => response,
            Err(_) => {
                warn!("Could not fetch {}", zipname);
                return;
            }
        };

        let content = match response.bytes() {
            Ok(bytes) => bytes,
            Err(_) => {
                error!("Could not get content for {}", zipname);
                return;
            }
        };

        let zippath = basepath.join(&zipname);
        let mut zipfile = match File::create(&zippath) {
            Ok(file) => file,
            Err(_) => {
                error!("Could not create {}", zipname);
                return;
            }
        };

        if zipfile.write_all(&content).is_err() {
            error!("Could not write content to {}", zipname);
            return;
        }

        if self.extract(&zipfile, &basepath).is_err() {
            error!("Could not extract {}", zipname);
            return;
        }

        if self.sanitizer.cleanup(&csvpath).is_err() {
            warn!("Could not sanitize {}", &csvname);
            return;
        }

        if fs::remove_file(&zippath).is_err() {
            debug!("Could not remove {}", &zipname);
            return;
        };

        info!("Fetched {}", &zipname);
    }

    fn extract(&self, zipfile: &File, basepath: &PathBuf) -> Result<(), IoError> {
        let mut archive = ZipArchive::new(zipfile)?;
        archive.extract(basepath)?;
        Ok(())
    }

    // pub fn load(&self, symbol: &str) -> Result<DataFrame, PolarsError> {
    //     let base_path = self.base_path_for(symbol);

    //     if !base_path.exists() {
    //         return Err(PolarsError::NotFound);
    //     }

    // }

    fn base_path_for(&self, symbol: &str) -> PathBuf {
        let mut path_buf = PathBuf::new();
        path_buf.push(self.config.base_raw_dir.as_str());
        path_buf.push(self.asset_cat.to_str());
        path_buf.push(symbol.to_uppercase());
        path_buf
    }

    fn base_uri_for(&self, timeperiod: &Timeperiod) -> String {
        match timeperiod {
            Timeperiod::Monthly => self.config.spot_hist_klines_monthly_uri.clone(),
            Timeperiod::Daily => self.config.spot_hist_klines_daily_uri.clone(),
        }
    }

    fn uri_for(&self, baseuri: &str, symbol: &str, filename: &str) -> url::Url {
        Url::parse(baseuri)
            .unwrap()
            .join(symbol)
            .unwrap()
            .join(DEFAULT_TIMEFRAME)
            .unwrap()
            .join(filename)
            .unwrap()
    }

    fn date_format_for(&self, timeperiod: &Timeperiod) -> String {
        match timeperiod {
            Timeperiod::Monthly => self.config.date_format_monthly.clone(),
            Timeperiod::Daily => self.config.date_format_daily.clone(),
        }
    }

    fn file_name_for(&self, symbol: &str, date: &DateTime<Utc>, dateformat: &str) -> String {
        let mut filename = self.config.download_file_format.clone();
        filename = filename.replace("[[Symbol]]", &symbol.to_uppercase());
        filename = filename.replace("[[Timeframe]]", "1m");
        filename = filename.replace("[[Date]]", &date.format(dateformat).to_string());
        filename
    }

    fn fromdate_for(&self, init_date: &DateTime<Utc>, timeperiod: &Timeperiod) -> DateTime<Utc> {
        if *timeperiod == Timeperiod::Monthly {
            return *init_date;
        }
        datetime::create_utc(init_date.year(), init_date.month(), 1)
    }

    fn dates_for(
        &self,
        fromdate: &DateTime<Utc>,
        todate: &DateTime<Utc>,
        timeperiod: &Timeperiod,
    ) -> Vec<DateTime<Utc>> {
        let mut dates = vec![];
        match timeperiod {
            Timeperiod::Monthly => {
                let fromdate = fromdate.with_day(1).unwrap();
                let years = todate.year() - fromdate.year();
                let from_month = fromdate.month() as i32;
                let to_month = todate.month() as i32;
                let months = (years * 12) + (to_month - from_month);

                for months_to_add in 0..=months {
                    dates.push(
                        fromdate
                            .checked_add_months(Months::new(months_to_add as u32))
                            .unwrap(),
                    );
                }
            }
            Timeperiod::Daily => {
                let days = (*todate - *fromdate).num_days() as u64;
                for days in 0..=days {
                    dates.push(fromdate.checked_add_days(Days::new(days)).unwrap());
                }
            }
        }
        dates
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_monthly_dates_for() {
        let provider = get_provider();
        let fromdate = datetime::create_utc(2021, 10, 10);
        let todate = datetime::create_utc(2022, 2, 20);
        let dates = provider.dates_for(&fromdate, &todate, &Timeperiod::Monthly);

        assert_eq!(5, dates.len());
        assert_eq!(datetime::create_utc(2021, 10, 1), dates[0]);
        assert_eq!(datetime::create_utc(2021, 11, 1), dates[1]);
        assert_eq!(datetime::create_utc(2021, 12, 1), dates[2]);
        assert_eq!(datetime::create_utc(2022, 1, 1), dates[3]);
        assert_eq!(datetime::create_utc(2022, 2, 1), dates[4]);
    }

    #[test]
    fn test_daily_dates_for() {
        let provider = get_provider();
        let fromdate = datetime::create_utc(2023, 4, 29);
        let todate = datetime::create_utc(2023, 5, 2);
        let dates = provider.dates_for(&fromdate, &todate, &Timeperiod::Daily);

        assert_eq!(4, dates.len());
        assert_eq!(datetime::create_utc(2023, 4, 29), dates[0]);
        assert_eq!(datetime::create_utc(2023, 4, 30), dates[1]);
        assert_eq!(datetime::create_utc(2023, 5, 1), dates[2]);
        assert_eq!(datetime::create_utc(2023, 5, 2), dates[3]);
    }

    #[test]
    fn test_monthly_file_name_for() {
        let provider = get_provider();
        let date = datetime::create_utc(2023, 4, 1);
        let file_name = provider.file_name_for("btcusdt", &date, "%Y-%m");
        assert_eq!("BTCUSDT-1m-2023-04.zip", file_name);
    }

    #[test]
    fn test_daily_file_name_for() {
        let provider = get_provider();
        let date = datetime::create_utc(2023, 4, 4);
        let file_name = provider.file_name_for("btcusdt", &date, "%Y-%m-%d");
        assert_eq!("BTCUSDT-1m-2023-04-04.zip", file_name);
    }

    fn get_provider() -> DataProvider {
        let config = DataConfig::new();
        DataProvider::new(config, AssetCategory::Spot)
    }
}
