use chrono::{DateTime, NaiveDateTime, Utc};

pub fn create_utc(year: i32, month: u32, day: u32) -> DateTime<Utc> {
    DateTime::from_utc(
        chrono::NaiveDate::from_ymd_opt(year, month, day)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap(),
        Utc,
    )
}

pub fn from_timestamp(timestamp: &i64) -> NaiveDateTime {
    let mut ts = *timestamp;
    if ts.to_string().len() <= 10 {
        ts *= 1000;
    }
    NaiveDateTime::from_timestamp_millis(ts).unwrap()
}
