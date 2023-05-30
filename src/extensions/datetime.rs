use chrono::{DateTime, Utc};

pub fn create_utc(year: i32, month: u32, day: u32) -> DateTime<Utc> {
    DateTime::from_utc(
        chrono::NaiveDate::from_ymd_opt(year, month, day)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap(),
        Utc,
    )
}
