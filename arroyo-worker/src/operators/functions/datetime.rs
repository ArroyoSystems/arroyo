use std::time::SystemTime;

use arroyo_types::{DatePart, DateTruncPrecision};
use chrono::{DateTime, Datelike, Duration, Timelike, Utc};

fn quarter_month(date: &DateTime<Utc>) -> u32 {
    1 + 3 * ((date.month() - 1) / 3)
}

fn trunc_to_second(d: DateTime<Utc>) -> Option<DateTime<Utc>> {
    d.with_nanosecond(0)
}

fn trunc_to_minute(d: DateTime<Utc>) -> Option<DateTime<Utc>> {
    trunc_to_second(d).and_then(|d| d.with_second(0))
}
fn trunc_to_hour(d: DateTime<Utc>) -> Option<DateTime<Utc>> {
    trunc_to_minute(d)
        .and_then(|d| d.with_minute(0))
        .and_then(|d| d.with_nanosecond(0))
}

fn trunc_to_day(d: DateTime<Utc>) -> Option<DateTime<Utc>> {
    trunc_to_hour(d).and_then(|d| d.with_hour(0))
}

fn trunc_to_week(d: DateTime<Utc>) -> Option<DateTime<Utc>> {
    trunc_to_day(d).map(|d| d - Duration::seconds(60 * 60 * 24 * d.weekday() as i64))
}

fn trunc_to_month(d: DateTime<Utc>) -> Option<DateTime<Utc>> {
    trunc_to_day(d).and_then(|d| d.with_day0(0))
}

fn trunc_to_quarter(d: DateTime<Utc>) -> Option<DateTime<Utc>> {
    trunc_to_month(d).and_then(|d| d.with_month(quarter_month(&d)))
}

fn trunc_to_year(d: DateTime<Utc>) -> Option<DateTime<Utc>> {
    trunc_to_month(d).and_then(|d| d.with_month0(0))
}

pub fn date_trunc(date_trunc_precision: DateTruncPrecision, argument: SystemTime) -> SystemTime {
    let datetime: DateTime<Utc> = argument.into();
    let truncated = match date_trunc_precision {
        DateTruncPrecision::Second => trunc_to_second(datetime),
        DateTruncPrecision::Minute => trunc_to_minute(datetime),
        DateTruncPrecision::Hour => trunc_to_hour(datetime),
        DateTruncPrecision::Day => trunc_to_day(datetime),
        DateTruncPrecision::Week => trunc_to_week(datetime),
        DateTruncPrecision::Month => trunc_to_month(datetime),
        DateTruncPrecision::Quarter => trunc_to_quarter(datetime),
        DateTruncPrecision::Year => trunc_to_year(datetime),
    };
    truncated.unwrap().into()
}

pub fn date_part(part: DatePart, argument: SystemTime) -> u32 {
    let datetime: DateTime<Utc> = argument.into();
    match part {
        DatePart::DayOfYear => datetime.ordinal(),
        DatePart::DayOfWeek => datetime.weekday().num_days_from_monday(),
        DatePart::Nanosecond => datetime.nanosecond(),
        DatePart::Microsecond => datetime.nanosecond() / 1_000,
        DatePart::Millisecond => datetime.nanosecond() / 1_000_000,
        DatePart::Second => datetime.second(),
        DatePart::Minute => datetime.minute(),
        DatePart::Hour => datetime.hour(),
        DatePart::Day => datetime.day(),
        DatePart::Week => datetime.iso_week().week(),
        DatePart::Month => datetime.month(),
        DatePart::Year => datetime.year().try_into().unwrap(),
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use arroyo_types::{DatePart, DateTruncPrecision};
    use lazy_static::lazy_static;
    use std::{collections::HashMap, time::SystemTime};

    lazy_static! {
        static ref DATE_PART_TESTCASES: HashMap<DatePart, u32> = {
            let mut m = HashMap::new();
            m.insert(DatePart::Year, 2023);
            m.insert(DatePart::Month, 6);
            m.insert(DatePart::Week, 23);
            m.insert(DatePart::Day, 10);
            m.insert(DatePart::Hour, 0);
            m.insert(DatePart::Minute, 48);
            m.insert(DatePart::Second, 10);
            m.insert(DatePart::Millisecond, 284);
            m.insert(DatePart::Microsecond, 284546);
            m.insert(DatePart::Nanosecond, 284546794);
            m.insert(DatePart::DayOfWeek, 5);
            m.insert(DatePart::DayOfYear, 161);
            m
        };
        static ref DATE_TRUNC_TESTCASES: HashMap<DateTruncPrecision, &'static str> = {
            let mut m = HashMap::new();
            m.insert(DateTruncPrecision::Year, "2023-01-01T00:00:00Z");
            m.insert(DateTruncPrecision::Quarter, "2023-04-01T00:00:00Z");
            m.insert(DateTruncPrecision::Month, "2023-06-01T00:00:00Z");
            m.insert(DateTruncPrecision::Week, "2023-06-05T00:00:00Z");
            m.insert(DateTruncPrecision::Day, "2023-06-10T00:00:00Z");
            m.insert(DateTruncPrecision::Hour, "2023-06-10T00:00:00Z");
            m.insert(DateTruncPrecision::Minute, "2023-06-10T00:48:00Z");
            m.insert(DateTruncPrecision::Second, "2023-06-10T00:48:10Z");
            m
        };
        static ref REFERENCE_DATETIME: SystemTime = {
            let date_fixed_offset = DateTime::parse_from_rfc3339("2023-06-10T00:48:10.284546794Z")
                .expect("Failed to parse date and time");
            date_fixed_offset.into()
        };
    }

    #[test]
    fn test_date_trunc_is_correct() {
        for (key, value) in DATE_TRUNC_TESTCASES.iter() {
            let observed: DateTime<Utc> = date_trunc(*key, *REFERENCE_DATETIME).into();
            let date_fixed_offset =
                DateTime::parse_from_rfc3339(value).expect("Failed to parse date and time");
            let expected: DateTime<Utc> = DateTime::from_utc(date_fixed_offset.naive_utc(), Utc);
            assert_eq!(observed, expected, "Wrong result for {:?} ", key)
        }
    }

    #[test]
    fn test_date_part_is_correct() {
        for (key, value) in DATE_PART_TESTCASES.iter() {
            assert_eq!(
                date_part(*key, *REFERENCE_DATETIME),
                *value,
                "Wrong result for {:?}",
                key
            )
        }
    }
}
