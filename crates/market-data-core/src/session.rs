use chrono::{DateTime, Timelike, Utc};
use chrono_tz::America::New_York;
use serde::{Deserialize, Serialize};

/// Trading session classification.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Session {
    /// Pre-market: 4:00 - 9:30 ET
    PreMarket,
    /// Regular market hours: 9:30 - 16:00 ET
    Regular,
    /// After-hours: 16:00 - 20:00 ET
    AfterHours,
}

impl Session {
    /// Classify a UTC timestamp into a trading session.
    /// Returns `None` if the timestamp falls outside all sessions (before 4:00 or after 20:00 ET).
    pub fn classify(timestamp: &DateTime<Utc>) -> Option<Self> {
        let et = timestamp.with_timezone(&New_York);
        let hour = et.hour();
        let minute = et.minute();
        let total_minutes = hour * 60 + minute;

        // Pre-market: 4:00 (240) to 9:29 (569)
        // Regular: 9:30 (570) to 15:59 (959)
        // After-hours: 16:00 (960) to 19:59 (1199)
        match total_minutes {
            240..570 => Some(Session::PreMarket),
            570..960 => Some(Session::Regular),
            960..1200 => Some(Session::AfterHours),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn utc_from_et(
        year: i32,
        month: u32,
        day: u32,
        hour: u32,
        min: u32,
        est: bool,
    ) -> DateTime<Utc> {
        use chrono::NaiveDate;
        let offset_hours: i64 = if est { 5 } else { 4 };
        let naive = NaiveDate::from_ymd_opt(year, month, day)
            .unwrap()
            .and_hms_opt(hour, min, 0)
            .unwrap();
        let utc_naive = naive + chrono::Duration::hours(offset_hours);
        Utc.from_utc_datetime(&utc_naive)
    }

    #[test]
    fn classify_premarket_start() {
        // 4:00 ET = PreMarket
        let ts = utc_from_et(2025, 1, 15, 4, 0, true);
        assert_eq!(Session::classify(&ts), Some(Session::PreMarket));
    }

    #[test]
    fn classify_premarket_end() {
        // 9:29 ET = PreMarket
        let ts = utc_from_et(2025, 1, 15, 9, 29, true);
        assert_eq!(Session::classify(&ts), Some(Session::PreMarket));
    }

    #[test]
    fn classify_regular_start() {
        // 9:30 ET = Regular
        let ts = utc_from_et(2025, 1, 15, 9, 30, true);
        assert_eq!(Session::classify(&ts), Some(Session::Regular));
    }

    #[test]
    fn classify_regular_end() {
        // 15:59 ET = Regular
        let ts = utc_from_et(2025, 1, 15, 15, 59, true);
        assert_eq!(Session::classify(&ts), Some(Session::Regular));
    }

    #[test]
    fn classify_afterhours_start() {
        // 16:00 ET = AfterHours
        let ts = utc_from_et(2025, 1, 15, 16, 0, true);
        assert_eq!(Session::classify(&ts), Some(Session::AfterHours));
    }

    #[test]
    fn classify_afterhours_end() {
        // 19:59 ET = AfterHours
        let ts = utc_from_et(2025, 1, 15, 19, 59, true);
        assert_eq!(Session::classify(&ts), Some(Session::AfterHours));
    }

    #[test]
    fn classify_outside_sessions() {
        // 20:00 ET = None
        let ts = utc_from_et(2025, 1, 15, 20, 0, true);
        assert_eq!(Session::classify(&ts), None);

        // 3:59 ET = None
        let ts = utc_from_et(2025, 1, 15, 3, 59, true);
        assert_eq!(Session::classify(&ts), None);
    }

    #[test]
    fn classify_dst_edt() {
        // During EDT (summer): 9:30 ET should still be Regular
        // July 15 is during EDT (UTC-4)
        let ts = utc_from_et(2025, 7, 15, 9, 30, false);
        assert_eq!(Session::classify(&ts), Some(Session::Regular));
    }

    #[test]
    fn classify_dst_transition_boundary() {
        // March 9, 2025 is DST spring forward day (EST->EDT)
        // 9:30 ET on that day
        let ts = utc_from_et(2025, 3, 10, 9, 30, false); // EDT after spring forward
        assert_eq!(Session::classify(&ts), Some(Session::Regular));
    }
}
