//! UTC instant formatting and parsing for the console, SQL shell, and TUI.
//!
//! Corium stores `:db/txInstant` as Unix milliseconds; operators name points in
//! time as calendar timestamps. These conversions are the boundary, and they
//! are deliberately dependency-free — the calendar arithmetic is Howard
//! Hinnant's `days_from_civil`/`civil_from_days` pair.

/// Formats Unix milliseconds as `YYYY-MM-DD HH:MM:SS.mmm` in UTC.
#[must_use]
pub fn format_instant(unix_ms: i64) -> String {
    let seconds = unix_ms.div_euclid(1_000);
    let millis = unix_ms.rem_euclid(1_000);
    let days = seconds.div_euclid(86_400);
    let tod = seconds.rem_euclid(86_400);
    let (year, month, day) = civil_from_days(days);
    format!(
        "{year:04}-{month:02}-{day:02} {:02}:{:02}:{:02}.{millis:03}",
        tod / 3_600,
        (tod % 3_600) / 60,
        tod % 60
    )
}

/// Parses a UTC timestamp into Unix milliseconds.
///
/// Accepts `YYYY-MM-DD`, `YYYY-MM-DDTHH:MM[:SS[.mmm]]`, a space in place of
/// the `T`, and an optional trailing `Z`. Times are always UTC: an offset
/// would silently change which transactions a view covers, so it is rejected
/// rather than assumed.
#[must_use]
pub fn parse_instant(text: &str) -> Option<i64> {
    let text = text.trim();
    let text = text.strip_suffix('Z').unwrap_or(text);
    let (date, time) = match text.split_once(['T', ' ']) {
        Some((date, time)) => (date, Some(time)),
        None => (text, None),
    };
    let mut date_parts = date.split('-');
    let year = date_parts.next()?.parse::<i64>().ok()?;
    let month = parse_field(date_parts.next()?, 1, 12)?;
    let day = parse_field(date_parts.next()?, 1, 31)?;
    if date_parts.next().is_some() {
        return None;
    }
    let days_in_month = match month {
        2 if is_leap_year(year) => 29,
        2 => 28,
        4 | 6 | 9 | 11 => 30,
        _ => 31,
    };
    if day > days_in_month {
        return None;
    }

    let (hour, minute, second, millis) = match time {
        None => (0, 0, 0, 0),
        Some(time) => {
            let mut parts = time.split(':');
            let hour = parse_field(parts.next()?, 0, 23)?;
            let minute = parse_field(parts.next()?, 0, 59)?;
            let (second, millis) = match parts.next() {
                None => (0, 0),
                Some(seconds) => match seconds.split_once('.') {
                    // Leap seconds are accepted and land on the next minute.
                    None => (parse_field(seconds, 0, 60)?, 0),
                    Some((seconds, fraction)) => {
                        let scaled: String =
                            fraction.chars().chain("000".chars()).take(3).collect();
                        (parse_field(seconds, 0, 60)?, parse_field(&scaled, 0, 999)?)
                    }
                },
            };
            if parts.next().is_some() {
                return None;
            }
            (hour, minute, second, millis)
        }
    };

    let days = days_from_civil(year, month, day);
    let seconds = days * 86_400 + i128::from(hour * 3_600 + minute * 60 + second);
    i64::try_from(seconds * 1_000 + i128::from(millis)).ok()
}

/// A point in time named on a console or SQL-shell command line.
pub enum TimePoint {
    /// A transaction basis.
    T(u64),
    /// A wall-clock instant in Unix milliseconds.
    Instant(i64),
}

/// Reads an `as-of`/`since` argument as a basis `t` or a UTC timestamp.
///
/// # Errors
/// Returns a usage message when the argument is missing or is neither.
pub fn parse_time_point(argument: Option<&str>, command: &str) -> Result<TimePoint, String> {
    let argument = argument.ok_or_else(|| format!("usage: {command} t|timestamp"))?;
    if let Ok(t) = argument.parse::<u64>() {
        return Ok(TimePoint::T(t));
    }
    parse_instant(argument)
        .map(TimePoint::Instant)
        .ok_or_else(|| {
            format!(
                "{command} requires a transaction number or a UTC timestamp \
                 like 2026-07-25T09:30:00Z"
            )
        })
}

/// Parses a fixed calendar field, rejecting anything outside its range so a
/// typo becomes an error instead of a silently shifted date.
fn parse_field(text: &str, low: i64, high: i64) -> Option<i64> {
    let value = text.parse::<i64>().ok()?;
    (low..=high).contains(&value).then_some(value)
}

fn is_leap_year(year: i64) -> bool {
    year.rem_euclid(4) == 0 && (year.rem_euclid(100) != 0 || year.rem_euclid(400) == 0)
}

/// Gregorian calendar date for a day count since 1970-01-01 (Howard Hinnant's
/// `civil_from_days`).
fn civil_from_days(days: i64) -> (i64, i64, i64) {
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = yoe + era * 400 + i64::from(month <= 2);
    (year, month, day)
}

/// Day count since 1970-01-01 for a Gregorian date (Howard Hinnant's
/// `days_from_civil`), the inverse of [`civil_from_days`].
fn days_from_civil(year: i64, month: i64, day: i64) -> i128 {
    let year = i128::from(year) - i128::from(month <= 2);
    let era = year.div_euclid(400);
    let yoe = year - era * 400;
    let month = i128::from(month);
    let day = i128::from(day);
    let mp = if month > 2 { month - 3 } else { month + 9 };
    let doy = (153 * mp + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn instants_format_as_utc() {
        assert_eq!(format_instant(0), "1970-01-01 00:00:00.000");
        assert_eq!(format_instant(1_700_000_000_123), "2023-11-14 22:13:20.123");
        assert_eq!(format_instant(-1), "1969-12-31 23:59:59.999");
    }

    #[test]
    fn parsing_round_trips_formatting() {
        for millis in [0_i64, 1_700_000_000_123, -86_400_000, 4_102_444_800_000] {
            let text = format_instant(millis);
            assert_eq!(parse_instant(&text), Some(millis), "{text}");
        }
    }

    #[test]
    fn accepts_the_shapes_operators_type() {
        assert_eq!(parse_instant("1970-01-01"), Some(0));
        assert_eq!(parse_instant("1970-01-02Z"), Some(86_400_000));
        assert_eq!(parse_instant("1970-01-01T00:00:01"), Some(1_000));
        assert_eq!(parse_instant("1970-01-01 00:01"), Some(60_000));
        assert_eq!(parse_instant("1970-01-01T00:00:00.5Z"), Some(500));
    }

    #[test]
    fn rejects_malformed_and_out_of_range_timestamps() {
        for text in [
            "",
            "not-a-date",
            "1970-13-01",
            "1970-01-32",
            "1970-01-01T24:00",
            "1970-01-01T00:60",
            "1970-01-01T00:00:00+02:00",
            "1970-01-01-01",
            "1970-01",
            "2026-02-29",
            "2026-02-31",
            "2026-04-31",
        ] {
            assert_eq!(parse_instant(text), None, "{text}");
        }
    }
}
