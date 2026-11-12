/*
 * Copyright © 2024 Volodymyr Kadzhaia
 * Copyright © 2024 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! The `time:` built-ins over `xsd:dateTime` text

use crate::matcher::Matcher;
use crate::numeric::number_literal;
use crate::rules::PTerm;
use crate::syntax::Node;
use shared::terms::Binding;

const XSD_DATE_TIME: &str = "http://www.w3.org/2001/XMLSchema#dateTime";

/// A `time:` built-in
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimeOp {
    /// `time:year`
    Year,
    /// `time:month`
    Month,
    /// `time:day`
    Day,
    /// `time:hour`
    Hour,
    /// `time:minute`
    Minute,
    /// `time:second`
    Second,
    /// `time:timeZone`
    TimeZone,
    /// `time:localTime`: the instant the run started, as `xsd:dateTime`
    LocalTime,
}

impl TimeOp {
    /// The operation a `time:` local name names
    pub(crate) fn from_name(name: &str) -> Option<Self> {
        Some(match name {
            "year" => TimeOp::Year,
            "month" => TimeOp::Month,
            "day" => TimeOp::Day,
            "hour" => TimeOp::Hour,
            "minute" => TimeOp::Minute,
            "second" => TimeOp::Second,
            "timeZone" => TimeOp::TimeZone,
            "localTime" => TimeOp::LocalTime,
            _ => return None,
        })
    }

    /// Whether the atom can run with what is bound
    pub(crate) fn ready(self, subject: &PTerm, ground: &dyn Fn(&PTerm) -> bool) -> bool {
        self == TimeOp::LocalTime || ground(subject)
    }

    /// Every extension of `binding` under which the atom holds
    pub(crate) fn evaluate(
        self,
        m: &mut Matcher<'_>,
        subject: &PTerm,
        object: &PTerm,
        binding: Binding,
    ) -> Vec<Binding> {
        let value = if self == TimeOp::LocalTime {
            let (seconds, millis) = m.now();
            Node::typed(format_utc(seconds, millis), XSD_DATE_TIME)
        } else {
            let Some(text) = m.ground(subject, &binding).and_then(|id| match m.node(id) {
                Node::Literal { lexical, .. } => Some(lexical),
                _ => None,
            }) else {
                return Vec::new();
            };
            let Some(parts) = DateTime::parse(&text) else {
                return Vec::new();
            };
            let number = |n: i64| number_literal(n as f64, true);
            match self {
                TimeOp::Year => number(parts.year),
                TimeOp::Month => number(parts.month),
                TimeOp::Day => number(parts.day),
                TimeOp::Hour => number(parts.hour),
                TimeOp::Minute => number(parts.minute),
                TimeOp::Second => number(parts.second),
                TimeOp::TimeZone | TimeOp::LocalTime => Node::Literal {
                    lexical: parts.zone,
                    datatype: None,
                    language: None,
                },
            }
        };
        let id = m.intern(&value);
        m.unify_value(object, id, binding)
    }
}

/// The fields of `YYYY-MM-DDTHH:MM:SS` followed by an optional zone
struct DateTime {
    year: i64,
    month: i64,
    day: i64,
    hour: i64,
    minute: i64,
    second: i64,
    zone: String,
}

impl DateTime {
    fn parse(text: &str) -> Option<Self> {
        let field = |range: std::ops::Range<usize>| text.get(range)?.parse().ok();
        Some(DateTime {
            year: field(0..4)?,
            month: field(5..7)?,
            day: field(8..10)?,
            hour: field(11..13)?,
            minute: field(14..16)?,
            second: field(17..19)?,
            zone: text.get(19..).unwrap_or("").to_string(),
        })
    }
}

/// The civil date of a day count since 1970-01-01
fn civil_from_days(days: i64) -> (i64, i64, i64) {
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = mp + if mp < 10 { 3 } else { -9 };
    (yoe + era * 400 + i64::from(month <= 2), month, day)
}

/// An instant as `xsd:dateTime` text in UTC, with milliseconds
fn format_utc(seconds: i64, millis: u32) -> String {
    let (year, month, day) = civil_from_days(seconds.div_euclid(86_400));
    let of_day = seconds.rem_euclid(86_400);
    format!(
        "{year:04}-{month:02}-{day:02}T{:02}:{:02}:{:02}.{millis:03}+00:00",
        of_day / 3600,
        of_day % 3600 / 60,
        of_day % 60
    )
}

#[cfg(test)]
mod tests {
    use super::format_utc;

    #[test]
    fn instants_format_as_utc_date_times() {
        assert_eq!(format_utc(0, 0), "1970-01-01T00:00:00.000+00:00");
        assert_eq!(format_utc(951_782_400, 5), "2000-02-29T00:00:00.005+00:00");
    }
}
