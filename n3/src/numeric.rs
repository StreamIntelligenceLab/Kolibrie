/*
 * Copyright © 2024 Volodymyr Kadzhaia
 * Copyright © 2024 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! Numbers as N3 built-ins read and write them

use crate::syntax::{Node, XSD_DECIMAL, XSD_DOUBLE, XSD_INTEGER};
use num_bigint::BigInt;

/// `xsd:float`
const XSD_FLOAT: &str = "http://www.w3.org/2001/XMLSchema#float";

/// A numeric literal's value
#[derive(Debug, Clone)]
pub(crate) struct Numeric {
    /// The value as a float, for every operation not closed over the integers
    pub(crate) value: f64,
    /// Whether the literal is an `xsd:integer`
    pub(crate) integer: bool,
    /// The exact value of an `xsd:integer`
    pub(crate) exact: Option<BigInt>,
}

/// Reads a number lexical form, including the special float values
fn parse_lexical(lexical: &str) -> Option<f64> {
    match lexical {
        "INF" | "+INF" => Some(f64::INFINITY),
        "-INF" => Some(f64::NEG_INFINITY),
        "NaN" => Some(f64::NAN),
        other => other.parse().ok(),
    }
}

/// The value of a literal with a numeric datatype
pub(crate) fn numeric(node: &Node) -> Option<Numeric> {
    let Node::Literal {
        lexical,
        datatype: Some(datatype),
        ..
    } = node
    else {
        return None;
    };
    let integer = datatype == XSD_INTEGER;
    if !(integer || datatype == XSD_DECIMAL || datatype == XSD_DOUBLE || datatype == XSD_FLOAT) {
        return None;
    }
    let exact = if integer {
        lexical.trim_start_matches('+').parse().ok()
    } else {
        None
    };
    Some(Numeric {
        value: parse_lexical(lexical)?,
        integer,
        exact,
    })
}

/// An `xsd:integer` literal
pub(crate) fn integer_literal(value: &BigInt) -> Node {
    Node::typed(value.to_string(), XSD_INTEGER)
}

/// A computed float as a literal: an integer when asked for and whole, else decimal or double
pub(crate) fn number_literal(value: f64, prefer_integer: bool) -> Node {
    if prefer_integer && value.is_finite() && value.fract() == 0.0 {
        return Node::typed(format!("{value:.0}"), XSD_INTEGER);
    }
    let text = shortest(value);
    let datatype = if text.contains('e') || !value.is_finite() {
        XSD_DOUBLE
    } else {
        XSD_DECIMAL
    };
    Node::typed(text, datatype)
}

/// The shortest round-trip digits of `value`, positional within 21 digits, else exponential
pub(crate) fn shortest(value: f64) -> String {
    if value.is_nan() {
        return "NaN".to_string();
    }
    if value.is_infinite() {
        return if value < 0.0 { "-INF" } else { "INF" }.to_string();
    }
    if value == 0.0 {
        return "0".to_string();
    }
    let exponential = format!("{:e}", value.abs());
    let Some((mantissa, exponent)) = exponential.split_once('e') else {
        return value.to_string();
    };
    let exponent: i32 = exponent.parse().unwrap_or(0);
    let digits: String = mantissa.chars().filter(|c| *c != '.').collect();
    let count = digits.len() as i32;
    let point = exponent + 1;
    let body = if point >= count && point <= 21 {
        format!("{digits}{}", "0".repeat((point - count) as usize))
    } else if point > 0 && point <= 21 {
        format!(
            "{}.{}",
            &digits[..point as usize],
            &digits[point as usize..]
        )
    } else if point > -6 && point <= 0 {
        format!("0.{}{digits}", "0".repeat((-point) as usize))
    } else {
        let tail = if count == 1 {
            String::new()
        } else {
            format!(".{}", &digits[1..])
        };
        let sign = if point > 1 { "+" } else { "-" };
        format!("{}{tail}e{sign}{}", &digits[..1], (point - 1).abs())
    };
    if value < 0.0 {
        format!("-{body}")
    } else {
        body
    }
}

/// Whether two computed floats are the same number, within a few units in the last place
pub(crate) fn floats_equal(x: f64, y: f64) -> bool {
    let scale = x.abs().max(y.abs()).max(1.0);
    (x - y).abs() <= 16.0 * f64::EPSILON * scale
}

/// Whether two numbers are equal: exactly for two integers, approximately otherwise
pub(crate) fn numbers_equal(x: &Numeric, y: &Numeric) -> bool {
    if let (Some(p), Some(q)) = (&x.exact, &y.exact) {
        return p == q;
    }
    if x.value.is_nan() || y.value.is_nan() {
        return x.value.is_nan() && y.value.is_nan();
    }
    if x.value.is_infinite() || y.value.is_infinite() {
        return x.value == y.value;
    }
    floats_equal(x.value, y.value)
}

/// How two numbers order, exactly for two integers
pub(crate) fn compare(x: &Numeric, y: &Numeric) -> Option<std::cmp::Ordering> {
    match (&x.exact, &y.exact) {
        (Some(p), Some(q)) => Some(p.cmp(q)),
        _ => x.value.partial_cmp(&y.value),
    }
}

/// `xsd:date`
const XSD_DATE: &str = "http://www.w3.org/2001/XMLSchema#date";
/// `xsd:dateTime`
const XSD_DATE_TIME: &str = "http://www.w3.org/2001/XMLSchema#dateTime";
/// `xsd:duration`
pub(crate) const XSD_DURATION: &str = "http://www.w3.org/2001/XMLSchema#duration";

/// The day count since 1970-01-01 of a civil date
fn days_from_civil(year: i64, month: i64, day: i64) -> i64 {
    let year = if month <= 2 { year - 1 } else { year };
    let era = if year >= 0 { year } else { year - 399 } / 400;
    let yoe = year - era * 400;
    let doy = (153 * (month + if month > 2 { -3 } else { 9 }) + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

/// The instant an `xsd:date` or `xsd:dateTime` denotes, in seconds since the Unix epoch; a
/// date is its midnight, and a time without a zone is UTC
pub(crate) fn datetime_seconds(node: &Node) -> Option<f64> {
    let Node::Literal {
        lexical,
        datatype: Some(datatype),
        ..
    } = node
    else {
        return None;
    };
    if datatype != XSD_DATE && datatype != XSD_DATE_TIME {
        return None;
    }
    let digits = |range: std::ops::Range<usize>| -> Option<i64> {
        let text = lexical.get(range)?;
        text.bytes()
            .all(|b| b.is_ascii_digit())
            .then(|| text.parse().ok())?
    };
    if lexical.get(4..5) != Some("-") || lexical.get(7..8) != Some("-") {
        return None;
    }
    let (year, month, day) = (digits(0..4)?, digits(5..7)?, digits(8..10)?);
    let mut rest = &lexical[10..];
    let (mut hour, mut minute, mut second, mut fraction) = (0, 0, 0, 0.0);
    if let Some(time) = rest.strip_prefix('T') {
        let field = |range: std::ops::Range<usize>| -> Option<i64> {
            let text = time.get(range)?;
            text.bytes()
                .all(|b| b.is_ascii_digit())
                .then(|| text.parse().ok())?
        };
        if time.get(2..3) != Some(":") || time.get(5..6) != Some(":") {
            return None;
        }
        (hour, minute, second) = (field(0..2)?, field(3..5)?, field(6..8)?);
        rest = &time[8..];
        if let Some(after_dot) = rest.strip_prefix('.') {
            let end = after_dot
                .find(|c: char| !c.is_ascii_digit())
                .unwrap_or(after_dot.len());
            if end == 0 {
                return None;
            }
            fraction = format!("0.{}", &after_dot[..end]).parse().ok()?;
            rest = &after_dot[end..];
        }
    }
    let offset = match rest {
        "" | "Z" => 0,
        zone if zone.len() == 6 && zone.as_bytes()[3] == b':' => {
            let sign = match &zone[..1] {
                "+" => 1,
                "-" => -1,
                _ => return None,
            };
            sign * (zone[1..3].parse::<i64>().ok()? * 3600 + zone[4..6].parse::<i64>().ok()? * 60)
        }
        _ => return None,
    };
    let seconds = days_from_civil(year, month, day) * 86_400 + hour * 3600 + minute * 60 + second;
    Some((seconds - offset) as f64 + fraction)
}

/// The length of an `xsd:duration` in seconds, years and months at their Gregorian averages
pub(crate) fn duration_seconds(node: &Node) -> Option<f64> {
    let Node::Literal {
        lexical,
        datatype: Some(datatype),
        ..
    } = node
    else {
        return None;
    };
    if datatype != XSD_DURATION {
        return None;
    }
    let (negative, body) = match lexical.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, lexical.as_str()),
    };
    let body = body.strip_prefix('P')?;
    let (date, time) = match body.split_once('T') {
        Some((date, time)) => (date, Some(time)),
        None => (body, None),
    };
    let mut seconds = 0.0;
    let mut add = |part: &str, units: &[(char, f64)]| -> Option<()> {
        let mut number = String::new();
        let mut next_unit = 0;
        for c in part.chars() {
            if c.is_ascii_digit() || c == '.' {
                number.push(c);
                continue;
            }
            let position = units[next_unit..].iter().position(|(u, _)| *u == c)? + next_unit;
            seconds += number.parse::<f64>().ok()? * units[position].1;
            number.clear();
            next_unit = position + 1;
        }
        number.is_empty().then_some(())
    };
    add(
        date,
        &[('Y', 31_556_952.0), ('M', 2_629_746.0), ('D', 86_400.0)],
    )?;
    if let Some(time) = time {
        add(time, &[('H', 3_600.0), ('M', 60.0), ('S', 1.0)])?;
    }
    Some(if negative { -seconds } else { seconds })
}

/// A number of seconds as an `xsd:duration`
pub(crate) fn duration_literal(seconds: f64) -> Node {
    let sign = if seconds < 0.0 { "-" } else { "" };
    Node::typed(
        format!("{sign}PT{}S", shortest(seconds.abs())),
        XSD_DURATION,
    )
}
