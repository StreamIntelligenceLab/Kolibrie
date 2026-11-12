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
