/*
 * Copyright © 2024 Volodymyr Kadzhaia
 * Copyright © 2024 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! N3 built-in predicates: computed instead of looked up

use crate::lists::ListOp;
use crate::literals::LiteralOp;
use crate::matcher::Matcher;
use crate::numeric::{
    compare, datetime_seconds, duration_literal, duration_seconds, integer_literal, number_literal,
    numbers_equal, numeric, Numeric,
};
use crate::rules::PTerm;
use crate::scoped::ScopedOp;
use crate::strings::TextOp;
use crate::syntax::Node;
use crate::time::TimeOp;
use num_bigint::BigInt;
use num_traits::{Signed, ToPrimitive, Zero};
use shared::terms::Binding;
use std::cmp::Ordering;

const MATH: &str = "http://www.w3.org/2000/10/swap/math#";
const STRING: &str = "http://www.w3.org/2000/10/swap/string#";
const LOG: &str = "http://www.w3.org/2000/10/swap/log#";
const LIST: &str = "http://www.w3.org/2000/10/swap/list#";
const TIME: &str = "http://www.w3.org/2000/10/swap/time#";
const CRYPTO: &str = "http://www.w3.org/2000/10/swap/crypto#";

/// Largest exact power worth computing, in bits of the result
const MAX_EXACT_POWER_BITS: u64 = 2_000_000;

/// Longest text `string:format` may pad a field to
const MAX_FORMAT_WIDTH: usize = 16 * 1024 * 1024;

/// A built-in predicate this engine evaluates
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Builtin {
    /// `math:sum`
    Sum,
    /// `math:difference`
    Difference,
    /// `math:product`
    Product,
    /// `math:quotient`
    Quotient,
    /// `math:integerQuotient`
    IntegerQuotient,
    /// `math:remainder`
    Remainder,
    /// `math:exponentiation`
    Exponentiation,
    /// `math:negation`
    Negation,
    /// `math:absoluteValue`
    AbsoluteValue,
    /// `math:rounded`
    Rounded,
    /// A trigonometric or angle function with an inverse
    Trig(Trig),
    /// A numeric comparison
    Compare(Comparison),
    /// A string comparison or test
    Text(TextTest),
    /// `string:concatenation`
    Concatenation,
    /// `string:format`
    Format,
    /// `log:equalTo`
    EqualTo,
    /// `log:notEqualTo`
    NotEqualTo,
    /// A `list:` built-in
    List(ListOp),
    /// A `log:` built-in that reads a graph
    Scoped(ScopedOp),
    /// A regular expression, digest or generated identifier
    Strings(TextOp),
    /// A `log:` built-in over one term
    Term(LiteralOp),
    /// A `time:` built-in
    Time(TimeOp),
}

/// The built-ins eyeron implements that this engine does not yet, by namespace and local name;
/// any other name in a built-in namespace is an ordinary predicate, as in eyeron
const NOT_YET: [(&str, &str); 5] = [
    (
        "https://eyereasoner.github.io/eyeling/vocab/sudoku#",
        "solve",
    ),
    (LOG, "content"),
    (LOG, "semantics"),
    (LOG, "semanticsOrError"),
    (LOG, "parsedAsN3"),
];

/// The functions `math:` defines with an inverse
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Trig {
    /// `math:sin`
    Sin,
    /// `math:cos`
    Cos,
    /// `math:tan`
    Tan,
    /// `math:asin`
    Asin,
    /// `math:acos`
    Acos,
    /// `math:atan`
    Atan,
    /// `math:sinh`
    Sinh,
    /// `math:cosh`
    Cosh,
    /// `math:tanh`
    Tanh,
    /// `math:degrees`
    Degrees,
}

/// The numeric comparisons
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Comparison {
    /// `math:greaterThan`
    GreaterThan,
    /// `math:lessThan`
    LessThan,
    /// `math:notGreaterThan`
    NotGreaterThan,
    /// `math:notLessThan`
    NotLessThan,
    /// `math:equalTo`
    EqualTo,
    /// `math:notEqualTo`
    NotEqualTo,
}

/// The string comparisons and tests
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TextTest {
    /// `string:lessThan`
    LessThan,
    /// `string:greaterThan`
    GreaterThan,
    /// `string:notLessThan`
    NotLessThan,
    /// `string:notGreaterThan`
    NotGreaterThan,
    /// `string:contains`
    Contains,
    /// `string:containsIgnoringCase`
    ContainsIgnoringCase,
    /// `string:startsWith`
    StartsWith,
    /// `string:endsWith`
    EndsWith,
    /// `string:equalIgnoringCase`
    EqualIgnoringCase,
    /// `string:notEqualIgnoringCase`
    NotEqualIgnoringCase,
}

impl Builtin {
    /// The built-in an IRI names, if this engine implements it
    pub fn from_iri(iri: &str) -> Option<Self> {
        if let Some(name) = iri.strip_prefix(MATH) {
            return Some(match name {
                "sum" => Builtin::Sum,
                "difference" => Builtin::Difference,
                "product" => Builtin::Product,
                "quotient" => Builtin::Quotient,
                "integerQuotient" => Builtin::IntegerQuotient,
                "remainder" => Builtin::Remainder,
                "exponentiation" => Builtin::Exponentiation,
                "negation" => Builtin::Negation,
                "absoluteValue" => Builtin::AbsoluteValue,
                "rounded" => Builtin::Rounded,
                "sin" => Builtin::Trig(Trig::Sin),
                "cos" => Builtin::Trig(Trig::Cos),
                "tan" => Builtin::Trig(Trig::Tan),
                "asin" => Builtin::Trig(Trig::Asin),
                "acos" => Builtin::Trig(Trig::Acos),
                "atan" => Builtin::Trig(Trig::Atan),
                "sinh" => Builtin::Trig(Trig::Sinh),
                "cosh" => Builtin::Trig(Trig::Cosh),
                "tanh" => Builtin::Trig(Trig::Tanh),
                "degrees" => Builtin::Trig(Trig::Degrees),
                "greaterThan" => Builtin::Compare(Comparison::GreaterThan),
                "lessThan" => Builtin::Compare(Comparison::LessThan),
                "notGreaterThan" => Builtin::Compare(Comparison::NotGreaterThan),
                "notLessThan" => Builtin::Compare(Comparison::NotLessThan),
                "equalTo" => Builtin::Compare(Comparison::EqualTo),
                "notEqualTo" => Builtin::Compare(Comparison::NotEqualTo),
                _ => return None,
            });
        }
        if let Some(name) = iri.strip_prefix(STRING) {
            return Some(match name {
                "matches" => Builtin::Strings(TextOp::Matches),
                "notMatches" => Builtin::Strings(TextOp::NotMatches),
                "replace" => Builtin::Strings(TextOp::Replace),
                "scrape" => Builtin::Strings(TextOp::Scrape),
                "concatenation" => Builtin::Concatenation,
                "format" => Builtin::Format,
                "lessThan" => Builtin::Text(TextTest::LessThan),
                "greaterThan" => Builtin::Text(TextTest::GreaterThan),
                "notLessThan" => Builtin::Text(TextTest::NotLessThan),
                "notGreaterThan" => Builtin::Text(TextTest::NotGreaterThan),
                "contains" => Builtin::Text(TextTest::Contains),
                "containsIgnoringCase" => Builtin::Text(TextTest::ContainsIgnoringCase),
                "startsWith" => Builtin::Text(TextTest::StartsWith),
                "endsWith" => Builtin::Text(TextTest::EndsWith),
                "equalIgnoringCase" => Builtin::Text(TextTest::EqualIgnoringCase),
                "notEqualIgnoringCase" => Builtin::Text(TextTest::NotEqualIgnoringCase),
                _ => return None,
            });
        }
        if let Some(name) = iri.strip_prefix(LIST) {
            return ListOp::from_name(name).map(Builtin::List);
        }
        if let Some(name) = iri.strip_prefix(TIME) {
            return TimeOp::from_name(name).map(Builtin::Time);
        }
        if iri.strip_prefix(CRYPTO) == Some("sha") {
            return Some(Builtin::Strings(TextOp::Sha));
        }
        match iri.strip_prefix(LOG)? {
            "equalTo" => Some(Builtin::EqualTo),
            "notEqualTo" => Some(Builtin::NotEqualTo),
            "uuid" => Some(Builtin::Strings(TextOp::Uuid)),
            name => ScopedOp::from_name(name)
                .map(Builtin::Scoped)
                .or_else(|| LiteralOp::from_name(name).map(Builtin::Term)),
        }
    }

    /// Whether an IRI names a built-in eyeron implements and this engine does not yet
    pub(crate) fn not_yet(iri: &str) -> bool {
        NOT_YET
            .iter()
            .any(|(namespace, name)| iri.strip_prefix(namespace) == Some(name))
    }

    /// Whether the built-in reads the whole current graph, so its rule waits for saturation
    pub(crate) fn reads_whole_graph(self) -> bool {
        matches!(self, Builtin::Scoped(op) if op.reads_whole_graph())
    }

    /// Whether the atom can run in its forward mode, computing from its inputs alone; a
    /// reverse mode waits until no premise is left that could bind those inputs from facts
    pub(crate) fn forward(
        self,
        subject: &PTerm,
        object: &PTerm,
        ground: &dyn Fn(&PTerm) -> bool,
    ) -> bool {
        match self {
            Builtin::EqualTo | Builtin::Compare(_) | Builtin::Text(_) | Builtin::NotEqualTo => {
                ground(subject) && ground(object)
            }
            Builtin::List(op) => op.forward(subject, object, ground),
            Builtin::Scoped(op) => op.forward(subject, ground),
            Builtin::Term(op) => op.forward(subject, ground),
            Builtin::Strings(op) => op.ready(subject, object, ground),
            Builtin::Time(op) => op.ready(subject, ground),
            _ => ground(subject),
        }
    }

    /// Whether the atom can run in some mode with what is bound: `ground` says whether a
    /// position is fixed
    pub(crate) fn ready(
        self,
        subject: &PTerm,
        object: &PTerm,
        ground: &dyn Fn(&PTerm) -> bool,
    ) -> bool {
        match self {
            Builtin::Exponentiation => {
                ground(subject)
                    || (ground(object)
                        && matches!(subject, PTerm::List(items) if items.len() == 2 && ground(&items[0])))
            }
            Builtin::Negation | Builtin::Trig(_) => ground(subject) || ground(object),
            Builtin::Sum
            | Builtin::Difference
            | Builtin::Product
            | Builtin::Quotient
            | Builtin::IntegerQuotient
            | Builtin::Remainder
            | Builtin::AbsoluteValue
            | Builtin::Rounded
            | Builtin::Concatenation
            | Builtin::Format => ground(subject),
            Builtin::EqualTo => ground(subject) || ground(object),
            Builtin::Compare(_) | Builtin::Text(_) | Builtin::NotEqualTo => {
                ground(subject) && ground(object)
            }
            Builtin::List(op) => op.ready(subject, object, ground),
            Builtin::Scoped(op) => op.ready(),
            Builtin::Strings(op) => op.ready(subject, object, ground),
            Builtin::Term(op) => op.ready(subject, object, ground),
            Builtin::Time(op) => op.ready(subject, ground),
        }
    }

    /// Every extension of `binding` under which `subject builtin object` holds
    pub(crate) fn evaluate(
        self,
        matcher: &mut Matcher<'_>,
        subject: &PTerm,
        object: &PTerm,
        binding: Binding,
    ) -> Vec<Binding> {
        match self {
            Builtin::List(op) => return op.evaluate(matcher, subject, object, binding),
            Builtin::Scoped(op) => return op.evaluate(matcher, subject, object, binding),
            Builtin::Strings(op) => return op.evaluate(matcher, subject, object, binding),
            Builtin::Term(op) => return op.evaluate(matcher, subject, object, binding),
            Builtin::Time(op) => return op.evaluate(matcher, subject, object, binding),
            _ => {}
        }
        let s = matcher.ground(subject, &binding);
        let o = matcher.ground(object, &binding);
        match self {
            Builtin::Sum => {
                let Some(items) = numbers(matcher, s) else {
                    return Vec::new();
                };
                let result = match exact_all(&items) {
                    Some(exact) => integer_literal(&exact.into_iter().sum()),
                    None => number_literal(
                        items.iter().map(|n| n.value).sum(),
                        items.iter().all(|n| n.integer),
                    ),
                };
                emit(matcher, object, o, &result, binding)
            }
            Builtin::Product => {
                let Some(items) = numbers(matcher, s) else {
                    return Vec::new();
                };
                let result = match exact_all(&items) {
                    Some(exact) => integer_literal(&exact.into_iter().product()),
                    None => number_literal(
                        items.iter().map(|n| n.value).product(),
                        items.iter().all(|n| n.integer),
                    ),
                };
                emit(matcher, object, o, &result, binding)
            }
            Builtin::Difference if dates(matcher, s).is_some() => {
                let Some([a, b]) = dates(matcher, s) else {
                    return Vec::new();
                };
                emit(matcher, object, o, &duration_literal(a - b), binding)
            }
            Builtin::Difference
            | Builtin::Quotient
            | Builtin::IntegerQuotient
            | Builtin::Remainder => {
                let Some(pair) = numbers(matcher, s).filter(|items| items.len() == 2) else {
                    return Vec::new();
                };
                let Some(result) = binary(self, &pair[0], &pair[1]) else {
                    return Vec::new();
                };
                emit(matcher, object, o, &result, binding)
            }
            Builtin::Exponentiation => exponentiation(matcher, subject, object, s, o, binding),
            Builtin::Negation | Builtin::AbsoluteValue | Builtin::Rounded => {
                unary(self, matcher, subject, object, s, o, binding)
            }
            Builtin::Trig(function) => trig(function, matcher, subject, object, s, o, binding),
            Builtin::Compare(comparison) => {
                let (Some(left), Some(right)) = (comparable(matcher, s), comparable(matcher, o))
                else {
                    return Vec::new();
                };
                if left.1 != right.1 {
                    return Vec::new();
                }
                let (left, right) = (left.0, right.0);
                if holds(comparison, &left, &right) {
                    vec![binding]
                } else {
                    Vec::new()
                }
            }
            Builtin::Text(test) => {
                let (Some(left), Some(right)) = (text(matcher, s), text(matcher, o)) else {
                    return Vec::new();
                };
                if text_holds(test, &left, &right) {
                    vec![binding]
                } else {
                    Vec::new()
                }
            }
            Builtin::Concatenation => {
                let Some(parts) = texts(matcher, s) else {
                    return Vec::new();
                };
                emit(matcher, object, o, &plain(parts.concat()), binding)
            }
            Builtin::Format => {
                let Some(parts) = texts(matcher, s) else {
                    return Vec::new();
                };
                let Some((format, args)) = parts.split_first() else {
                    return Vec::new();
                };
                match simple_format(format, args) {
                    Some(result) => emit(matcher, object, o, &plain(result), binding),
                    None => Vec::new(),
                }
            }
            Builtin::EqualTo => match (s, o) {
                (Some(left), Some(right)) => {
                    if matcher.same_term(left, right) {
                        vec![binding]
                    } else {
                        Vec::new()
                    }
                }
                (Some(left), None) => matcher.unify(object, left, binding),
                (None, Some(right)) => matcher.unify(subject, right, binding),
                (None, None) => Vec::new(),
            },
            Builtin::NotEqualTo => match (s, o) {
                (Some(left), Some(right)) if !matcher.same_term(left, right) => vec![binding],
                _ => Vec::new(),
            },
            Builtin::List(_)
            | Builtin::Scoped(_)
            | Builtin::Strings(_)
            | Builtin::Term(_)
            | Builtin::Time(_) => Vec::new(),
        }
    }
}

fn plain(text: String) -> Node {
    Node::Literal {
        lexical: text,
        datatype: None,
        language: None,
    }
}

/// Binds or checks `object` against a computed result, comparing numbers by value
fn emit(
    matcher: &mut Matcher<'_>,
    object: &PTerm,
    ground: Option<u32>,
    result: &Node,
    binding: Binding,
) -> Vec<Binding> {
    if let Some(id) = ground {
        let existing = matcher.node(id);
        let equal = match (numeric(&existing), numeric(result)) {
            (Some(x), Some(y)) => numbers_equal(&x, &y),
            _ => matcher.intern(result) == id,
        };
        return if equal { vec![binding] } else { Vec::new() };
    }
    let id = matcher.intern(result);
    matcher.unify(object, id, binding)
}

fn number(matcher: &Matcher<'_>, id: Option<u32>) -> Option<Numeric> {
    numeric(&matcher.node(id?))
}

fn numbers(matcher: &Matcher<'_>, id: Option<u32>) -> Option<Vec<Numeric>> {
    matcher
        .list_items(id?)?
        .into_iter()
        .map(|member| number(matcher, Some(member)))
        .collect()
}

/// The instants of a pair of dates or date-times
fn dates(matcher: &Matcher<'_>, id: Option<u32>) -> Option<[f64; 2]> {
    let items = matcher.list_items(id?)?;
    let [a, b] = items.as_slice() else {
        return None;
    };
    Some([
        datetime_seconds(&matcher.node(*a))?,
        datetime_seconds(&matcher.node(*b))?,
    ])
}

/// What a comparison reads a term as, and whether it is a date: a number, a duration in
/// seconds, or a date's instant; a date only compares with another date
fn comparable(matcher: &Matcher<'_>, id: Option<u32>) -> Option<(Numeric, bool)> {
    let node = matcher.node(id?);
    if let Some(n) = numeric(&node) {
        return Some((n, false));
    }
    let as_float = |value| Numeric {
        value,
        integer: false,
        exact: None,
    };
    if let Some(instant) = datetime_seconds(&node) {
        return Some((as_float(instant), true));
    }
    Some((as_float(duration_seconds(&node)?), false))
}

fn exact_all(items: &[Numeric]) -> Option<Vec<BigInt>> {
    items.iter().map(|n| n.exact.clone()).collect()
}

/// The text a term stands for: a literal's lexical form, or an IRI
fn text(matcher: &Matcher<'_>, id: Option<u32>) -> Option<String> {
    match matcher.node(id?) {
        Node::Literal { lexical, .. } => Some(lexical),
        Node::Iri(iri) => Some(iri),
        _ => None,
    }
}

fn texts(matcher: &Matcher<'_>, id: Option<u32>) -> Option<Vec<String>> {
    matcher
        .list_items(id?)?
        .into_iter()
        .map(|member| text(matcher, Some(member)))
        .collect()
}

fn binary(builtin: Builtin, a: &Numeric, b: &Numeric) -> Option<Node> {
    let exact = a.exact.as_ref().zip(b.exact.as_ref());
    match builtin {
        Builtin::Difference => Some(match exact {
            Some((x, y)) => integer_literal(&(x - y)),
            None => number_literal(a.value - b.value, a.integer && b.integer),
        }),
        Builtin::Quotient => {
            if b.value == 0.0 {
                return None;
            }
            Some(number_literal(a.value / b.value, a.integer && b.integer))
        }
        Builtin::IntegerQuotient => match exact {
            Some((x, y)) if !y.is_zero() => Some(integer_literal(&(x / y))),
            Some(_) => None,
            None if b.value == 0.0 => None,
            None => Some(number_literal((a.value / b.value).trunc(), true)),
        },
        Builtin::Remainder => match exact {
            Some((x, y)) if !y.is_zero() => Some(integer_literal(&(x % y))),
            Some(_) => None,
            None if b.value == 0.0 => None,
            None => Some(number_literal(a.value % b.value, true)),
        },
        _ => None,
    }
}

/// The whole exponent that raises one integer to another exactly, if there is one; an
/// integer too large for a float has no float logarithm to round
fn exact_logarithm(base: &Numeric, result: &Numeric) -> Option<u32> {
    let (base, result) = (base.exact.as_ref()?, result.exact.as_ref()?);
    if *base <= BigInt::from(1) {
        return None;
    }
    let estimate = (log2(result) / log2(base)).round();
    if !estimate.is_finite() || estimate < 0.0 || estimate > f64::from(u32::MAX) {
        return None;
    }
    let estimate = estimate as u32;
    [
        estimate.saturating_sub(1),
        estimate,
        estimate.saturating_add(1),
    ]
    .into_iter()
    .find(|&exponent| {
        base.bits().saturating_mul(u64::from(exponent)) <= MAX_EXACT_POWER_BITS + base.bits()
            && base.pow(exponent) == *result
    })
}

/// The base-2 logarithm of a positive integer of any size, to float precision
fn log2(value: &BigInt) -> f64 {
    let bits = value.bits();
    let shift = bits.saturating_sub(64);
    let top = (value >> shift).to_f64().unwrap_or(f64::NAN);
    top.log2() + shift as f64
}

fn exact_power(base: &Numeric, exponent: &Numeric) -> Option<Node> {
    let base = base.exact.as_ref()?;
    let exponent = exponent.exact.as_ref()?.to_u32()?;
    let bits = base.bits().checked_mul(u64::from(exponent))?;
    if bits > MAX_EXACT_POWER_BITS {
        return None;
    }
    Some(integer_literal(&base.pow(exponent)))
}

fn exponentiation(
    matcher: &mut Matcher<'_>,
    subject: &PTerm,
    object: &PTerm,
    s: Option<u32>,
    o: Option<u32>,
    binding: Binding,
) -> Vec<Binding> {
    if let Some(pair) = numbers(matcher, s).filter(|items| items.len() == 2) {
        let result = exact_power(&pair[0], &pair[1]).unwrap_or_else(|| {
            number_literal(
                pair[0].value.powf(pair[1].value),
                pair[0].integer && pair[1].integer,
            )
        });
        return emit(matcher, object, o, &result, binding);
    }
    // The inverse: `(base ?exponent) math:exponentiation result`.
    let PTerm::List(items) = subject else {
        return Vec::new();
    };
    let (Some(base), Some(result)) = (items.first().and_then(|b| matcher.ground(b, &binding)), o)
    else {
        return Vec::new();
    };
    let (Some(base), Some(result)) = (number(matcher, Some(base)), number(matcher, Some(result)))
    else {
        return Vec::new();
    };
    if base.value <= 0.0 || result.value <= 0.0 || items.len() != 2 {
        return Vec::new();
    }
    let exponent = match exact_logarithm(&base, &result) {
        Some(exponent) => integer_literal(&BigInt::from(exponent)),
        None => number_literal(result.value.ln() / base.value.ln(), true),
    };
    let id = matcher.intern(&exponent);
    matcher.unify(&items[1], id, binding)
}

fn unary(
    builtin: Builtin,
    matcher: &mut Matcher<'_>,
    subject: &PTerm,
    object: &PTerm,
    s: Option<u32>,
    o: Option<u32>,
    binding: Binding,
) -> Vec<Binding> {
    let apply = |n: &Numeric| -> Node {
        match (&n.exact, builtin) {
            (Some(x), Builtin::Negation) => integer_literal(&-x),
            (Some(x), Builtin::AbsoluteValue) => integer_literal(&x.abs()),
            (Some(x), _) => integer_literal(x),
            (None, Builtin::Negation) => number_literal(-n.value, true),
            (None, Builtin::AbsoluteValue) => number_literal(n.value.abs(), true),
            (None, _) => number_literal((n.value + 0.5).floor(), true),
        }
    };
    if let Some(n) = number(matcher, s) {
        let result = apply(&n);
        return emit(matcher, object, o, &result, binding);
    }
    if builtin != Builtin::Negation || s.is_some() {
        return Vec::new();
    }
    let Some(n) = number(matcher, o) else {
        return Vec::new();
    };
    let id = matcher.intern(&apply(&n));
    matcher.unify(subject, id, binding)
}

/// A function of one float
type Float = fn(f64) -> f64;

/// A function and its inverse
fn trig_pair(function: Trig) -> (Float, Float) {
    match function {
        Trig::Sin => (f64::sin, f64::asin),
        Trig::Cos => (f64::cos, f64::acos),
        Trig::Tan => (f64::tan, f64::atan),
        Trig::Asin => (f64::asin, f64::sin),
        Trig::Acos => (f64::acos, f64::cos),
        Trig::Atan => (f64::atan, f64::tan),
        Trig::Sinh => (f64::sinh, f64::asinh),
        Trig::Cosh => (f64::cosh, f64::acosh),
        Trig::Tanh => (f64::tanh, f64::atanh),
        Trig::Degrees => (f64::to_degrees, f64::to_radians),
    }
}

fn trig(
    function: Trig,
    matcher: &mut Matcher<'_>,
    subject: &PTerm,
    object: &PTerm,
    s: Option<u32>,
    o: Option<u32>,
    binding: Binding,
) -> Vec<Binding> {
    let (forward, inverse) = trig_pair(function);
    if let Some(n) = number(matcher, s) {
        let result = number_literal(forward(n.value), n.integer);
        return emit(matcher, object, o, &result, binding);
    }
    if s.is_some() {
        return Vec::new();
    }
    let Some(n) = number(matcher, o) else {
        return Vec::new();
    };
    let computed = inverse(n.value);
    if computed.is_nan() {
        return Vec::new();
    }
    let id = matcher.intern(&number_literal(computed, n.integer));
    matcher.unify(subject, id, binding)
}

fn holds(comparison: Comparison, left: &Numeric, right: &Numeric) -> bool {
    let exact = left
        .exact
        .as_ref()
        .zip(right.exact.as_ref())
        .map(|(x, y)| x.cmp(y));
    let ordering = compare(left, right);
    match comparison {
        Comparison::GreaterThan => ordering == Some(Ordering::Greater),
        Comparison::LessThan => ordering == Some(Ordering::Less),
        Comparison::NotGreaterThan => {
            matches!(ordering, Some(Ordering::Less | Ordering::Equal))
        }
        Comparison::NotLessThan => {
            matches!(ordering, Some(Ordering::Greater | Ordering::Equal))
        }
        Comparison::EqualTo => match exact {
            Some(o) => o.is_eq(),
            None => (left.value - right.value).abs() <= f64::EPSILON,
        },
        Comparison::NotEqualTo => match exact {
            Some(o) => o.is_ne(),
            None => (left.value - right.value).abs() > f64::EPSILON,
        },
    }
}

fn text_holds(test: TextTest, left: &str, right: &str) -> bool {
    let lower = || (left.to_lowercase(), right.to_lowercase());
    match test {
        TextTest::LessThan => left < right,
        TextTest::GreaterThan => left > right,
        TextTest::NotLessThan => left >= right,
        TextTest::NotGreaterThan => left <= right,
        TextTest::Contains => left.contains(right),
        TextTest::StartsWith => left.starts_with(right),
        TextTest::EndsWith => left.ends_with(right),
        TextTest::ContainsIgnoringCase => {
            let (l, r) = lower();
            l.contains(&r)
        }
        TextTest::EqualIgnoringCase => {
            let (l, r) = lower();
            l == r
        }
        TextTest::NotEqualIgnoringCase => {
            let (l, r) = lower();
            l != r
        }
    }
}

/// `printf`-style formatting with `%s`, `%d` and `%f`, flags `-` and `0`, width and precision
fn simple_format(format: &str, args: &[String]) -> Option<String> {
    let mut out = String::new();
    let mut chars = format.chars().peekable();
    let mut next_arg = 0;
    while let Some(c) = chars.next() {
        if c != '%' {
            out.push(c);
            continue;
        }
        if chars.peek() == Some(&'%') {
            chars.next();
            out.push('%');
            continue;
        }
        let left = chars.next_if_eq(&'-').is_some();
        let zero = chars.next_if_eq(&'0').is_some();
        let mut width = String::new();
        while let Some(d) = chars.next_if(char::is_ascii_digit) {
            width.push(d);
        }
        let mut precision = None;
        if chars.next_if_eq(&'.').is_some() {
            let mut digits = String::new();
            while let Some(d) = chars.next_if(char::is_ascii_digit) {
                digits.push(d);
            }
            precision = digits.parse::<usize>().ok();
        }
        let spec = chars.next()?;
        let arg = args.get(next_arg)?;
        next_arg += 1;
        let mut rendered = match spec {
            's' => match precision {
                Some(p) => arg.chars().take(p).collect(),
                None => arg.clone(),
            },
            'd' => format!("{:.0}", arg.parse::<f64>().ok()?.trunc()),
            'f' => format!("{:.*}", precision.unwrap_or(6), arg.parse::<f64>().ok()?),
            _ => return None,
        };
        if let Ok(width) = width.parse::<usize>() {
            if width > MAX_FORMAT_WIDTH {
                return None;
            }
            if rendered.len() < width {
                let pad: String = std::iter::repeat_n(
                    if zero && !left { '0' } else { ' ' },
                    width - rendered.len(),
                )
                .collect();
                rendered = if left {
                    rendered + &pad
                } else {
                    pad + &rendered
                };
            }
        }
        out.push_str(&rendered);
    }
    (next_arg == args.len()).then_some(out)
}
