/*
 * Copyright © 2025 Volodymyr Kadzhaia
 * Copyright © 2025 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::cmp::Ordering;

/// Rank of a value's class, which decides ordering before any value is compared
const CLASS_UNBOUND: u8 = 0;
const CLASS_NUMBER: u8 = 1;
const CLASS_OTHER: u8 = 2;

/// A comparable view of one solution value
#[derive(Debug, Clone, Copy)]
pub struct OrderKey<'a> {
    class: u8,
    number: f64,
    lexical: &'a str,
}

impl<'a> OrderKey<'a> {
    /// Classifies a value; `None` is an unbound variable
    pub fn new(value: Option<&'a str>) -> Self {
        match value {
            None => OrderKey {
                class: CLASS_UNBOUND,
                number: 0.0,
                lexical: "",
            },
            Some(value) => match value.parse::<f64>() {
                Ok(number) => OrderKey {
                    class: CLASS_NUMBER,
                    number,
                    lexical: value,
                },
                Err(_) => OrderKey {
                    class: CLASS_OTHER,
                    number: 0.0,
                    lexical: value,
                },
            },
        }
    }
}

impl PartialEq for OrderKey<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl Eq for OrderKey<'_> {}

impl PartialOrd for OrderKey<'_> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for OrderKey<'_> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.class
            .cmp(&other.class)
            // `total_cmp` keeps NaN and the two zeroes ordered, which `partial_cmp` cannot
            .then_with(|| self.number.total_cmp(&other.number))
            // Two spellings of one number ("1" and "1.0") are numerically
            .then_with(|| self.lexical.cmp(other.lexical))
    }
}

/// Compares two solution values, either of which may be unbound
pub fn compare(left: Option<&str>, right: Option<&str>) -> Ordering {
    OrderKey::new(left).cmp(&OrderKey::new(right))
}

/// Returns the smallest of the given values, or `None` if there are none
pub fn minimum<'a, I: IntoIterator<Item = &'a str>>(values: I) -> Option<&'a str> {
    values
        .into_iter()
        .min_by(|left, right| compare(Some(left), Some(right)))
}

/// Returns the largest of the given values, or `None` if there are none
pub fn maximum<'a, I: IntoIterator<Item = &'a str>>(values: I) -> Option<&'a str> {
    values
        .into_iter()
        .max_by(|left, right| compare(Some(left), Some(right)))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The values that break a numeric-or-lexical comparator
    const CYCLE: [&str; 3] = ["9", "100", "50x"];

    /// A spread covering both classes, both zeroes, NaN, and empty strings
    const SPREAD: [&str; 16] = [
        "9", "100", "50x", "1", "20", "3.5", "abc", "007", "-4", "1e3", "z", "0", "-0",
        "NaN", "", "  ",
    ];

    fn keys(values: &[&'static str]) -> Vec<OrderKey<'static>> {
        values.iter().map(|value| OrderKey::new(Some(value))).collect()
    }

    #[test]
    fn the_cycle_that_motivated_this_module_is_gone() {
        let mut values = CYCLE;
        values.sort_by(|left, right| compare(Some(left), Some(right)));
        assert_eq!(values, ["9", "100", "50x"]);
    }

    #[test]
    fn numbers_sort_before_other_terms() {
        assert_eq!(compare(Some("999999"), Some("abc")), Ordering::Less);
        assert_eq!(compare(Some("abc"), Some("999999")), Ordering::Greater);
    }

    #[test]
    fn unbound_sorts_before_everything() {
        assert_eq!(compare(None, Some("-99999")), Ordering::Less);
        assert_eq!(compare(None, Some("")), Ordering::Less);
        assert_eq!(compare(None, None), Ordering::Equal);
    }

    #[test]
    fn numbers_compare_by_value_not_by_spelling() {
        assert_eq!(compare(Some("9"), Some("100")), Ordering::Less);
        assert_eq!(compare(Some("1e3"), Some("999")), Ordering::Greater);
        // Numerically equal, so the lexical form decides — but it decides
        assert_eq!(compare(Some("007"), Some("7")), Ordering::Less);
        assert_eq!(compare(Some("7"), Some("007")), Ordering::Greater);
    }

    #[test]
    fn the_order_is_reflexive() {
        for key in keys(&SPREAD) {
            assert_eq!(key.cmp(&key), Ordering::Equal);
        }
    }

    #[test]
    fn the_order_is_antisymmetric() {
        let keys = keys(&SPREAD);
        for left in &keys {
            for right in &keys {
                assert_eq!(
                    left.cmp(right),
                    right.cmp(left).reverse(),
                    "{:?} against {:?}",
                    left,
                    right
                );
            }
        }
    }

    #[test]
    fn the_order_is_transitive() {
        let keys = keys(&SPREAD);
        for left in &keys {
            for middle in &keys {
                if left.cmp(middle) != Ordering::Less {
                    continue;
                }
                for right in &keys {
                    if middle.cmp(right) != Ordering::Less {
                        continue;
                    }
                    assert_eq!(
                        left.cmp(right),
                        Ordering::Less,
                        "{:?} < {:?} < {:?}",
                        left,
                        middle,
                        right
                    );
                }
            }
        }
    }

    #[test]
    fn sorting_is_deterministic_whatever_the_input_order() {
        let mut forward = SPREAD;
        forward.sort_by(|left, right| compare(Some(left), Some(right)));

        let mut backward = SPREAD;
        backward.reverse();
        backward.sort_by(|left, right| compare(Some(left), Some(right)));

        assert_eq!(forward, backward);
    }

    #[test]
    fn minimum_and_maximum_span_both_classes() {
        let values = ["abc", "9", "100", "50x"];
        assert_eq!(minimum(values), Some("9"));
        assert_eq!(maximum(values), Some("abc"));
        assert_eq!(minimum(Vec::<&str>::new()), None);
        assert_eq!(maximum(Vec::<&str>::new()), None);
    }
}
