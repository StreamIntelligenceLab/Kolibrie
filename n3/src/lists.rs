/*
 * Copyright © 2024 Volodymyr Kadzhaia
 * Copyright © 2024 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! The `list:` built-ins, over native lists and `rdf:first`/`rdf:rest` chains alike

use crate::builtins::Builtin;
use crate::matcher::Matcher;
use crate::numeric::{number_literal, numeric};
use crate::rules::PTerm;
use crate::syntax::Node;
use shared::terms::Binding;
use std::cmp::Ordering;

/// A `list:` built-in
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ListOp {
    /// `list:first`
    First,
    /// `list:rest`
    Rest,
    /// `list:firstRest`
    FirstRest,
    /// `list:last`
    Last,
    /// `list:length`
    Length,
    /// `list:member`
    Member,
    /// `list:in`
    In,
    /// `list:memberAt`
    MemberAt,
    /// `list:remove`
    Remove,
    /// `list:notMember`
    NotMember,
    /// `list:append`
    Append,
    /// `list:reverse`
    Reverse,
    /// `list:sort`
    Sort,
    /// `list:iterate`
    Iterate,
    /// `list:map`
    Map,
}

impl ListOp {
    /// The operation a `list:` local name names
    pub(crate) fn from_name(name: &str) -> Option<Self> {
        Some(match name {
            "first" => ListOp::First,
            "rest" => ListOp::Rest,
            "firstRest" => ListOp::FirstRest,
            "last" => ListOp::Last,
            "length" => ListOp::Length,
            "member" => ListOp::Member,
            "in" => ListOp::In,
            "memberAt" => ListOp::MemberAt,
            "remove" => ListOp::Remove,
            "notMember" => ListOp::NotMember,
            "append" => ListOp::Append,
            "reverse" => ListOp::Reverse,
            "sort" => ListOp::Sort,
            "iterate" => ListOp::Iterate,
            "map" => ListOp::Map,
            _ => return None,
        })
    }

    /// Whether the atom can run computing from its inputs alone
    pub(crate) fn forward(
        self,
        subject: &PTerm,
        object: &PTerm,
        ground: &dyn Fn(&PTerm) -> bool,
    ) -> bool {
        match self {
            ListOp::In => ground(object),
            ListOp::NotMember => ground(subject) && ground(object),
            _ => ground(subject),
        }
    }

    /// Whether the atom can run in some mode with what is bound
    pub(crate) fn ready(
        self,
        subject: &PTerm,
        object: &PTerm,
        ground: &dyn Fn(&PTerm) -> bool,
    ) -> bool {
        match self {
            ListOp::In => ground(object),
            ListOp::NotMember => ground(subject) && ground(object),
            ListOp::FirstRest | ListOp::Reverse | ListOp::Sort | ListOp::Append => {
                ground(subject) || ground(object)
            }
            _ => ground(subject),
        }
    }

    /// Every extension of `binding` under which the atom holds
    pub(crate) fn evaluate(
        self,
        m: &mut Matcher<'_>,
        subject: &PTerm,
        object: &PTerm,
        binding: Binding,
    ) -> Vec<Binding> {
        let s = m.ground(subject, &binding);
        let o = m.ground(object, &binding);
        let items = s.and_then(|id| m.list_items(id));
        match self {
            ListOp::First => match items.as_deref() {
                Some([first, ..]) => m.unify_value(object, *first, binding),
                _ => Vec::new(),
            },
            ListOp::Rest => match items {
                Some(items) if !items.is_empty() => {
                    let rest = m.store.list(items[1..].to_vec());
                    m.unify_value(object, rest, binding)
                }
                _ => Vec::new(),
            },
            ListOp::Last => match items.as_deref().and_then(<[u32]>::last) {
                Some(&last) => m.unify_value(object, last, binding),
                None => Vec::new(),
            },
            ListOp::Length => match items {
                Some(items) => {
                    let length = m.intern(&number_literal(items.len() as f64, true));
                    m.unify_value(object, length, binding)
                }
                None => Vec::new(),
            },
            ListOp::Member => items
                .unwrap_or_default()
                .into_iter()
                .flat_map(|item| m.unify_value(object, item, binding.clone()))
                .collect(),
            ListOp::In => o
                .and_then(|id| m.list_items(id))
                .unwrap_or_default()
                .into_iter()
                .flat_map(|item| m.unify_value(subject, item, binding.clone()))
                .collect(),
            ListOp::NotMember => match (items, o) {
                (Some(items), Some(candidate)) => {
                    if items.iter().any(|&item| m.equivalent(item, candidate)) {
                        Vec::new()
                    } else {
                        vec![binding]
                    }
                }
                _ => Vec::new(),
            },
            ListOp::MemberAt => {
                let Some([list, index]) = items.and_then(|parts| <[u32; 2]>::try_from(parts).ok())
                else {
                    return Vec::new();
                };
                let (Some(members), Some(index)) = (m.list_items(list), numeric(&m.node(index)))
                else {
                    return Vec::new();
                };
                if index.value < 0.0 || index.value.fract() != 0.0 {
                    return Vec::new();
                }
                match members.get(index.value as usize) {
                    Some(&member) => m.unify_value(object, member, binding),
                    None => Vec::new(),
                }
            }
            ListOp::Remove => {
                let Some([list, unwanted]) =
                    items.and_then(|parts| <[u32; 2]>::try_from(parts).ok())
                else {
                    return Vec::new();
                };
                let Some(members) = m.list_items(list) else {
                    return Vec::new();
                };
                let kept: Vec<u32> = members
                    .into_iter()
                    .filter(|&item| !m.equivalent(item, unwanted))
                    .collect();
                let kept = m.store.list(kept);
                m.unify_value(object, kept, binding)
            }
            ListOp::FirstRest => {
                if let Some(items) = items {
                    let Some((&first, rest)) = items.split_first() else {
                        return Vec::new();
                    };
                    let rest = m.store.list(rest.to_vec());
                    let pair = m.store.list(vec![first, rest]);
                    return m.unify_value(object, pair, binding);
                }
                let Some([first, rest]) = o
                    .and_then(|id| m.list_items(id))
                    .and_then(|pair| <[u32; 2]>::try_from(pair).ok())
                else {
                    return Vec::new();
                };
                let Some(rest) = m.list_items(rest) else {
                    return Vec::new();
                };
                let whole = m.store.list([vec![first], rest].concat());
                m.unify_value(subject, whole, binding)
            }
            ListOp::Reverse => {
                let (source, target) = match items {
                    Some(items) => (items, object),
                    None => match o.and_then(|id| m.list_items(id)) {
                        Some(items) => (items, subject),
                        None => return Vec::new(),
                    },
                };
                let reversed = m.store.list(source.into_iter().rev().collect());
                m.unify_value(target, reversed, binding)
            }
            ListOp::Sort => {
                let (mut source, target) = match items {
                    Some(items) => (items, object),
                    None => match o.and_then(|id| m.list_items(id)) {
                        Some(items) => (items, subject),
                        None => return Vec::new(),
                    },
                };
                source.sort_by(|a, b| sort_order(m, *a, *b));
                let sorted = m.store.list(source);
                m.unify_value(target, sorted, binding)
            }
            ListOp::Iterate => {
                let Some(items) = items else {
                    return Vec::new();
                };
                let mut out = Vec::new();
                for (index, item) in items.into_iter().enumerate() {
                    let index = m.intern(&number_literal(index as f64, true));
                    let pair = m.store.list(vec![index, item]);
                    out.extend(m.unify_value(object, pair, binding.clone()));
                }
                out
            }
            ListOp::Append => append(m, subject, object, items, o, binding),
            ListOp::Map => map(m, object, items, binding),
        }
    }
}

/// `list:append`: concatenates known parts, or splits a known result among unknown parts
fn append(
    m: &mut Matcher<'_>,
    subject: &PTerm,
    object: &PTerm,
    parts: Option<Vec<u32>>,
    result: Option<u32>,
    binding: Binding,
) -> Vec<Binding> {
    if let Some(parts) = parts {
        let mut joined = Vec::new();
        for part in parts {
            let Some(items) = m.list_items(part) else {
                return Vec::new();
            };
            joined.extend(items);
        }
        let joined = m.store.list(joined);
        return m.unify_value(object, joined, binding);
    }
    let (PTerm::List(parts), Some(result)) = (subject, result) else {
        return Vec::new();
    };
    let Some(result) = m.list_items(result) else {
        return Vec::new();
    };
    let mut out = Vec::new();
    split(m, parts, &result, binding, &mut out);
    out
}

/// Assigns consecutive slices of `result` to `parts`, every way that fits
fn split(
    m: &mut Matcher<'_>,
    parts: &[PTerm],
    result: &[u32],
    binding: Binding,
    out: &mut Vec<Binding>,
) {
    let Some((part, rest)) = parts.split_first() else {
        if result.is_empty() && !out.contains(&binding) {
            out.push(binding);
        }
        return;
    };
    let fixed = |p: &PTerm| match p {
        PTerm::List(items) => Some(items.len()),
        _ => None,
    };
    let lengths: Vec<usize> = match fixed(part) {
        Some(length) => vec![length],
        None => {
            let reserved: usize = rest.iter().filter_map(fixed).sum();
            (0..=result.len().saturating_sub(reserved)).collect()
        }
    };
    for length in lengths {
        if length > result.len() {
            continue;
        }
        let slice = m.store.list(result[..length].to_vec());
        for next in m.unify_value(part, slice, binding.clone()) {
            split(m, rest, &result[length..], next, out);
        }
    }
}

/// `list:map`: applies a predicate to every member and collects its values
fn map(
    m: &mut Matcher<'_>,
    object: &PTerm,
    parts: Option<Vec<u32>>,
    binding: Binding,
) -> Vec<Binding> {
    let Some([inputs, predicate]) = parts.and_then(|p| <[u32; 2]>::try_from(p).ok()) else {
        return Vec::new();
    };
    let (Some(inputs), Node::Iri(iri)) = (m.list_items(inputs), m.node(predicate)) else {
        return Vec::new();
    };
    let output = PTerm::Var("?list:map output".to_string());
    let mut mapped = Vec::new();
    for input in inputs {
        let solutions = match Builtin::from_iri(&iri) {
            Some(builtin) => builtin.evaluate(m, &PTerm::Const(input), &output, Binding::new()),
            None => {
                let mut values: Vec<u32> = m
                    .facts
                    .query(Some(input), Some(predicate), None)
                    .into_iter()
                    .map(|t| t.object)
                    .collect();
                values.sort_unstable();
                values
                    .into_iter()
                    .map(|value| Binding::from([("?list:map output".to_string(), value)]))
                    .collect()
            }
        };
        mapped.extend(
            solutions
                .iter()
                .filter_map(|solution| solution.get("?list:map output").copied()),
        );
    }
    let mapped = m.store.list(mapped);
    m.unify_value(object, mapped, binding)
}

/// The order `list:sort` puts terms in: numbers by value, other literals by text, lists by
/// their members, IRIs by text, and lists before anything else
fn sort_order(m: &Matcher<'_>, a: u32, b: u32) -> Ordering {
    if let (Some(x), Some(y)) = (m.list_items(a), m.list_items(b)) {
        for (p, q) in x.iter().zip(&y) {
            let ordering = sort_order(m, *p, *q);
            if ordering != Ordering::Equal {
                return ordering;
            }
        }
        return x.len().cmp(&y.len());
    }
    let (left, right) = (m.node(a), m.node(b));
    match (&left, &right) {
        (Node::Literal { lexical: l, .. }, Node::Literal { lexical: r, .. }) => {
            match (numeric(&left), numeric(&right)) {
                (Some(x), Some(y)) => x.value.partial_cmp(&y.value).unwrap_or(Ordering::Equal),
                _ => l.cmp(r),
            }
        }
        (Node::Iri(l), Node::Iri(r)) => l.cmp(r),
        (Node::List(_), _) => Ordering::Less,
        (_, Node::List(_)) => Ordering::Greater,
        _ => m.store.n3(a).cmp(&m.store.n3(b)),
    }
}
