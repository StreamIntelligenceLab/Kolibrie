/*
 * Copyright © 2024 Volodymyr Kadzhaia
 * Copyright © 2024 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! Built-ins over text: regular expressions, digests and generated identifiers

use crate::matcher::Matcher;
use crate::rules::PTerm;
use crate::syntax::Node;
use regex::Regex;
use shared::terms::Binding;
use std::cell::RefCell;
use std::collections::HashMap;

/// Most compiled patterns kept per thread
const REGEX_CACHE_CAPACITY: usize = 16;

thread_local! {
    static REGEX_CACHE: RefCell<HashMap<String, Option<Regex>>> = RefCell::new(HashMap::new());
}

/// A built-in over text
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TextOp {
    /// `string:matches`
    Matches,
    /// `string:notMatches`
    NotMatches,
    /// `string:replace`
    Replace,
    /// `string:scrape`
    Scrape,
    /// `crypto:sha`, the SHA-1 digest in hex
    Sha,
    /// `log:uuid`, a UUID-shaped string derived from the term's SHA-1
    Uuid,
}

impl TextOp {
    /// Whether the atom can run with what is bound
    pub(crate) fn ready(
        self,
        subject: &PTerm,
        object: &PTerm,
        ground: &dyn Fn(&PTerm) -> bool,
    ) -> bool {
        match self {
            TextOp::Matches | TextOp::NotMatches => ground(subject) && ground(object),
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
        let text = |m: &Matcher<'_>, id: Option<u32>| match m.node(id?) {
            Node::Literal { lexical, .. } => Some(lexical),
            Node::Iri(iri) => Some(iri),
            _ => None,
        };
        let parts = |m: &Matcher<'_>, id: Option<u32>| -> Option<Vec<String>> {
            m.list_items(id?)?
                .into_iter()
                .map(|item| text(m, Some(item)))
                .collect()
        };
        let result = match self {
            TextOp::Matches | TextOp::NotMatches => {
                let (Some(subject_text), Some(pattern)) = (text(m, s), text(m, o)) else {
                    return Vec::new();
                };
                let Some(matched) = with_regex(&pattern, |re| re.is_match(&subject_text)) else {
                    return Vec::new();
                };
                return if matched == (self == TextOp::Matches) {
                    vec![binding]
                } else {
                    Vec::new()
                };
            }
            TextOp::Replace => {
                let Some([input, from, to]) =
                    parts(m, s).and_then(|p| <[String; 3]>::try_from(p).ok())
                else {
                    return Vec::new();
                };
                let replacement = rust_replacement(&to);
                match with_regex(&from, |re| {
                    re.replace_all(&input, replacement.as_str()).into_owned()
                }) {
                    Some(replaced) => replaced,
                    None => input.replace(&from, &to),
                }
            }
            TextOp::Scrape => {
                let Some([input, pattern]) =
                    parts(m, s).and_then(|p| <[String; 2]>::try_from(p).ok())
                else {
                    return Vec::new();
                };
                let scraped = with_regex(&pattern, |re| {
                    re.captures(&input).and_then(|captures| {
                        (1..captures.len())
                            .find_map(|index| captures.get(index))
                            .or_else(|| captures.get(0))
                            .map(|found| found.as_str().to_string())
                    })
                });
                match scraped.flatten() {
                    Some(found) => found,
                    None => return Vec::new(),
                }
            }
            TextOp::Sha => match text(m, s) {
                Some(input) => sha1_hex(input.as_bytes()),
                None => return Vec::new(),
            },
            TextOp::Uuid => match text(m, s) {
                Some(input) => uuid_of(&input),
                None => return Vec::new(),
            },
        };
        let id = m.intern(&Node::Literal {
            lexical: result,
            datatype: None,
            language: None,
        });
        m.unify_value(object, id, binding)
    }
}

/// Runs `apply` with the compiled pattern, or returns `None` when it does not compile
fn with_regex<T>(pattern: &str, apply: impl FnOnce(&Regex) -> T) -> Option<T> {
    let compiled = REGEX_CACHE.with(|cache| {
        let mut cache = cache.borrow_mut();
        if let Some(found) = cache.get(pattern) {
            return found.clone();
        }
        if cache.len() >= REGEX_CACHE_CAPACITY {
            cache.clear();
        }
        let compiled = Regex::new(pattern).ok();
        cache.insert(pattern.to_string(), compiled.clone());
        compiled
    });
    compiled.as_ref().map(apply)
}

/// Rewrites an XPath-style replacement for the regex crate: `\$` is a literal dollar sign and
/// `\\` a literal backslash
fn rust_replacement(replacement: &str) -> String {
    let mut out = String::with_capacity(replacement.len());
    let mut chars = replacement.chars().peekable();
    while let Some(c) = chars.next() {
        if c != '\\' {
            out.push(c);
            continue;
        }
        match chars.next() {
            Some('$') => out.push_str("$$"),
            Some('\\') => out.push('\\'),
            Some(other) => {
                out.push('\\');
                out.push(other);
            }
            None => out.push('\\'),
        }
    }
    out
}

/// The SHA-1 digest of `data` as lowercase hex
pub(crate) fn sha1_hex(data: &[u8]) -> String {
    let mut h: [u32; 5] = [
        0x6745_2301,
        0xEFCD_AB89,
        0x98BA_DCFE,
        0x1032_5476,
        0xC3D2_E1F0,
    ];
    let mut message = data.to_vec();
    message.push(0x80);
    while message.len() % 64 != 56 {
        message.push(0);
    }
    message.extend_from_slice(&((data.len() as u64) * 8).to_be_bytes());
    for chunk in message.chunks(64) {
        let mut w = [0u32; 80];
        for (i, word) in chunk.chunks(4).enumerate() {
            w[i] = u32::from_be_bytes([word[0], word[1], word[2], word[3]]);
        }
        for i in 16..80 {
            w[i] = (w[i - 3] ^ w[i - 8] ^ w[i - 14] ^ w[i - 16]).rotate_left(1);
        }
        let [mut a, mut b, mut c, mut d, mut e] = h;
        for (i, &word) in w.iter().enumerate() {
            let (f, k) = match i {
                0..=19 => ((b & c) | (!b & d), 0x5A82_7999),
                20..=39 => (b ^ c ^ d, 0x6ED9_EBA1),
                40..=59 => ((b & c) | (b & d) | (c & d), 0x8F1B_BCDC),
                _ => (b ^ c ^ d, 0xCA62_C1D6),
            };
            let temp = a
                .rotate_left(5)
                .wrapping_add(f)
                .wrapping_add(e)
                .wrapping_add(k)
                .wrapping_add(word);
            e = d;
            d = c;
            c = b.rotate_left(30);
            b = a;
            a = temp;
        }
        for (slot, value) in h.iter_mut().zip([a, b, c, d, e]) {
            *slot = slot.wrapping_add(value);
        }
    }
    h.iter().map(|word| format!("{word:08x}")).collect()
}

/// A UUID-shaped string from the first 16 bytes of the text's SHA-1, with version 5 and the
/// RFC 4122 variant set; no namespace is mixed in, as in eyeron
fn uuid_of(text: &str) -> String {
    let hex = sha1_hex(text.as_bytes());
    let mut bytes = [0u8; 16];
    for (i, byte) in bytes.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&hex[2 * i..2 * i + 2], 16).unwrap_or(0);
    }
    bytes[6] = (bytes[6] & 0x0f) | 0x50;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    let h: String = bytes.iter().map(|b| format!("{b:02x}")).collect();
    format!(
        "{}-{}-{}-{}-{}",
        &h[0..8],
        &h[8..12],
        &h[12..16],
        &h[16..20],
        &h[20..32]
    )
}

#[cfg(test)]
mod tests {
    use super::sha1_hex;

    #[test]
    fn sha1_matches_the_published_test_vectors() {
        assert_eq!(sha1_hex(b""), "da39a3ee5e6b4b0d3255bfef95601890afd80709");
        assert_eq!(sha1_hex(b"abc"), "a9993e364706816aba3e25717850c26c9cd0d89d");
        assert_eq!(
            sha1_hex(b"abcdbcdecdefdefgefghfghighijhijkijkljklmklmnlmnomnopnopq"),
            "84983e441c3bd26ebaae4aa1f95129e5e54670f1"
        );
    }
}
