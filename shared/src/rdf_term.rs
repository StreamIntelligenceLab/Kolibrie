/*
 * Copyright © 2026 Volodymyr Kadzhaia
 * Copyright © 2026 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! What a term's text means, and how a term is spelled so that two terms are the
//! same string exactly when RDF says they are the same term.
//!
//! The [`Dictionary`](crate::dictionary::Dictionary) keys on a term's text, so the
//! spelling *is* the identity: two terms sharing a spelling share an id and can never
//! be told apart again. That makes the choice of spelling a correctness question
//! rather than a formatting one, and it had been getting it wrong — the N-Triples
//! reader discarded a literal's datatype, so `"25"^^xsd:integer`, `"25"^^xsd:string`
//! and plain `"25"` all interned as `25`, with the datatype unrecoverable.
//!
//! # The canonical spelling
//!
//! | term | spelled |
//! |---|---|
//! | IRI | bare: `http://example/x` |
//! | plain literal | bare: `25` |
//! | `xsd:string` literal | bare: `25` — RDF 1.1 §3.3 says it *is* the plain literal |
//! | typed literal | `"25"^^<http://www.w3.org/2001/XMLSchema#integer>` |
//! | language literal | `"hello"@en` |
//! | blank node | `_:b` |
//!
//! Bare where it can be, quoted where it must be. An IRI and a plain literal keep the
//! spelling they always had, which is why this change leaves the overwhelming
//! majority of terms, and the results built from them, byte-identical. A typed or
//! language-tagged literal is quoted, which is what makes it distinguishable: a
//! well-formed IRI cannot begin with `"`, so no quoted form can collide with one.
//!
//! The quoted form is also the spelling [`tag_store`](crate::tag_store) and
//! [`hybrid`](crate::hybrid) already intern directly, so the reader and those
//! writers now agree. They did not before: a probability written as
//! `"0.8"^^<…#double>` could never match a `0.8` read from a file, because the two
//! paths minted different ids for one term.
//!
//! # Not value equality
//!
//! `"1"^^xsd:integer` and `"01"^^xsd:integer` denote one value and remain two terms.
//! Comparing values is a separate operation on top of these, not a spelling rule.

/// The three kinds of RDF term, as told from a canonical spelling.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TermKind {
    /// An IRI, spelled bare.
    Iri,
    /// A blank node label, spelled `_:label`.
    BlankNode,
    /// A literal, plain or quoted.
    Literal,
}

/// A literal taken apart: what it says, and what says how to read it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Literal {
    /// The lexical form, unescaped.
    pub lexical: String,
    /// The datatype IRI, absent for a plain or language-tagged literal.
    pub datatype: Option<String>,
    /// The language tag, absent unless the literal carries one.
    pub language: Option<String>,
}

/// The IRI of `xsd:string`, the datatype a plain literal already has.
pub const XSD_STRING: &str = "http://www.w3.org/2001/XMLSchema#string";

/// Escapes a lexical form for the inside of a quoted literal.
///
/// The inverse of the reader's unescaping. Without it a lexical form containing a
/// quote or a backslash would produce a spelling that reads back as a different
/// term — the one way a canonical form can be worse than no canonical form.
pub fn escape_lexical(lexical: &str) -> String {
    let mut out = String::with_capacity(lexical.len());
    for character in lexical.chars() {
        match character {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            '\u{0008}' => out.push_str("\\b"),
            '\u{000c}' => out.push_str("\\f"),
            _ => out.push(character),
        }
    }
    out
}

/// Spells a literal canonically: bare when it can be, quoted when it must be.
///
/// A plain literal and an `xsd:string` literal are one term and get one spelling.
/// Everything else is quoted, carrying whichever of the datatype or language tag it
/// has, so that the spelling determines the term.
pub fn spell_literal(lexical: &str, datatype: Option<&str>, language: Option<&str>) -> String {
    match (datatype, language) {
        (_, Some(tag)) => format!("\"{}\"@{}", escape_lexical(lexical), tag),
        (Some(iri), None) if iri != XSD_STRING => {
            format!("\"{}\"^^<{}>", escape_lexical(lexical), iri)
        }
        // Plain, or explicitly xsd:string, which RDF 1.1 says is the same term.
        _ => lexical.to_string(),
    }
}

/// Which kind of term this spelling denotes.
///
/// Reads the spelling rather than consulting a second map, so there is no parallel
/// table to keep in step with the dictionary. The three forms are disjoint for
/// well-formed RDF: an IRI has a scheme, so it can begin with neither `"` nor `_:`.
pub fn kind_of(spelling: &str) -> TermKind {
    if spelling.starts_with('"') {
        TermKind::Literal
    } else if spelling.starts_with("_:") {
        TermKind::BlankNode
    } else if looks_like_iri(spelling) {
        TermKind::Iri
    } else {
        // A bare spelling that is not an IRI is a plain literal: `25`, `hello`.
        TermKind::Literal
    }
}

/// Whether a bare spelling looks like an IRI rather than a plain literal.
///
/// An absolute IRI has a scheme followed by `:`, and the scheme is letters, digits,
/// `+`, `-` or `.` starting with a letter (RFC 3987). `urn:a` and `http://x` qualify;
/// `25` and `hello world` do not.
fn looks_like_iri(spelling: &str) -> bool {
    let Some(colon) = spelling.find(':') else {
        return false;
    };
    let scheme = &spelling[..colon];
    !scheme.is_empty()
        && scheme.starts_with(|c: char| c.is_ascii_alphabetic())
        && scheme
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '+' || c == '-' || c == '.')
}

/// Rewrites an N-Triples literal into its canonical spelling.
///
/// The one place a literal's text becomes a term, so that a reader and an encoder
/// cannot disagree about what was read. They did: the N-Triples reader spelled the
/// term one way and the encoder then re-parsed it and kept only the lexical form,
/// dropping the datatype a second time in a second function. One function, used by
/// both, is what stops that recurring.
///
/// Idempotent, which is what lets it sit on a path that may already have run: a
/// canonical spelling reads back as the literal it was spelled from, and spells to
/// itself. Returns `None` for text that is not a literal, so a caller can pass an
/// IRI through unchanged.
pub fn canonicalise_literal(text: &str) -> Option<String> {
    let literal = read_literal(text)?;
    Some(spell_literal(
        &literal.lexical,
        literal.datatype.as_deref(),
        literal.language.as_deref(),
    ))
}

/// The number a term denotes, or `None` if it denotes none.
///
/// Reads the lexical form before parsing, which is the whole point: a literal's
/// number lives in its lexical form, not in its spelling. `"25"^^<…#integer>` is
/// twenty-five, and parsing the spelling itself would find nothing — the failure
/// mode that would otherwise arrive with the datatype, silently turning `ORDER BY`
/// into string ordering and `SUM` into no answer at all.
///
/// Says nothing about whether the datatype is numeric: a term is read as a number
/// when its lexical form is one, which is what the engine did before and keeps
/// `"25"` working whether or not anyone typed it.
pub fn numeric_value(spelling: &str) -> Option<f64> {
    read_literal(spelling)?.lexical.trim().parse::<f64>().ok()
}

/// The text a term denotes: a literal's lexical form, or the term itself.
///
/// What to compare, print or hand to a consumer that wants the value rather than the
/// term. An IRI and a blank node are their own text; a literal sheds its quotes,
/// datatype and language tag.
pub fn lexical_value(spelling: &str) -> &str {
    if !spelling.starts_with('"') {
        return spelling;
    }
    match unescape_quoted(spelling) {
        // Borrowing back from the input is only sound when unescaping changed
        // nothing, so an escaped form falls back to the whole spelling rather than
        // allocating behind the caller's back. Callers needing the unescaped text
        // use `read_literal`.
        Some((value, rest)) if value.len() + rest.len() + 2 == spelling.len() => {
            &spelling[1..1 + value.len()]
        }
        _ => spelling,
    }
}

/// Takes a canonical spelling apart into a literal, or `None` if it denotes an IRI or
/// a blank node.
///
/// The inverse of [`spell_literal`]: a plain spelling comes back as a lexical form
/// with no datatype, and a quoted one carries whichever it was spelled with.
pub fn read_literal(spelling: &str) -> Option<Literal> {
    if !spelling.starts_with('"') {
        return match kind_of(spelling) {
            TermKind::Literal => Some(Literal {
                lexical: spelling.to_string(),
                datatype: None,
                language: None,
            }),
            _ => None,
        };
    }

    let (lexical, rest) = unescape_quoted(spelling)?;
    if let Some(tag) = rest.strip_prefix('@') {
        return Some(Literal {
            lexical,
            datatype: None,
            language: Some(tag.to_string()),
        });
    }
    if let Some(datatype) = rest.strip_prefix("^^") {
        let datatype = datatype
            .strip_prefix('<')
            .and_then(|iri| iri.strip_suffix('>'))
            .unwrap_or(datatype);
        return Some(Literal {
            lexical,
            datatype: Some(datatype.to_string()),
            language: None,
        });
    }
    Some(Literal {
        lexical,
        datatype: None,
        language: None,
    })
}

/// Reads the quoted part of a literal, returning its unescaped text and what follows
/// the closing quote.
fn unescape_quoted(spelling: &str) -> Option<(String, &str)> {
    let body = spelling.strip_prefix('"')?;
    let mut characters = body.char_indices();
    let mut value = String::new();
    while let Some((offset, character)) = characters.next() {
        match character {
            '"' => return Some((value, &body[offset + character.len_utf8()..])),
            '\\' => {
                let (_, escaped) = characters.next()?;
                match escaped {
                    'n' => value.push('\n'),
                    'r' => value.push('\r'),
                    't' => value.push('\t'),
                    'b' => value.push('\u{0008}'),
                    'f' => value.push('\u{000c}'),
                    '"' => value.push('"'),
                    '\'' => value.push('\''),
                    '\\' => value.push('\\'),
                    // `\uXXXX` and `\UXXXXXXXX`, as N-Triples spells a code point.
                    // An ill-formed escape makes the whole literal unreadable rather
                    // than silently becoming the letter `u`.
                    escape @ ('u' | 'U') => {
                        let digits = if escape == 'u' { 4 } else { 8 };
                        let mut scalar = String::with_capacity(digits);
                        for _ in 0..digits {
                            let (_, digit) = characters.next()?;
                            if !digit.is_ascii_hexdigit() {
                                return None;
                            }
                            scalar.push(digit);
                        }
                        value.push(char::from_u32(u32::from_str_radix(&scalar, 16).ok()?)?);
                    }
                    other => value.push(other),
                }
            }
            character => value.push(character),
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    const XSD_INTEGER: &str = "http://www.w3.org/2001/XMLSchema#integer";

    #[test]
    fn a_plain_literal_keeps_the_spelling_it_always_had() {
        // The property that keeps this change small: the common case does not move.
        assert_eq!(spell_literal("25", None, None), "25");
        assert_eq!(spell_literal("hello", None, None), "hello");
    }

    #[test]
    fn an_xsd_string_literal_is_the_plain_literal() {
        // RDF 1.1 §3.3. Spelling them alike is what makes them one term.
        assert_eq!(spell_literal("25", Some(XSD_STRING), None), "25");
        assert_eq!(
            spell_literal("25", Some(XSD_STRING), None),
            spell_literal("25", None, None)
        );
    }

    #[test]
    fn a_datatype_makes_a_different_spelling_than_the_lexical_form() {
        let typed = spell_literal("25", Some(XSD_INTEGER), None);
        assert_eq!(typed, format!("\"25\"^^<{XSD_INTEGER}>"));
        assert_ne!(typed, spell_literal("25", None, None));
        assert_ne!(typed, spell_literal("25", Some(XSD_STRING), None));
    }

    #[test]
    fn a_language_tag_cannot_collide_with_a_plain_literal_spelled_the_same_way() {
        // The collision that used to exist: `"hello"@en` was stored as `hello@en`,
        // which is also how a plain literal of that text was stored.
        assert_eq!(spell_literal("hello", None, Some("en")), "\"hello\"@en");
        assert_ne!(
            spell_literal("hello", None, Some("en")),
            spell_literal("hello@en", None, None)
        );
    }

    #[test]
    fn a_quote_in_the_lexical_form_survives_the_round_trip() {
        // Without escaping, this spelling would read back as a different term --
        // the one way a canonical form can be worse than none.
        let spelled = spell_literal("a\"b\\c", Some(XSD_INTEGER), None);
        let read = read_literal(&spelled).expect("a literal");
        assert_eq!(read.lexical, "a\"b\\c");
        assert_eq!(read.datatype.as_deref(), Some(XSD_INTEGER));
    }

    #[test]
    fn every_spelling_reads_back_as_what_it_was_spelled_from() {
        let cases = [
            ("25", None, None),
            ("25", Some(XSD_INTEGER), None),
            ("hello", None, Some("en")),
            ("with space", None, None),
            ("", Some(XSD_INTEGER), None),
        ];
        for (lexical, datatype, language) in cases {
            let spelled = spell_literal(lexical, datatype, language);
            let read = read_literal(&spelled)
                .unwrap_or_else(|| panic!("{spelled:?} should read back as a literal"));
            assert_eq!(read.lexical, lexical, "lexical form of {spelled:?}");
            assert_eq!(read.language.as_deref(), language, "language of {spelled:?}");
            // A plain and an xsd:string literal share a spelling, so the datatype
            // comes back absent for both; everything else round-trips exactly.
            let expected = datatype.filter(|iri| *iri != XSD_STRING);
            assert_eq!(read.datatype.as_deref(), expected, "datatype of {spelled:?}");
        }
    }

    #[test]
    fn a_spelling_says_which_kind_of_term_it_is() {
        assert_eq!(kind_of("http://example/x"), TermKind::Iri);
        assert_eq!(kind_of("urn:a"), TermKind::Iri);
        assert_eq!(kind_of("_:b"), TermKind::BlankNode);
        assert_eq!(kind_of("\"25\"^^<http://x>"), TermKind::Literal);
        assert_eq!(kind_of("\"hello\"@en"), TermKind::Literal);
        // Bare and not an IRI, so a plain literal rather than a malformed IRI.
        assert_eq!(kind_of("25"), TermKind::Literal);
        assert_eq!(kind_of("hello world"), TermKind::Literal);
    }

    #[test]
    fn canonicalising_twice_changes_nothing() {
        // The property the two call sites rely on: whichever of them runs first, the
        // other must not strip what it finds.
        for raw in [
            "\"25\"^^<http://www.w3.org/2001/XMLSchema#integer>",
            "\"hello\"@en",
            "\"25\"",
            "25",
        ] {
            let once = canonicalise_literal(raw).expect("a literal");
            let twice = canonicalise_literal(&once).expect("a literal");
            assert_eq!(once, twice, "canonicalising {raw:?} is not idempotent");
        }
    }

    #[test]
    fn canonicalising_keeps_what_the_reader_used_to_drop() {
        let typed = canonicalise_literal("\"25\"^^<http://www.w3.org/2001/XMLSchema#integer>")
            .expect("a literal");
        assert!(typed.contains(XSD_INTEGER), "datatype lost: {typed:?}");
        let tagged = canonicalise_literal("\"hello\"@en").expect("a literal");
        assert!(tagged.ends_with("@en"), "language tag lost: {tagged:?}");
    }

    #[test]
    fn a_typed_number_is_still_a_number() {
        // The regression this exists to prevent: the datatype arriving in the
        // spelling and taking the number with it.
        assert_eq!(numeric_value("25"), Some(25.0));
        assert_eq!(
            numeric_value(&spell_literal("25", Some(XSD_INTEGER), None)),
            Some(25.0)
        );
        assert_eq!(
            numeric_value(&spell_literal("-3.5", Some(XSD_INTEGER), None)),
            Some(-3.5)
        );
        assert_eq!(numeric_value("hello"), None);
        assert_eq!(numeric_value("http://example/25"), None, "an IRI is not a number");
    }

    #[test]
    fn the_text_a_term_denotes_sheds_only_the_literal_furniture() {
        assert_eq!(lexical_value("25"), "25");
        assert_eq!(lexical_value("http://example/x"), "http://example/x");
        assert_eq!(
            lexical_value(&spell_literal("25", Some(XSD_INTEGER), None)),
            "25"
        );
        assert_eq!(lexical_value(&spell_literal("hello", None, Some("en"))), "hello");
    }

    #[test]
    fn the_escapes_n_triples_defines_are_all_understood() {
        // Ported from the reader's own decoder, which these replaced. Dropping
        // `\\uXXXX`, `\\b` or `\\f` would have turned a valid literal into a
        // different one without any error.
        let cases = [
            ("\"a\\u0041b\"", "aAb"),
            ("\"a\\U00000041b\"", "aAb"),
            ("\"a\\bb\"", "a\u{0008}b"),
            ("\"a\\fb\"", "a\u{000c}b"),
            ("\"a\\tb\"", "a\tb"),
            ("\"a\\'b\"", "a'b"),
        ];
        for (raw, expected) in cases {
            let read = read_literal(raw).unwrap_or_else(|| panic!("{raw} should read"));
            assert_eq!(read.lexical, expected, "reading {raw}");
        }
    }

    #[test]
    fn an_ill_formed_unicode_escape_is_refused_not_guessed() {
        // `\\uZZZZ` is not the letter u followed by ZZZZ.
        assert_eq!(read_literal("\"a\\uZZZZb\""), None);
        assert_eq!(read_literal("\"a\\u00\""), None);
    }

    #[test]
    fn a_control_character_survives_the_round_trip() {
        for text in ["a\u{0008}b", "a\u{000c}b", "a\tb", "a\nb"] {
            let spelled = spell_literal(text, Some(XSD_INTEGER), None);
            let read = read_literal(&spelled).expect("a literal");
            assert_eq!(read.lexical, text, "round trip of {text:?} via {spelled:?}");
        }
    }

    #[test]
    fn an_iri_is_not_read_as_a_literal() {
        assert_eq!(read_literal("http://example/x"), None);
        assert_eq!(read_literal("_:b"), None);
    }
}
