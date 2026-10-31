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

/// The well-known path RDF 1.1 §3.5 gives Skolem IRIs.
///
/// Reserved by RFC 8615 for exactly this, so an IRI minted under it cannot collide
/// with one an application chose, and a consumer that recognises the path knows the
/// IRI stands for what was a blank node.
pub const SKOLEM_PATH: &str = "/.well-known/genid/";

/// A Skolem IRI for `label`, under an authority the caller controls.
///
/// `base` has to be supplied rather than defaulted, and that is the point: a Skolem
/// IRI is globally unique because the authority minting it is, and this crate has no
/// identity of its own to offer. Defaulting it would mint the same IRI on two
/// machines and reintroduce the collision skolemisation exists to remove.
pub fn skolem_iri(label: &str, base: &str) -> String {
    format!(
        "{}{SKOLEM_PATH}{}",
        base.trim_end_matches('/'),
        encode_label(label)
    )
}

/// Replaces a blank node with a Skolem IRI, leaving every other term untouched.
///
/// What to do at an exchange boundary. A blank node label means nothing outside its
/// document, so two systems merging each other's blank nodes fuse unrelated nodes;
/// RDF 1.1 §3.5 answers that by replacing them with IRIs, which are global. The
/// receiver may keep them as IRIs -- which is what makes the merge safe -- or turn
/// them back with [`deskolemise`].
pub fn skolemise<'a>(spelling: &'a str, base: &str) -> std::borrow::Cow<'a, str> {
    match spelling.strip_prefix("_:") {
        Some(label) => std::borrow::Cow::Owned(skolem_iri(label, base)),
        None => std::borrow::Cow::Borrowed(spelling),
    }
}

/// Recovers the blank node a Skolem IRI stands for, or `None` for any other term.
///
/// Optional by design: a consumer is entitled to leave a Skolem IRI as an IRI, and
/// doing so is what keeps a merge sound. This is for a consumer that would rather
/// have the blank node back, and recognises the IRI by its well-known path rather
/// than by its authority, so it works on IRIs any publisher minted.
pub fn deskolemise(spelling: &str) -> Option<String> {
    let (_, label) = spelling.split_once(SKOLEM_PATH)?;
    if label.is_empty() || label.contains('/') {
        return None;
    }
    Some(format!("_:{}", decode_label(label)))
}

/// Percent-encodes the characters that would end a label's IRI path segment early.
///
/// Deliberately narrow. A well-formed N-Triples blank node label is already a valid
/// IRI path segment -- letters, digits, `_`, `-`, `.` and Unicode are all permitted
/// in an IRI -- so nothing read from a document needs this. A label built by hand
/// through [`spell_blank_node`] can hold anything, and these are the characters that
/// would make the result a different IRI, or no IRI at all.
fn encode_label(label: &str) -> String {
    let mut out = String::with_capacity(label.len());
    for character in label.chars() {
        match character {
            '%' | '/' | '?' | '#' | '[' | ']' | '\\' => {
                out.push_str(&format!("%{:02X}", character as u32));
            }
            character if character.is_whitespace() || character.is_control() => {
                for byte in character.to_string().bytes() {
                    out.push_str(&format!("%{byte:02X}"));
                }
            }
            character => out.push(character),
        }
    }
    out
}

/// The inverse of [`encode_label`].
fn decode_label(label: &str) -> String {
    let mut bytes = Vec::with_capacity(label.len());
    let mut characters = label.chars();
    while let Some(character) = characters.next() {
        if character != '%' {
            let mut buffer = [0u8; 4];
            bytes.extend_from_slice(character.encode_utf8(&mut buffer).as_bytes());
            continue;
        }
        let high = characters.next().and_then(|c| c.to_digit(16));
        let low = characters.next().and_then(|c| c.to_digit(16));
        match (high, low) {
            (Some(high), Some(low)) => bytes.push((high * 16 + low) as u8),
            // Not an escape after all, so it stands for itself rather than vanishing.
            _ => bytes.push(b'%'),
        }
    }
    String::from_utf8_lossy(&bytes).into_owned()
}

/// Writes a term's canonical spelling as N-Triples term syntax.
///
/// The inverse of what the readers do, and the half of the term model that was
/// missing. A canonical spelling is deliberately *not* a wire form: an IRI is stored
/// bare and a plain literal unquoted, because that keeps identity cheap and leaves
/// existing terms untouched. Handing that text straight to a file is what produced
/// `http://example/x .` for an IRI and `"\"25\"^^<...>"` for a typed literal -- the
/// latter being a plain literal whose text happens to look like a typed one, which
/// any other RDF tool reads as a string.
///
/// It survived unnoticed because this crate's own readers re-canonicalise what they
/// read, so a round trip through Kolibrie recovered the datatype. Only a second
/// implementation would have noticed, which is the case that matters for exchanging
/// data between systems.
pub fn to_ntriples_term(spelling: &str) -> String {
    // A quoted triple is already written in its own bracketed form, and a blank node
    // label is already N-Triples syntax.
    if spelling.starts_with("<<") || spelling.starts_with("_:") {
        return spelling.to_string();
    }
    match kind_of(spelling) {
        TermKind::Iri => format!("<{spelling}>"),
        TermKind::BlankNode => spelling.to_string(),
        // A quoted spelling is already N-Triples literal syntax, datatype or language
        // tag included, so it is passed through rather than quoted a second time.
        TermKind::Literal if spelling.starts_with('"') => spelling.to_string(),
        TermKind::Literal => format!("\"{}\"", escape_lexical(spelling)),
    }
}

/// Spells a blank node belonging to one document, from the label as written.
///
/// The counterpart to [`spell_literal`], and the way to build a blank node without
/// knowing either half of the convention: that the term carries a `_:` prefix, and
/// that the label has to be scoped or it collides with every other document's.
/// `label` is the bare label, so `"b"` rather than `"_:b"`.
///
/// There is deliberately no equivalent for an IRI. An IRI is spelled as itself, so a
/// constructor would be the identity function and would only suggest that something
/// is being done to it.
pub fn spell_blank_node(label: &str, document: u64) -> String {
    scope_blank_node(&format!("_:{label}"), document).into_owned()
}

/// A fresh document scope, for relabelling the blank nodes of one document.
///
/// One per document read, taken before parsing begins so that parallel chunks of the
/// same document share it. Monotonic and never reused, which is what makes two reads
/// of the same file two sets of nodes -- as RDF requires, since a label identifies a
/// node only within the document it was read from.
pub fn next_document_scope() -> u64 {
    static NEXT_DOCUMENT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
    NEXT_DOCUMENT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
}

/// Rewrites a blank node label so that it belongs to one document, leaving any other
/// term untouched.
///
/// A blank node label means nothing outside its document (RDF 1.1 §3.4), so two files
/// each saying `_:b` describe two different nodes and merging them must keep them
/// apart. Relabelling is forced rather than chosen: the dictionary keys on a term's
/// text, so two distinct nodes must have two distinct spellings.
///
/// The written label stays legible at the end of the new one, and the form matches
/// what SPARQL updates already mint for the same reason, `_:kolibrie-update-N-label`.
///
/// Returns the term borrowed when nothing needed rewriting, so the ordinary case of a
/// document without blank nodes costs no allocation.
pub fn scope_blank_node<'a>(term: &'a str, document: u64) -> std::borrow::Cow<'a, str> {
    match term.strip_prefix("_:") {
        Some(label) => std::borrow::Cow::Owned(format!(
            "_:kolibrie-doc-{document}-{}",
            strip_scope(label)
        )),
        None => std::borrow::Cow::Borrowed(term),
    }
}

/// Removes a scope this crate added, leaving the label as it was written.
///
/// Scoping replaces a previous scope instead of nesting inside it, because reading a
/// graph back from its own serialisation is a new document and scopes it again: a
/// label would otherwise collect one prefix per round trip and grow without bound.
/// The node is still a new node each time, which is what RDF says and what the
/// replaced counter records; only the spelling stays a sane length.
///
/// A label a user genuinely wrote as `kolibrie-doc-5-x` is indistinguishable from one
/// this produced, and is treated as the latter.
fn strip_scope(label: &str) -> &str {
    for marker in ["kolibrie-doc-", "kolibrie-update-"] {
        let Some(rest) = label.strip_prefix(marker) else {
            continue;
        };
        let digits = rest
            .find(|c: char| !c.is_ascii_digit())
            .unwrap_or(rest.len());
        if digits > 0 {
            if let Some(stripped) = rest[digits..].strip_prefix('-') {
                return stripped;
            }
        }
    }
    label
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
    fn a_scoped_label_is_still_a_blank_node_and_still_legible() {
        let scoped = scope_blank_node("_:b", 7);
        assert_eq!(scoped, "_:kolibrie-doc-7-b");
        assert_eq!(kind_of(&scoped), TermKind::BlankNode);
        assert!(scoped.ends_with('b'), "the written label stays readable");
    }

    #[test]
    fn scoping_leaves_every_other_kind_of_term_alone() {
        for term in ["http://example/x", "25", "\"25\"^^<http://x>", "\"hello\"@en"] {
            assert_eq!(scope_blank_node(term, 7), term, "{term:?} is not a blank node");
            assert!(
                matches!(scope_blank_node(term, 7), std::borrow::Cow::Borrowed(_)),
                "{term:?} should not have been reallocated"
            );
        }
    }

    #[test]
    fn two_scopes_give_one_label_two_terms_and_one_scope_gives_it_one() {
        // Both halves of the property: labels separate across documents and stay
        // shared within one. Minting per occurrence would satisfy the first and
        // destroy every blank-node-linked structure in the graph.
        let (first, second) = (next_document_scope(), next_document_scope());
        assert_ne!(first, second, "each document gets its own scope");
        assert_ne!(
            scope_blank_node("_:b", first),
            scope_blank_node("_:b", second)
        );
        assert_eq!(
            scope_blank_node("_:b", first),
            scope_blank_node("_:b", first)
        );
    }

    #[test]
    fn scoping_replaces_a_previous_scope_rather_than_nesting_in_it() {
        // Reading a graph back from its own serialisation scopes it again, so
        // without this a label would collect one prefix per round trip.
        let once = scope_blank_node("_:b", 1).into_owned();
        let twice = scope_blank_node(&once, 2).into_owned();
        let thrice = scope_blank_node(&twice, 3).into_owned();
        assert_eq!(once, "_:kolibrie-doc-1-b");
        assert_eq!(twice, "_:kolibrie-doc-2-b");
        assert_eq!(thrice, "_:kolibrie-doc-3-b");
        // Still a new node each time, which is the point; only the spelling stays
        // a sane length.
        assert_ne!(once, twice);
        assert_ne!(twice, thrice);
    }

    #[test]
    fn scoping_also_replaces_the_scope_a_sparql_update_minted() {
        let from_update = "_:kolibrie-update-42-blank";
        assert_eq!(
            scope_blank_node(from_update, 7),
            "_:kolibrie-doc-7-blank",
            "an update's scope is replaced too, so a round trip does not nest it"
        );
    }

    #[test]
    fn a_label_that_merely_looks_scoped_is_left_as_written() {
        // No digits, so not a scope this crate wrote.
        assert_eq!(
            scope_blank_node("_:kolibrie-doc-x", 7),
            "_:kolibrie-doc-7-kolibrie-doc-x"
        );
    }

    #[test]
    fn a_blank_node_can_be_built_from_its_bare_label() {
        let document = next_document_scope();
        let built = spell_blank_node("b", document);
        assert_eq!(kind_of(&built), TermKind::BlankNode);
        // The same term the readers produce for `_:b` in that document, so a term
        // built by hand joins against one that was read.
        assert_eq!(built, scope_blank_node("_:b", document));
    }

    #[test]
    fn a_term_is_written_as_the_n_triples_syntax_for_its_kind() {
        assert_eq!(to_ntriples_term("http://example/x"), "<http://example/x>");
        assert_eq!(to_ntriples_term("urn:a"), "<urn:a>");
        assert_eq!(to_ntriples_term("_:kolibrie-doc-1-b"), "_:kolibrie-doc-1-b");
        assert_eq!(to_ntriples_term("25"), "\"25\"");
        assert_eq!(to_ntriples_term("hello world"), "\"hello world\"");
    }

    #[test]
    fn a_typed_literal_is_not_quoted_a_second_time() {
        // The bug this exists to stop: quoting the canonical spelling again turns a
        // typed literal into a plain one whose text looks typed, which this crate's
        // own readers then recover from and nobody else can.
        let typed = spell_literal("25", Some(XSD_INTEGER), None);
        assert_eq!(to_ntriples_term(&typed), format!("\"25\"^^<{XSD_INTEGER}>"));
        let tagged = spell_literal("hello", None, Some("en"));
        assert_eq!(to_ntriples_term(&tagged), "\"hello\"@en");
    }

    #[test]
    fn a_written_term_reads_back_as_the_term_it_was_written_from() {
        // Round trip through the wire syntax, which is the property an exchange
        // between two systems depends on.
        for spelling in [
            "http://example/x".to_string(),
            "urn:a".to_string(),
            "_:kolibrie-doc-1-b".to_string(),
            spell_literal("25", None, None),
            spell_literal("25", Some(XSD_INTEGER), None),
            spell_literal("hello", None, Some("en")),
            spell_literal("a\"b\\c", None, None),
        ] {
            let written = to_ntriples_term(&spelling);
            // What a reader does with that text: strip the IRI brackets, or
            // canonicalise the literal.
            let read_back = if let Some(iri) = written
                .strip_prefix('<')
                .and_then(|rest| rest.strip_suffix('>'))
            {
                iri.to_string()
            } else if written.starts_with('"') {
                canonicalise_literal(&written).expect("a literal")
            } else {
                written.clone()
            };
            assert_eq!(read_back, spelling, "{written:?} did not read back");
        }
    }

    #[test]
    fn a_blank_node_becomes_an_iri_under_the_well_known_path() {
        let skolem = skolemise("_:kolibrie-doc-7-b", "http://example.org");
        assert_eq!(
            skolem,
            "http://example.org/.well-known/genid/kolibrie-doc-7-b"
        );
        // An IRI, not a blank node, which is the whole point: it is global.
        assert_eq!(kind_of(&skolem), TermKind::Iri);
    }

    #[test]
    fn skolemising_leaves_every_other_kind_of_term_alone() {
        for term in ["http://example/x", "25", "\"25\"^^<http://x>", "\"hello\"@en"] {
            assert_eq!(skolemise(term, "http://example.org"), term);
            assert!(
                matches!(skolemise(term, "http://example.org"), std::borrow::Cow::Borrowed(_)),
                "{term:?} should not have been reallocated"
            );
        }
    }

    #[test]
    fn two_bases_give_one_label_two_iris() {
        // The property skolemisation exists for: two systems exchanging data no
        // longer collide, because the authority minting the IRI differs.
        assert_ne!(
            skolemise("_:b", "http://alice.example"),
            skolemise("_:b", "http://bob.example")
        );
    }

    #[test]
    fn a_trailing_slash_on_the_base_does_not_double_up() {
        assert_eq!(
            skolemise("_:b", "http://example.org/"),
            skolemise("_:b", "http://example.org")
        );
    }

    #[test]
    fn a_skolem_iri_gives_its_blank_node_back() {
        let original = "_:kolibrie-doc-7-b";
        let skolem = skolemise(original, "http://example.org").into_owned();
        assert_eq!(deskolemise(&skolem).as_deref(), Some(original));
        // Recognised by the well-known path, so an IRI any publisher minted works.
        assert_eq!(
            deskolemise("https://other.example/.well-known/genid/xyz").as_deref(),
            Some("_:xyz")
        );
    }

    #[test]
    fn an_ordinary_iri_is_not_mistaken_for_a_skolem_one() {
        assert_eq!(deskolemise("http://example.org/thing"), None);
        assert_eq!(deskolemise("_:b"), None, "a blank node is not a Skolem IRI");
        // A path continuing past the label is some other resource, not a node.
        assert_eq!(
            deskolemise("http://example.org/.well-known/genid/a/b"),
            None
        );
        assert_eq!(deskolemise("http://example.org/.well-known/genid/"), None);
    }

    #[test]
    fn an_awkward_label_survives_the_round_trip_as_a_single_iri_segment() {
        // A label built by hand can hold anything; these are the characters that
        // would otherwise end the path segment early or change the IRI.
        for label in ["a/b", "a?b", "a#b", "a b", "a%b", "100%", "héllo"] {
            let skolem = skolemise(&format!("_:{label}"), "http://example.org").into_owned();
            let segment = skolem
                .split_once(SKOLEM_PATH)
                .expect("minted under the well-known path")
                .1;
            assert!(
                !segment.contains('/'),
                "{skolem:?} must be one path segment, got segment {segment:?}"
            );
            assert_eq!(
                deskolemise(&skolem).as_deref(),
                Some(format!("_:{label}").as_str()),
                "{label:?} did not survive"
            );
        }
    }

    #[test]
    fn an_iri_is_not_read_as_a_literal() {
        assert_eq!(read_literal("http://example/x"), None);
        assert_eq!(read_literal("_:b"), None);
    }
}
