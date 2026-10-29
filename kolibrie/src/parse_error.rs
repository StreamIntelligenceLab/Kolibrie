/*
 * Copyright © 2026 Volodymyr Kadzhaia
 * Copyright © 2026 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! What a failed parse says, without saying it in `nom`'s words.

/// What the parser was looking for where it gave up.
///
/// The crate's own vocabulary rather than the parser library's error kinds. Those
/// kinds name combinators -- `Tag`, `Alt`, `TakeWhile1`, `Many0` -- which is an
/// implementation detail of how the grammar happens to be written, and leaking them
/// would tie a caller's `match` to that. These say what a reader of the query needs
/// to know.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Expected {
    /// A particular keyword or token.
    Token,
    /// A particular character.
    Character,
    /// One of several alternatives.
    Alternative,
    /// At least one character of something, where none were found.
    AtLeastOne,
    /// A repeated element, where the repetition could not proceed.
    Repetition,
    /// The end of the input, with something still left over.
    EndOfInput,
    /// Something the parser does not describe more precisely.
    Other,
}

/// Why a parse failed, and where in the input it gave up.
///
/// # Why this exists
///
/// The parser is written with `nom`, and its entry points used to return `nom`'s
/// `IResult`, which makes `nom::Err<nom::error::Error<&str>>` part of this crate's
/// public API. §4's rule is that nothing leaks: a caller should not have to name a
/// third-party type to use this crate, and this crate should be able to change parser
/// library without a breaking release.
///
/// It was not hypothetical. One test reached into the error to find which lexeme was
/// rejected, and had to match `nom::Err::Error | nom::Err::Failure` against
/// `nom::Err::Incomplete` to do it. That is this crate's business, not its callers'.
/// [`offending`](Self::offending) answers the question directly.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParseError<'a> {
    offending: &'a str,
    incomplete: bool,
    expected: Expected,
}

impl<'a> ParseError<'a> {
    /// The input remaining at the point the parse gave up.
    ///
    /// Empty when the parser ran out of input rather than rejecting something, which
    /// [`is_incomplete`](Self::is_incomplete) distinguishes.
    pub fn offending(&self) -> &'a str {
        self.offending
    }

    /// Whether the parse failed because the input ended, rather than because
    /// something in it was rejected.
    pub fn is_incomplete(&self) -> bool {
        self.incomplete
    }

    /// What the parser was looking for at that point.
    pub fn expected(&self) -> Expected {
        self.expected
    }

    /// Where the failure is, as a byte offset into the whole input.
    ///
    /// The parser reports the text it rejected rather than how far it had come, so
    /// the offset needs the original input to recover.
    ///
    /// Computed from where the rejected text sits inside `input`, not by subtracting
    /// its length. Subtracting assumes the parser handed back the whole unconsumed
    /// *suffix*, and it does not always: for `DELETE DATA { <urn:s> <urn:p> ?illegal }`
    /// it narrows the error to `?illegal`, which starts at byte 30 while the
    /// subtraction says 32. The error messages built on that were quietly off by the
    /// width of whatever followed.
    ///
    /// Returns 0 if `input` is not the text this error came from.
    pub fn offset_in(&self, input: &str) -> usize {
        let base = input.as_ptr() as usize;
        let rejected = self.offending.as_ptr() as usize;
        rejected
            .checked_sub(base)
            .filter(|offset| *offset <= input.len())
            .unwrap_or(0)
    }

    /// Built from the parser library's own error.
    ///
    /// `pub(crate)` on purpose: this is the one place that knows `nom`, and keeping
    /// the conversion private is what keeps the leak closed.
    pub(crate) fn from_nom(error: nom::Err<nom::error::Error<&'a str>>) -> Self {
        use nom::error::ErrorKind;
        match error {
            nom::Err::Error(error) | nom::Err::Failure(error) => Self {
                offending: error.input,
                incomplete: false,
                expected: match error.code {
                    ErrorKind::Tag => Expected::Token,
                    ErrorKind::Char => Expected::Character,
                    ErrorKind::Alt => Expected::Alternative,
                    ErrorKind::TakeWhile1 => Expected::AtLeastOne,
                    ErrorKind::Many0 | ErrorKind::Many1 => Expected::Repetition,
                    ErrorKind::Eof => Expected::EndOfInput,
                    _ => Expected::Other,
                },
            },
            nom::Err::Incomplete(_) => Self {
                offending: "",
                incomplete: true,
                expected: Expected::EndOfInput,
            },
        }
    }
}

impl std::fmt::Display for ParseError<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.incomplete {
            return write!(f, "the input ended before the parse was complete");
        }
        let offending = self.offending.trim();
        let shown: String = offending.chars().take(40).collect();
        if shown.is_empty() {
            write!(f, "the input could not be parsed")
        } else if shown.len() < offending.len() {
            write!(f, "could not parse from `{shown}`...")
        } else {
            write!(f, "could not parse from `{shown}`")
        }
    }
}

impl std::error::Error for ParseError<'_> {}

/// What a parser entry point returns: the unconsumed input and the value, or why not.
///
/// The remaining input is part of the success value because these are parsers, and a
/// caller that wants the whole input consumed has to check that it was.
pub type ParseResult<'a, T> = Result<(&'a str, T), ParseError<'a>>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_message_quotes_the_offending_input_and_stays_bounded() {
        let long = "x".repeat(200);
        let error = ParseError {
            offending: &long,
            incomplete: false,
            expected: Expected::Token,
        };
        let shown = error.to_string();
        assert!(shown.contains("could not parse from"));
        assert!(shown.ends_with("..."), "a long lexeme is truncated: {shown}");
        assert!(
            shown.len() < 100,
            "the message does not carry the whole input"
        );
    }

    #[test]
    fn an_incomplete_parse_says_so_rather_than_quoting_nothing() {
        let error = ParseError {
            offending: "",
            incomplete: true,
            expected: Expected::EndOfInput,
        };
        assert!(error.is_incomplete());
        assert_eq!(error.offending(), "");
        assert!(error.to_string().contains("ended before"));
    }

    #[test]
    fn an_offset_against_unrelated_input_is_not_invented() {
        // `offending` is a slice of the input it came from; given any other text
        // there is no meaningful answer, and 0 is the documented one.
        let error = ParseError {
            offending: "somewhere",
            incomplete: false,
            expected: Expected::Token,
        };
        assert_eq!(error.offset_in("unrelated text entirely"), 0);
    }
}
