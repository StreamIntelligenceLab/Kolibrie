/*
 * Copyright © 2026 Volodymyr Kadzhaia
 * Copyright © 2026 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

use super::error::RdfMessageError;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum VersionLabel {
    V1_1,
    V1_2,
    V1_2Basic,
    V1_1Messages,
    V1_2Messages,
    V1_2BasicMessages,
}

impl VersionLabel {
    /// Canonical string form, without quotes
    pub fn as_str(&self) -> &'static str {
        match self {
            VersionLabel::V1_1 => "1.1",
            VersionLabel::V1_2 => "1.2",
            VersionLabel::V1_2Basic => "1.2-basic",
            VersionLabel::V1_1Messages => "1.1-messages",
            VersionLabel::V1_2Messages => "1.2-messages",
            VersionLabel::V1_2BasicMessages => "1.2-basic-messages",
        }
    }

    /// Whether this label enables the `MESSAGE` syntax
    pub fn supports_messages(&self) -> bool {
        matches!(
            self,
            VersionLabel::V1_1Messages
                | VersionLabel::V1_2Messages
                | VersionLabel::V1_2BasicMessages
        )
    }

    /// Parse a label, surrounding quotes are tolerated
    pub fn parse(label: &str) -> Result<Self, RdfMessageError> {
        let trimmed = label
            .trim()
            .trim_matches('"')
            .trim_matches('\'')
            .trim();
        match trimmed {
            "1.1" => Ok(VersionLabel::V1_1),
            "1.2" => Ok(VersionLabel::V1_2),
            "1.2-basic" => Ok(VersionLabel::V1_2Basic),
            "1.1-messages" => Ok(VersionLabel::V1_1Messages),
            "1.2-messages" => Ok(VersionLabel::V1_2Messages),
            "1.2-basic-messages" => Ok(VersionLabel::V1_2BasicMessages),
            other => Err(RdfMessageError::UnknownVersionLabel(other.to_string())),
        }
    }
}

impl std::fmt::Display for VersionLabel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_all_labels_with_and_without_quotes() {
        assert_eq!(VersionLabel::parse("1.2-messages"), Ok(VersionLabel::V1_2Messages));
        assert_eq!(VersionLabel::parse("\"1.2-messages\""), Ok(VersionLabel::V1_2Messages));
        assert_eq!(VersionLabel::parse("  '1.1'  "), Ok(VersionLabel::V1_1));
    }

    #[test]
    fn message_suffix_detected() {
        assert!(VersionLabel::V1_2Messages.supports_messages());
        assert!(VersionLabel::V1_2BasicMessages.supports_messages());
        assert!(VersionLabel::V1_1Messages.supports_messages());
        assert!(!VersionLabel::V1_2.supports_messages());
        assert!(!VersionLabel::V1_2Basic.supports_messages());
        assert!(!VersionLabel::V1_1.supports_messages());
    }

    #[test]
    fn unknown_label_is_error() {
        assert!(matches!(
            VersionLabel::parse("9.9-turbo"),
            Err(RdfMessageError::UnknownVersionLabel(_))
        ));
    }
}
