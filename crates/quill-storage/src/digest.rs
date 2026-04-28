use std::fmt;
use std::str::FromStr;

use thiserror::Error;

#[derive(Debug, Error)]
pub enum DigestError {
    #[error("missing algorithm separator (expected algo:hex)")]
    MissingSeparator,
    #[error("unsupported algorithm: {0}")]
    UnsupportedAlgorithm(String),
    #[error("invalid hex length: expected {expected}, got {got}")]
    InvalidHexLength { expected: usize, got: usize },
    #[error("invalid hex character")]
    InvalidHex,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Digest {
    algorithm: String,
    hex: String,
}

impl Digest {
    pub fn parse(s: &str) -> Result<Self, DigestError> {
        let (algo, hex) = s.split_once(':').ok_or(DigestError::MissingSeparator)?;
        if algo != "sha256" {
            return Err(DigestError::UnsupportedAlgorithm(algo.to_string()));
        }
        if hex.len() != 64 {
            return Err(DigestError::InvalidHexLength {
                expected: 64,
                got: hex.len(),
            });
        }
        if !hex.chars().all(|c| c.is_ascii_hexdigit()) {
            return Err(DigestError::InvalidHex);
        }
        Ok(Self {
            algorithm: algo.to_string(),
            hex: hex.to_lowercase(),
        })
    }

    pub fn algorithm(&self) -> &str {
        &self.algorithm
    }

    pub fn hex(&self) -> &str {
        &self.hex
    }
}

impl fmt::Display for Digest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.algorithm, self.hex)
    }
}

impl FromStr for Digest {
    type Err = DigestError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Digest::parse(s)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_valid_digest() {
        let d =
            Digest::parse("sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855")
                .unwrap();
        assert_eq!(d.algorithm(), "sha256");
        assert_eq!(d.hex().len(), 64);
    }

    #[test]
    fn rejects_unknown_algorithm() {
        assert!(matches!(
            Digest::parse("md5:abc"),
            Err(DigestError::UnsupportedAlgorithm(_))
        ));
    }

    #[test]
    fn rejects_short_hex() {
        assert!(matches!(
            Digest::parse("sha256:abc"),
            Err(DigestError::InvalidHexLength { .. })
        ));
    }
}
