//! Newtype wrappers for PostgreSQL types that don't map to native Rust types.

/// PostgreSQL `numeric` / `decimal` type, stored as its string representation.
/// Use this when you need exact decimal values without adding a decimal crate.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PgNumeric(pub String);

impl std::fmt::Display for PgNumeric {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl From<String> for PgNumeric {
    fn from(s: String) -> Self { Self(s) }
}
impl From<&str> for PgNumeric {
    fn from(s: &str) -> Self { Self(s.to_string()) }
}

/// PostgreSQL `inet` type, stored as its string representation (e.g. "192.168.1.1/24").
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PgInet(pub String);

impl std::fmt::Display for PgInet {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl From<String> for PgInet {
    fn from(s: String) -> Self { Self(s) }
}
impl From<&str> for PgInet {
    fn from(s: &str) -> Self { Self(s.to_string()) }
}

/// PostgreSQL timestamp that can represent `infinity` and `-infinity`.
/// Use this instead of `chrono::NaiveDateTime` when your column may contain infinity.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum PgTimestamp {
    /// A finite timestamp value (microseconds since PG epoch 2000-01-01).
    Value(i64),
    /// PostgreSQL `'infinity'`.
    Infinity,
    /// PostgreSQL `'-infinity'`.
    NegInfinity,
}

impl std::fmt::Display for PgTimestamp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Value(us) => write!(f, "{us}"),
            Self::Infinity => write!(f, "infinity"),
            Self::NegInfinity => write!(f, "-infinity"),
        }
    }
}

impl PgTimestamp {
    pub fn is_infinity(&self) -> bool {
        matches!(self, Self::Infinity | Self::NegInfinity)
    }

    pub fn is_finite(&self) -> bool {
        matches!(self, Self::Value(_))
    }
}

/// PostgreSQL date that can represent `infinity` and `-infinity`.
/// Use this instead of `chrono::NaiveDate` when your column may contain infinity.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum PgDate {
    /// A finite date value (days since PG epoch 2000-01-01).
    Value(i32),
    /// PostgreSQL `'infinity'`.
    Infinity,
    /// PostgreSQL `'-infinity'`.
    NegInfinity,
}

impl std::fmt::Display for PgDate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Value(d) => write!(f, "{d}"),
            Self::Infinity => write!(f, "infinity"),
            Self::NegInfinity => write!(f, "-infinity"),
        }
    }
}

impl PgDate {
    pub fn is_infinity(&self) -> bool {
        matches!(self, Self::Infinity | Self::NegInfinity)
    }

    pub fn is_finite(&self) -> bool {
        matches!(self, Self::Value(_))
    }
}
