//! Newtype wrappers for PostgreSQL types that don't map to native Rust types.

/// PostgreSQL `numeric` / `decimal` type, stored as its string representation.
/// Use this when you need exact decimal values without adding a decimal crate.
///
/// The unchecked constructors (`From<String>`, `From<&str>`) accept any string.
/// Use `TryFrom` for client-side validation:
///
/// ```
/// use resolute::PgNumeric;
/// let valid = PgNumeric::new_unchecked("123.45");  // unchecked
/// let checked = PgNumeric::try_from("123.45");  // validated
/// assert!(checked.is_ok());
/// assert!(PgNumeric::try_from("not a number").is_err());
/// ```
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

impl PgNumeric {
    /// Create from a string without validation.
    /// Prefer `TryFrom<&str>` for validated construction.
    pub fn new_unchecked(s: &str) -> Self { Self(s.to_string()) }
}

impl TryFrom<&str> for PgNumeric {
    type Error = String;
    /// Validate that the string is a valid PostgreSQL numeric literal.
    /// Accepts: optional sign, digits, optional decimal point, optional digits,
    /// optional exponent (e/E followed by optional sign and digits),
    /// and the special values `NaN`, `Infinity`, `-Infinity`.
    fn try_from(s: &str) -> Result<Self, Self::Error> {
        let s = s.trim();
        if s.is_empty() {
            return Err("empty string is not a valid numeric".into());
        }
        // Special values.
        if s == "NaN" || s == "Infinity" || s == "-Infinity" {
            return Ok(Self(s.to_string()));
        }
        // Validate numeric format: [+-]?[0-9]*\.?[0-9]*([eE][+-]?[0-9]+)?
        let bytes = s.as_bytes();
        let mut i = 0;
        // Optional sign.
        if i < bytes.len() && (bytes[i] == b'+' || bytes[i] == b'-') {
            i += 1;
        }
        let start = i;
        // Integer part.
        while i < bytes.len() && bytes[i].is_ascii_digit() {
            i += 1;
        }
        // Decimal point + fractional part.
        if i < bytes.len() && bytes[i] == b'.' {
            i += 1;
            while i < bytes.len() && bytes[i].is_ascii_digit() {
                i += 1;
            }
        }
        // Must have at least one digit.
        if i == start || (i == start + 1 && bytes[start] == b'.') {
            return Err(format!("invalid numeric: {s:?}"));
        }
        // Optional exponent.
        if i < bytes.len() && (bytes[i] == b'e' || bytes[i] == b'E') {
            i += 1;
            if i < bytes.len() && (bytes[i] == b'+' || bytes[i] == b'-') {
                i += 1;
            }
            let exp_start = i;
            while i < bytes.len() && bytes[i].is_ascii_digit() {
                i += 1;
            }
            if i == exp_start {
                return Err(format!("invalid numeric exponent: {s:?}"));
            }
        }
        if i != bytes.len() {
            return Err(format!("invalid numeric: unexpected character at position {i} in {s:?}"));
        }
        Ok(Self(s.to_string()))
    }
}

/// PostgreSQL `inet` / `cidr` type, stored as its string representation (e.g. "192.168.1.1/24").
///
/// The unchecked constructors (`From<String>`, `From<&str>`) accept any string.
/// Use `TryFrom` for client-side validation:
///
/// ```
/// use resolute::PgInet;
/// let valid = PgInet::new_unchecked("192.168.1.1/24");  // unchecked
/// let checked = PgInet::try_from("192.168.1.1/24");  // validated
/// assert!(checked.is_ok());
/// assert!(PgInet::try_from("not an address").is_err());
/// ```
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

impl PgInet {
    /// Create from a string without validation.
    /// Prefer `TryFrom<&str>` for validated construction.
    pub fn new_unchecked(s: &str) -> Self { Self(s.to_string()) }
}

impl TryFrom<&str> for PgInet {
    type Error = String;
    /// Validate that the string looks like a valid IPv4 or IPv6 address,
    /// optionally followed by a CIDR prefix length (/N).
    fn try_from(s: &str) -> Result<Self, Self::Error> {
        let s = s.trim();
        if s.is_empty() {
            return Err("empty string is not a valid inet address".into());
        }
        // Split off optional CIDR prefix.
        let (addr_part, prefix_part) = if let Some((addr, prefix)) = s.rsplit_once('/') {
            let prefix_len: u8 = prefix.parse().map_err(|_| {
                format!("invalid CIDR prefix length: {prefix:?}")
            })?;
            if prefix_len > 128 {
                return Err(format!("CIDR prefix length {prefix_len} exceeds maximum (128)"));
            }
            (addr, Some(prefix_len))
        } else {
            (s, None)
        };
        // Try parsing as IPv4 or IPv6.
        if addr_part.parse::<std::net::IpAddr>().is_err() {
            return Err(format!("invalid IP address: {addr_part:?}"));
        }
        // Validate prefix range for IPv4.
        if let Some(prefix_len) = prefix_part {
            if addr_part.contains('.') && prefix_len > 32 {
                return Err(format!("IPv4 CIDR prefix length {prefix_len} exceeds maximum (32)"));
            }
        }
        Ok(Self(s.to_string()))
    }
}

/// PostgreSQL timestamp that can represent `infinity` and `-infinity`.
/// Use this instead of `chrono::NaiveDateTime` when your column may contain infinity.
///
/// The infinity sentinels match PostgreSQL's wire protocol exactly:
/// `i64::MAX` = `DT_NOEND` (infinity) and `i64::MIN` = `DT_NOBEGIN` (-infinity)
/// as defined in PostgreSQL source `src/include/datatype/timestamp.h`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum PgTimestamp {
    /// A finite timestamp value (microseconds since PG epoch 2000-01-01).
    Value(i64),
    /// PostgreSQL `'infinity'` — wire value `i64::MAX` (`DT_NOEND` in PG source).
    Infinity,
    /// PostgreSQL `'-infinity'` — wire value `i64::MIN` (`DT_NOBEGIN` in PG source).
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
///
/// The infinity sentinels match PostgreSQL's wire protocol exactly:
/// `i32::MAX` = `DATEVAL_NOEND` (infinity) and `i32::MIN` = `DATEVAL_NOBEGIN` (-infinity)
/// as defined in PostgreSQL source `src/include/datatype/timestamp.h`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum PgDate {
    /// A finite date value (days since PG epoch 2000-01-01).
    Value(i32),
    /// PostgreSQL `'infinity'` — wire value `i32::MAX` (`DATEVAL_NOEND` in PG source).
    Infinity,
    /// PostgreSQL `'-infinity'` — wire value `i32::MIN` (`DATEVAL_NOBEGIN` in PG source).
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pg_numeric_valid() {
        assert!(PgNumeric::try_from("0").is_ok());
        assert!(PgNumeric::try_from("123").is_ok());
        assert!(PgNumeric::try_from("-456").is_ok());
        assert!(PgNumeric::try_from("3.14").is_ok());
        assert!(PgNumeric::try_from("-0.001").is_ok());
        assert!(PgNumeric::try_from(".5").is_ok());
        assert!(PgNumeric::try_from("1e10").is_ok());
        assert!(PgNumeric::try_from("2.5E-3").is_ok());
        assert!(PgNumeric::try_from("+42").is_ok());
        assert!(PgNumeric::try_from("NaN").is_ok());
        assert!(PgNumeric::try_from("Infinity").is_ok());
        assert!(PgNumeric::try_from("-Infinity").is_ok());
    }

    #[test]
    fn test_pg_numeric_invalid() {
        assert!(PgNumeric::try_from("").is_err());
        assert!(PgNumeric::try_from("abc").is_err());
        assert!(PgNumeric::try_from("12.34.56").is_err());
        assert!(PgNumeric::try_from("1e").is_err());
        assert!(PgNumeric::try_from(".").is_err());
        assert!(PgNumeric::try_from("- 1").is_err());
    }

    #[test]
    fn test_pg_inet_valid_ipv4() {
        assert!(PgInet::try_from("192.168.1.1").is_ok());
        assert!(PgInet::try_from("10.0.0.0/8").is_ok());
        assert!(PgInet::try_from("0.0.0.0/0").is_ok());
        assert!(PgInet::try_from("255.255.255.255/32").is_ok());
    }

    #[test]
    fn test_pg_inet_valid_ipv6() {
        assert!(PgInet::try_from("::1").is_ok());
        assert!(PgInet::try_from("fe80::1/64").is_ok());
        assert!(PgInet::try_from("2001:db8::1/128").is_ok());
    }

    #[test]
    fn test_pg_inet_invalid() {
        assert!(PgInet::try_from("").is_err());
        assert!(PgInet::try_from("not an address").is_err());
        assert!(PgInet::try_from("192.168.1.1/33").is_err()); // IPv4 max prefix is 32
        assert!(PgInet::try_from("::1/129").is_err()); // max prefix is 128
        assert!(PgInet::try_from("192.168.1.1/abc").is_err());
    }
}
