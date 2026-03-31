//! Decode PostgreSQL binary wire format into Rust types.
//!
//! Binary format is big-endian for all numeric types.
//! The wire protocol provides raw bytes (length already stripped by parser).

use crate::error::TypedError;

/// Trait for types that can be decoded from PostgreSQL binary format.
pub trait Decode: Sized {
    /// Decode from binary bytes. Returns an error if the bytes are malformed.
    fn decode(buf: &[u8]) -> Result<Self, TypedError>;

    /// Decode from a possibly-NULL column.
    fn decode_option(buf: Option<&[u8]>) -> Result<Option<Self>, TypedError> {
        match buf {
            Some(b) => Ok(Some(Self::decode(b)?)),
            None => Ok(None),
        }
    }
}

// ---------------------------------------------------------------------------
// Primitive implementations
// ---------------------------------------------------------------------------

impl Decode for bool {
    fn decode(buf: &[u8]) -> Result<Self, TypedError> {
        if buf.len() != 1 {
            return Err(TypedError::Decode {
                column: 0,
                message: format!("bool: expected 1 byte, got {}", buf.len()),
            });
        }
        Ok(buf[0] != 0)
    }
}

impl Decode for i16 {
    fn decode(buf: &[u8]) -> Result<Self, TypedError> {
        if buf.len() != 2 {
            return Err(TypedError::Decode {
                column: 0,
                message: format!("i16: expected 2 bytes, got {}", buf.len()),
            });
        }
        Ok(i16::from_be_bytes([buf[0], buf[1]]))
    }
}

impl Decode for i32 {
    fn decode(buf: &[u8]) -> Result<Self, TypedError> {
        if buf.len() != 4 {
            return Err(TypedError::Decode {
                column: 0,
                message: format!("i32: expected 4 bytes, got {}", buf.len()),
            });
        }
        Ok(i32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]))
    }
}

impl Decode for i64 {
    fn decode(buf: &[u8]) -> Result<Self, TypedError> {
        if buf.len() != 8 {
            return Err(TypedError::Decode {
                column: 0,
                message: format!("i64: expected 8 bytes, got {}", buf.len()),
            });
        }
        Ok(i64::from_be_bytes([
            buf[0], buf[1], buf[2], buf[3], buf[4], buf[5], buf[6], buf[7],
        ]))
    }
}

impl Decode for f32 {
    fn decode(buf: &[u8]) -> Result<Self, TypedError> {
        if buf.len() != 4 {
            return Err(TypedError::Decode {
                column: 0,
                message: format!("f32: expected 4 bytes, got {}", buf.len()),
            });
        }
        Ok(f32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]))
    }
}

impl Decode for f64 {
    fn decode(buf: &[u8]) -> Result<Self, TypedError> {
        if buf.len() != 8 {
            return Err(TypedError::Decode {
                column: 0,
                message: format!("f64: expected 8 bytes, got {}", buf.len()),
            });
        }
        Ok(f64::from_be_bytes([
            buf[0], buf[1], buf[2], buf[3], buf[4], buf[5], buf[6], buf[7],
        ]))
    }
}

impl Decode for String {
    fn decode(buf: &[u8]) -> Result<Self, TypedError> {
        String::from_utf8(buf.to_vec()).map_err(|e| TypedError::Decode {
            column: 0,
            message: format!("String: invalid UTF-8: {e}"),
        })
    }
}

impl Decode for Vec<u8> {
    fn decode(buf: &[u8]) -> Result<Self, TypedError> {
        Ok(buf.to_vec())
    }
}

// ---------------------------------------------------------------------------
// Newtype wrappers (numeric, inet)
// ---------------------------------------------------------------------------

impl Decode for crate::newtypes::PgNumeric {
    fn decode(buf: &[u8]) -> Result<Self, TypedError> {
        // PG binary numeric: ndigits(i16) weight(i16) sign(i16) dscale(i16) digits[](i16)
        if buf.len() < 8 {
            return Err(TypedError::Decode {
                column: 0,
                message: format!("PgNumeric: expected >= 8 bytes, got {}", buf.len()),
            });
        }
        let ndigits = i16::from_be_bytes([buf[0], buf[1]]) as usize;
        let weight = i16::from_be_bytes([buf[2], buf[3]]);
        let sign = i16::from_be_bytes([buf[4], buf[5]]);
        let dscale = i16::from_be_bytes([buf[6], buf[7]]) as usize;

        if buf.len() < 8 + ndigits * 2 {
            return Err(TypedError::Decode {
                column: 0,
                message: "PgNumeric: truncated digit data".into(),
            });
        }

        // Special cases.
        if ndigits == 0 {
            return if dscale > 0 {
                let zeros: String = "0".repeat(dscale);
                Ok(crate::newtypes::PgNumeric(format!("0.{zeros}")))
            } else {
                Ok(crate::newtypes::PgNumeric("0".into()))
            };
        }

        let digits: Vec<i16> = (0..ndigits)
            .map(|i| {
                let off = 8 + i * 2;
                i16::from_be_bytes([buf[off], buf[off + 1]])
            })
            .collect();

        // Build the string. Each digit is 0-9999 (base-10000).
        // weight = position of first digit group (0 = units, 1 = ten-thousands, etc.).
        let mut result = String::new();
        if sign == 0x4000 {
            result.push('-');
        }

        // Integer part: digit groups at positions weight down to 0.
        let int_groups = (weight + 1).max(0) as usize;
        for i in 0..int_groups {
            let d = digits.get(i).copied().unwrap_or(0);
            if i == 0 {
                result.push_str(&d.to_string());
            } else {
                result.push_str(&format!("{d:04}"));
            }
        }
        if int_groups == 0 {
            result.push('0');
        }

        // Fractional part.
        // Each fractional position p (-1, -2, ...) maps to digit index (weight - p).
        // Positions without stored digits are zero-padded.
        if dscale > 0 {
            result.push('.');
            let mut frac_chars = 0;
            let mut pos: i16 = -1;
            while frac_chars < dscale {
                let idx = (weight - pos) as isize;
                let d = if idx >= 0 && (idx as usize) < ndigits {
                    digits[idx as usize]
                } else {
                    0
                };
                let s = format!("{d:04}");
                for ch in s.chars() {
                    if frac_chars >= dscale {
                        break;
                    }
                    result.push(ch);
                    frac_chars += 1;
                }
                pos -= 1;
            }
        }

        Ok(crate::newtypes::PgNumeric(result))
    }
}

impl Decode for crate::newtypes::PgTimestamp {
    fn decode(buf: &[u8]) -> Result<Self, TypedError> {
        let us = i64::decode(buf)?;
        Ok(match us {
            PG_TIMESTAMP_INFINITY => crate::newtypes::PgTimestamp::Infinity,
            PG_TIMESTAMP_NEG_INFINITY => crate::newtypes::PgTimestamp::NegInfinity,
            v => crate::newtypes::PgTimestamp::Value(v),
        })
    }
}

impl Decode for crate::newtypes::PgDate {
    fn decode(buf: &[u8]) -> Result<Self, TypedError> {
        let days = i32::decode(buf)?;
        Ok(match days {
            PG_DATE_INFINITY => crate::newtypes::PgDate::Infinity,
            PG_DATE_NEG_INFINITY => crate::newtypes::PgDate::NegInfinity,
            v => crate::newtypes::PgDate::Value(v),
        })
    }
}

impl Decode for crate::newtypes::PgInet {
    fn decode(buf: &[u8]) -> Result<Self, TypedError> {
        // PG inet binary: family(u8) netmask(u8) is_cidr(u8) addr_len(u8) addr[addr_len]
        if buf.len() < 4 {
            return Err(TypedError::Decode {
                column: 0,
                message: "PgInet: too short".into(),
            });
        }
        let family = buf[0];
        let netmask = buf[1];
        let addr_len = buf[3] as usize;
        if buf.len() < 4 + addr_len {
            return Err(TypedError::Decode {
                column: 0,
                message: "PgInet: truncated address".into(),
            });
        }
        let addr = &buf[4..4 + addr_len];
        let s = match family {
            2 if addr_len == 4 => {
                format!("{}.{}.{}.{}/{netmask}", addr[0], addr[1], addr[2], addr[3])
            }
            3 if addr_len == 16 => {
                let mut parts = Vec::new();
                for i in (0..16).step_by(2) {
                    parts.push(format!("{:x}", u16::from_be_bytes([addr[i], addr[i + 1]])));
                }
                format!("{}/{netmask}", parts.join(":"))
            }
            _ => {
                return Err(TypedError::Decode {
                    column: 0,
                    message: format!("PgInet: unknown family {family}"),
                });
            }
        };
        Ok(crate::newtypes::PgInet(s))
    }
}

// ---------------------------------------------------------------------------
// Array types: generic via macro for all Decode types
// ---------------------------------------------------------------------------

/// Parse a PG array header, returns (element_oid, num_elements, data_offset).
fn parse_array_header(buf: &[u8]) -> Result<(u32, usize, usize), TypedError> {
    if buf.len() < 12 {
        return Err(TypedError::Decode {
            column: 0,
            message: "array: header too short".into(),
        });
    }
    let ndim = i32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]);
    let element_oid = u32::from_be_bytes([buf[8], buf[9], buf[10], buf[11]]);
    if ndim == 0 {
        return Ok((element_oid, 0, 12));
    }
    if ndim != 1 {
        return Err(TypedError::Decode {
            column: 0,
            message: format!("array: only 1D arrays supported, got {ndim}D"),
        });
    }
    if buf.len() < 20 {
        return Err(TypedError::Decode {
            column: 0,
            message: "array: 1D header too short".into(),
        });
    }
    let dim_len = i32::from_be_bytes([buf[12], buf[13], buf[14], buf[15]]) as usize;
    Ok((element_oid, dim_len, 20))
}

/// Decode array elements from binary format using a per-element decode function.
fn decode_array_elements<T, F>(buf: &[u8], decode_fn: F) -> Result<Vec<T>, TypedError>
where
    F: Fn(&[u8]) -> Result<T, TypedError>,
{
    let (_, count, mut offset) = parse_array_header(buf)?;
    let mut result = Vec::with_capacity(count);
    for _ in 0..count {
        if offset + 4 > buf.len() {
            return Err(TypedError::Decode {
                column: 0,
                message: "array: truncated element header".into(),
            });
        }
        let len = i32::from_be_bytes([
            buf[offset], buf[offset + 1], buf[offset + 2], buf[offset + 3],
        ]);
        offset += 4;
        if len == -1 {
            return Err(TypedError::Decode {
                column: 0,
                message: "array: unexpected NULL element".into(),
            });
        }
        let len = len as usize;
        if offset + len > buf.len() {
            return Err(TypedError::Decode {
                column: 0,
                message: "array: element data truncated".into(),
            });
        }
        result.push(decode_fn(&buf[offset..offset + len])?);
        offset += len;
    }
    Ok(result)
}

macro_rules! impl_array_decode {
    ($t:ty) => {
        impl Decode for Vec<$t> {
            fn decode(buf: &[u8]) -> Result<Self, TypedError> {
                decode_array_elements(buf, <$t>::decode)
            }
        }
    };
}

impl_array_decode!(bool);
impl_array_decode!(i16);
impl_array_decode!(i32);
impl_array_decode!(i64);
impl_array_decode!(f32);
impl_array_decode!(f64);
impl_array_decode!(String);

// ---------------------------------------------------------------------------
// Chrono types (behind "chrono" feature)
// ---------------------------------------------------------------------------

#[cfg(feature = "chrono")]
const PG_EPOCH_OFFSET_US: i64 = 946_684_800_000_000;

/// PostgreSQL sentinel values for infinity.
const PG_TIMESTAMP_INFINITY: i64 = i64::MAX;
const PG_TIMESTAMP_NEG_INFINITY: i64 = i64::MIN;
const PG_DATE_INFINITY: i32 = i32::MAX;
const PG_DATE_NEG_INFINITY: i32 = i32::MIN;

#[cfg(feature = "chrono")]
impl Decode for chrono::NaiveDateTime {
    fn decode(buf: &[u8]) -> Result<Self, TypedError> {
        let us = i64::decode(buf)?;
        if us == PG_TIMESTAMP_INFINITY {
            return Err(TypedError::Decode {
                column: 0,
                message: "NaiveDateTime: 'infinity' cannot be represented by chrono (use PgTimestamp instead)".into(),
            });
        }
        if us == PG_TIMESTAMP_NEG_INFINITY {
            return Err(TypedError::Decode {
                column: 0,
                message: "NaiveDateTime: '-infinity' cannot be represented by chrono (use PgTimestamp instead)".into(),
            });
        }
        let unix_us = us + PG_EPOCH_OFFSET_US;
        chrono::DateTime::from_timestamp_micros(unix_us)
            .map(|dt| dt.naive_utc())
            .ok_or_else(|| TypedError::Decode {
                column: 0,
                message: format!("NaiveDateTime: invalid microseconds {us}"),
            })
    }
}

#[cfg(feature = "chrono")]
impl Decode for chrono::DateTime<chrono::Utc> {
    fn decode(buf: &[u8]) -> Result<Self, TypedError> {
        let us = i64::decode(buf)?;
        if us == PG_TIMESTAMP_INFINITY || us == PG_TIMESTAMP_NEG_INFINITY {
            return Err(TypedError::Decode {
                column: 0,
                message: "DateTime<Utc>: infinity cannot be represented by chrono (use PgTimestamp instead)".into(),
            });
        }
        let unix_us = us + PG_EPOCH_OFFSET_US;
        chrono::DateTime::from_timestamp_micros(unix_us).ok_or_else(|| TypedError::Decode {
            column: 0,
            message: format!("DateTime<Utc>: invalid microseconds {us}"),
        })
    }
}

#[cfg(feature = "chrono")]
impl Decode for chrono::NaiveDate {
    fn decode(buf: &[u8]) -> Result<Self, TypedError> {
        let days = i32::decode(buf)?;
        if days == PG_DATE_INFINITY || days == PG_DATE_NEG_INFINITY {
            return Err(TypedError::Decode {
                column: 0,
                message: "NaiveDate: infinity cannot be represented by chrono (use PgDate instead)".into(),
            });
        }
        let pg_epoch = chrono::NaiveDate::from_ymd_opt(2000, 1, 1).unwrap();
        pg_epoch
            .checked_add_signed(chrono::Duration::days(days as i64))
            .ok_or_else(|| TypedError::Decode {
                column: 0,
                message: format!("NaiveDate: invalid days offset {days}"),
            })
    }
}

#[cfg(feature = "chrono")]
impl Decode for chrono::NaiveTime {
    fn decode(buf: &[u8]) -> Result<Self, TypedError> {
        let us = i64::decode(buf)?;
        let secs = (us / 1_000_000) as u32;
        let nano = ((us % 1_000_000) * 1000) as u32;
        chrono::NaiveTime::from_num_seconds_from_midnight_opt(secs, nano).ok_or_else(|| {
            TypedError::Decode {
                column: 0,
                message: format!("NaiveTime: invalid microseconds {us}"),
            }
        })
    }
}

// ---------------------------------------------------------------------------
// JSON types (behind "json" feature)
// ---------------------------------------------------------------------------

#[cfg(feature = "json")]
impl Decode for serde_json::Value {
    fn decode(buf: &[u8]) -> Result<Self, TypedError> {
        // JSONB binary: first byte is version (1), rest is JSON text.
        // JSON binary: just JSON text.
        let text = if !buf.is_empty() && buf[0] == 1 {
            &buf[1..] // Skip JSONB version byte.
        } else {
            buf
        };
        serde_json::from_slice(text).map_err(|e| TypedError::Decode {
            column: 0,
            message: format!("JSON: {e}"),
        })
    }
}

// ---------------------------------------------------------------------------
// UUID (behind "uuid" feature)
// ---------------------------------------------------------------------------

#[cfg(feature = "uuid")]
impl Decode for uuid::Uuid {
    fn decode(buf: &[u8]) -> Result<Self, TypedError> {
        if buf.len() != 16 {
            return Err(TypedError::Decode {
                column: 0,
                message: format!("UUID: expected 16 bytes, got {}", buf.len()),
            });
        }
        Ok(uuid::Uuid::from_bytes(buf.try_into().unwrap()))
    }
}

// ---------------------------------------------------------------------------
// Feature-gated array Decode impls
// ---------------------------------------------------------------------------

impl_array_decode!(crate::newtypes::PgNumeric);
impl_array_decode!(crate::newtypes::PgInet);

#[cfg(feature = "chrono")]
impl_array_decode!(chrono::NaiveDate);
#[cfg(feature = "chrono")]
impl_array_decode!(chrono::NaiveTime);
#[cfg(feature = "chrono")]
impl_array_decode!(chrono::NaiveDateTime);
#[cfg(feature = "chrono")]
impl_array_decode!(chrono::DateTime<chrono::Utc>);

#[cfg(feature = "uuid")]
impl_array_decode!(uuid::Uuid);

#[cfg(feature = "json")]
impl_array_decode!(serde_json::Value);

// ---------------------------------------------------------------------------
// Text-format fallback decode
// ---------------------------------------------------------------------------

/// Decode from PostgreSQL text format (for backwards compat and mixed-mode queries).
pub trait DecodeText: Sized {
    fn decode_text(s: &str) -> Result<Self, TypedError>;
}

impl DecodeText for bool {
    fn decode_text(s: &str) -> Result<Self, TypedError> {
        match s {
            "t" | "true" | "1" => Ok(true),
            "f" | "false" | "0" => Ok(false),
            _ => Err(TypedError::Decode { column: 0, message: format!("bool: {s:?}") }),
        }
    }
}

impl DecodeText for i16 {
    fn decode_text(s: &str) -> Result<Self, TypedError> {
        s.parse().map_err(|_| TypedError::Decode { column: 0, message: format!("i16: {s:?}") })
    }
}

impl DecodeText for i32 {
    fn decode_text(s: &str) -> Result<Self, TypedError> {
        s.parse().map_err(|_| TypedError::Decode { column: 0, message: format!("i32: {s:?}") })
    }
}

impl DecodeText for i64 {
    fn decode_text(s: &str) -> Result<Self, TypedError> {
        s.parse().map_err(|_| TypedError::Decode { column: 0, message: format!("i64: {s:?}") })
    }
}

impl DecodeText for f32 {
    fn decode_text(s: &str) -> Result<Self, TypedError> {
        s.parse().map_err(|_| TypedError::Decode { column: 0, message: format!("f32: {s:?}") })
    }
}

impl DecodeText for f64 {
    fn decode_text(s: &str) -> Result<Self, TypedError> {
        s.parse().map_err(|_| TypedError::Decode { column: 0, message: format!("f64: {s:?}") })
    }
}

impl DecodeText for String {
    fn decode_text(s: &str) -> Result<Self, TypedError> {
        Ok(s.to_string())
    }
}

impl DecodeText for Vec<u8> {
    fn decode_text(s: &str) -> Result<Self, TypedError> {
        // PG text format for bytea: hex encoding (\x...) or escape format.
        if let Some(hex) = s.strip_prefix("\\x") {
            (0..hex.len())
                .step_by(2)
                .map(|i| {
                    u8::from_str_radix(&hex[i..i + 2], 16).map_err(|_| TypedError::Decode {
                        column: 0,
                        message: format!("bytea: invalid hex at offset {i}"),
                    })
                })
                .collect()
        } else {
            Ok(s.as_bytes().to_vec())
        }
    }
}

#[cfg(feature = "chrono")]
impl DecodeText for chrono::NaiveDateTime {
    fn decode_text(s: &str) -> Result<Self, TypedError> {
        s.parse().map_err(|e| TypedError::Decode {
            column: 0,
            message: format!("NaiveDateTime: {e}"),
        })
    }
}

#[cfg(feature = "chrono")]
impl DecodeText for chrono::DateTime<chrono::Utc> {
    fn decode_text(s: &str) -> Result<Self, TypedError> {
        s.parse().map_err(|e| TypedError::Decode {
            column: 0,
            message: format!("DateTime<Utc>: {e}"),
        })
    }
}

#[cfg(feature = "chrono")]
impl DecodeText for chrono::NaiveDate {
    fn decode_text(s: &str) -> Result<Self, TypedError> {
        s.parse().map_err(|e| TypedError::Decode {
            column: 0,
            message: format!("NaiveDate: {e}"),
        })
    }
}

#[cfg(feature = "chrono")]
impl DecodeText for chrono::NaiveTime {
    fn decode_text(s: &str) -> Result<Self, TypedError> {
        s.parse().map_err(|e| TypedError::Decode {
            column: 0,
            message: format!("NaiveTime: {e}"),
        })
    }
}

#[cfg(feature = "json")]
impl DecodeText for serde_json::Value {
    fn decode_text(s: &str) -> Result<Self, TypedError> {
        serde_json::from_str(s).map_err(|e| TypedError::Decode {
            column: 0,
            message: format!("JSON: {e}"),
        })
    }
}

#[cfg(feature = "uuid")]
impl DecodeText for uuid::Uuid {
    fn decode_text(s: &str) -> Result<Self, TypedError> {
        s.parse().map_err(|e| TypedError::Decode {
            column: 0,
            message: format!("UUID: {e}"),
        })
    }
}

impl DecodeText for crate::newtypes::PgTimestamp {
    fn decode_text(s: &str) -> Result<Self, TypedError> {
        match s {
            "infinity" => Ok(crate::newtypes::PgTimestamp::Infinity),
            "-infinity" => Ok(crate::newtypes::PgTimestamp::NegInfinity),
            _ => s.parse::<i64>().map(crate::newtypes::PgTimestamp::Value).map_err(|_| {
                TypedError::Decode { column: 0, message: format!("PgTimestamp: {s:?}") }
            }),
        }
    }
}

impl DecodeText for crate::newtypes::PgDate {
    fn decode_text(s: &str) -> Result<Self, TypedError> {
        match s {
            "infinity" => Ok(crate::newtypes::PgDate::Infinity),
            "-infinity" => Ok(crate::newtypes::PgDate::NegInfinity),
            _ => s.parse::<i32>().map(crate::newtypes::PgDate::Value).map_err(|_| {
                TypedError::Decode { column: 0, message: format!("PgDate: {s:?}") }
            }),
        }
    }
}

impl DecodeText for crate::newtypes::PgNumeric {
    fn decode_text(s: &str) -> Result<Self, TypedError> {
        Ok(crate::newtypes::PgNumeric(s.to_string()))
    }
}

impl DecodeText for crate::newtypes::PgInet {
    fn decode_text(s: &str) -> Result<Self, TypedError> {
        Ok(crate::newtypes::PgInet(s.to_string()))
    }
}

// ---------------------------------------------------------------------------
// Generic text-format array parser
// ---------------------------------------------------------------------------

/// Parse a PostgreSQL text-format array `{elem1,elem2,"quoted elem",...}`.
/// Returns the unquoted, unescaped element strings.
/// Errors on unquoted `NULL` elements (use `Vec<Option<T>>` for nullable arrays).
pub(crate) fn parse_pg_text_array(s: &str) -> Result<Vec<String>, TypedError> {
    let s = s.trim();
    if !s.starts_with('{') || !s.ends_with('}') {
        return Err(TypedError::Decode {
            column: 0,
            message: format!("array: expected {{...}}, got {s:?}"),
        });
    }
    let inner = &s[1..s.len() - 1];
    if inner.trim().is_empty() {
        return Ok(Vec::new());
    }

    let mut result = Vec::new();
    let mut chars = inner.chars().peekable();

    while chars.peek().is_some() {
        // Skip whitespace.
        while chars.peek() == Some(&' ') {
            chars.next();
        }
        if chars.peek().is_none() {
            break;
        }

        if chars.peek() == Some(&'"') {
            // Quoted element: handle backslash escapes.
            chars.next(); // consume opening quote
            let mut elem = String::new();
            loop {
                match chars.next() {
                    Some('\\') => {
                        if let Some(c) = chars.next() {
                            elem.push(c);
                        }
                    }
                    Some('"') => break,
                    Some(c) => elem.push(c),
                    None => {
                        return Err(TypedError::Decode {
                            column: 0,
                            message: "array: unterminated quoted element".into(),
                        })
                    }
                }
            }
            result.push(elem);
        } else {
            // Unquoted element.
            let mut elem = String::new();
            while let Some(&c) = chars.peek() {
                if c == ',' {
                    break;
                }
                elem.push(c);
                chars.next();
            }
            let elem = elem.trim().to_string();
            if elem == "NULL" {
                return Err(TypedError::Decode {
                    column: 0,
                    message: "array: unexpected NULL element".into(),
                });
            }
            result.push(elem);
        }

        // Skip comma separator.
        if chars.peek() == Some(&',') {
            chars.next();
        }
    }

    Ok(result)
}

macro_rules! impl_array_decode_text {
    ($t:ty) => {
        impl DecodeText for Vec<$t> {
            fn decode_text(s: &str) -> Result<Self, TypedError> {
                parse_pg_text_array(s)?
                    .iter()
                    .map(|e| <$t>::decode_text(e))
                    .collect()
            }
        }
    };
}

impl_array_decode_text!(bool);
impl_array_decode_text!(i16);
impl_array_decode_text!(i32);
impl_array_decode_text!(i64);
impl_array_decode_text!(f32);
impl_array_decode_text!(f64);
impl_array_decode_text!(String);
impl_array_decode_text!(crate::newtypes::PgNumeric);
impl_array_decode_text!(crate::newtypes::PgInet);

#[cfg(feature = "chrono")]
impl_array_decode_text!(chrono::NaiveDate);
#[cfg(feature = "chrono")]
impl_array_decode_text!(chrono::NaiveTime);
#[cfg(feature = "chrono")]
impl_array_decode_text!(chrono::NaiveDateTime);
#[cfg(feature = "chrono")]
impl_array_decode_text!(chrono::DateTime<chrono::Utc>);

#[cfg(feature = "uuid")]
impl_array_decode_text!(uuid::Uuid);

#[cfg(feature = "json")]
impl_array_decode_text!(serde_json::Value);
