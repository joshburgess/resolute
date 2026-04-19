//! PostgreSQL range type support.
//!
//! Provides `PgRange<T>` for `int4range`, `int8range`, `numrange`,
//! `daterange`, `tsrange`, `tstzrange`.
//!
//! ```ignore
//! use resolute::PgRange;
//!
//! // Inclusive-exclusive range: [1, 10)
//! let r = PgRange::new(Some(1i32), Some(10i32), true, false);
//!
//! // Empty range:
//! let r: PgRange<i32> = PgRange::empty();
//!
//! // Unbounded lower:
//! let r = PgRange::new(None, Some(100i32), false, false);
//! ```

use crate::decode::{Decode, DecodeText};
use crate::encode::Encode;
use crate::error::TypedError;
use crate::oid::TypeOid;
use crate::pg_type::PgType;
use bytes::BytesMut;

/// A PostgreSQL range value.
#[derive(Debug, Clone, PartialEq)]
pub enum PgRange<T> {
    /// An empty range (contains no values).
    Empty,
    /// A non-empty range with optional bounds.
    Range {
        /// Lower bound (None = unbounded).
        lower: Option<T>,
        /// Upper bound (None = unbounded).
        upper: Option<T>,
        /// Whether the lower bound is inclusive.
        lower_inclusive: bool,
        /// Whether the upper bound is inclusive.
        upper_inclusive: bool,
    },
}

impl<T> PgRange<T> {
    /// Create a new range.
    pub fn new(
        lower: Option<T>,
        upper: Option<T>,
        lower_inclusive: bool,
        upper_inclusive: bool,
    ) -> Self {
        Self::Range {
            lower,
            upper,
            lower_inclusive,
            upper_inclusive,
        }
    }

    /// Create an empty range.
    pub fn empty() -> Self {
        Self::Empty
    }

    /// Check if the range is empty.
    pub fn is_empty(&self) -> bool {
        matches!(self, Self::Empty)
    }
}

// Binary format flags (from PostgreSQL src/include/utils/rangetypes.h)
const RANGE_EMPTY: u8 = 0x01;
const RANGE_LB_INC: u8 = 0x02;
const RANGE_UB_INC: u8 = 0x04;
const RANGE_LB_INF: u8 = 0x08;
const RANGE_UB_INF: u8 = 0x10;

impl<T: Encode + PgType> Encode for PgRange<T> {
    fn type_oid(&self) -> TypeOid {
        TypeOid::Unspecified
    }

    fn encode(&self, buf: &mut BytesMut) {
        match self {
            PgRange::Empty => {
                buf.extend_from_slice(&[RANGE_EMPTY]);
            }
            PgRange::Range {
                lower,
                upper,
                lower_inclusive,
                upper_inclusive,
            } => {
                let mut flags: u8 = 0;
                if *lower_inclusive {
                    flags |= RANGE_LB_INC;
                }
                if *upper_inclusive {
                    flags |= RANGE_UB_INC;
                }
                if lower.is_none() {
                    flags |= RANGE_LB_INF;
                }
                if upper.is_none() {
                    flags |= RANGE_UB_INF;
                }
                buf.extend_from_slice(&[flags]);

                if let Some(ref lb) = lower {
                    lb.encode_param(buf);
                }
                if let Some(ref ub) = upper {
                    ub.encode_param(buf);
                }
            }
        }
    }
}

impl<T: Decode + PgType> Decode for PgRange<T> {
    fn decode(buf: &[u8]) -> Result<Self, TypedError> {
        if buf.is_empty() {
            return Err(TypedError::Decode {
                column: 0,
                message: "range: empty buffer".into(),
            });
        }

        let flags = buf[0];
        if flags & RANGE_EMPTY != 0 {
            return Ok(PgRange::Empty);
        }

        let mut offset = 1;

        let lower = if flags & RANGE_LB_INF != 0 {
            None
        } else {
            if offset + 4 > buf.len() {
                return Err(TypedError::Decode {
                    column: 0,
                    message: "range: truncated lower bound length".into(),
                });
            }
            let len = i32::from_be_bytes([
                buf[offset],
                buf[offset + 1],
                buf[offset + 2],
                buf[offset + 3],
            ]) as usize;
            offset += 4;
            if offset + len > buf.len() {
                return Err(TypedError::Decode {
                    column: 0,
                    message: "range: truncated lower bound data".into(),
                });
            }
            let val = T::decode(&buf[offset..offset + len])?;
            offset += len;
            Some(val)
        };

        let upper = if flags & RANGE_UB_INF != 0 {
            None
        } else {
            if offset + 4 > buf.len() {
                return Err(TypedError::Decode {
                    column: 0,
                    message: "range: truncated upper bound length".into(),
                });
            }
            let len = i32::from_be_bytes([
                buf[offset],
                buf[offset + 1],
                buf[offset + 2],
                buf[offset + 3],
            ]) as usize;
            offset += 4;
            if offset + len > buf.len() {
                return Err(TypedError::Decode {
                    column: 0,
                    message: "range: truncated upper bound data".into(),
                });
            }
            let val = T::decode(&buf[offset..offset + len])?;
            Some(val)
        };

        Ok(PgRange::Range {
            lower,
            upper,
            lower_inclusive: flags & RANGE_LB_INC != 0,
            upper_inclusive: flags & RANGE_UB_INC != 0,
        })
    }
}

impl<T: DecodeText + PgType> DecodeText for PgRange<T> {
    fn decode_text(s: &str) -> Result<Self, TypedError> {
        let s = s.trim();
        if s == "empty" {
            return Ok(PgRange::Empty);
        }

        if s.len() < 3 {
            return Err(TypedError::Decode {
                column: 0,
                message: format!("range: invalid text format: {s:?}"),
            });
        }

        let lower_inclusive = s.starts_with('[');
        let upper_inclusive = s.ends_with(']');

        // Strip the brackets
        let inner = &s[1..s.len() - 1];
        let (lower_str, upper_str) = inner.split_once(',').ok_or_else(|| TypedError::Decode {
            column: 0,
            message: format!("range: missing comma in: {s:?}"),
        })?;

        let lower = if lower_str.is_empty() {
            None
        } else {
            Some(T::decode_text(lower_str)?)
        };

        let upper = if upper_str.is_empty() {
            None
        } else {
            Some(T::decode_text(upper_str)?)
        };

        Ok(PgRange::Range {
            lower,
            upper,
            lower_inclusive,
            upper_inclusive,
        })
    }
}

// OID constants for range types.
const INT4RANGE_OID: u32 = 3904;
const INT8RANGE_OID: u32 = 3926;
const NUMRANGE_OID: u32 = 3906;
const DATERANGE_OID: u32 = 3912;
const TSRANGE_OID: u32 = 3908;
const TSTZRANGE_OID: u32 = 3910;

impl PgType for PgRange<i32> {
    const OID: u32 = INT4RANGE_OID;
    const ARRAY_OID: u32 = 3905;
}

impl PgType for PgRange<i64> {
    const OID: u32 = INT8RANGE_OID;
    const ARRAY_OID: u32 = 3927;
}

impl PgType for PgRange<crate::newtypes::PgNumeric> {
    const OID: u32 = NUMRANGE_OID;
    const ARRAY_OID: u32 = 3907;
}

#[cfg(feature = "chrono")]
impl PgType for PgRange<chrono::NaiveDate> {
    const OID: u32 = DATERANGE_OID;
    const ARRAY_OID: u32 = 3913;
}

#[cfg(feature = "chrono")]
impl PgType for PgRange<chrono::NaiveDateTime> {
    const OID: u32 = TSRANGE_OID;
    const ARRAY_OID: u32 = 3909;
}

#[cfg(feature = "chrono")]
impl PgType for PgRange<chrono::DateTime<chrono::Utc>> {
    const OID: u32 = TSTZRANGE_OID;
    const ARRAY_OID: u32 = 3911;
}
