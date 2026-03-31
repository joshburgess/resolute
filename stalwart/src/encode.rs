//! Encode Rust types into PostgreSQL binary wire format.
//!
//! Binary format is big-endian for all numeric types.
//! The wire protocol sends: 4-byte length prefix + raw bytes.
//! Encode only produces the raw bytes; the length prefix is handled by the caller.

use bytes::{BufMut, BytesMut};

use crate::oid::TypeOid;

/// Trait for types that can be encoded into PostgreSQL binary format.
pub trait Encode {
    /// The PostgreSQL type OID for this Rust type.
    fn type_oid(&self) -> TypeOid;

    /// Encode the value into binary format, appending to `buf`.
    fn encode(&self, buf: &mut BytesMut);

    /// Encode as a binary parameter: 4-byte length + data.
    /// Returns None for NULL values (handled by Option<T>).
    fn encode_param(&self, buf: &mut BytesMut) {
        let start = buf.len();
        buf.put_i32(0); // placeholder for length
        self.encode(buf);
        let len = (buf.len() - start - 4) as i32;
        let start_bytes = &mut buf[start..start + 4];
        start_bytes.copy_from_slice(&len.to_be_bytes());
    }
}

// ---------------------------------------------------------------------------
// Primitive implementations
// ---------------------------------------------------------------------------

impl Encode for bool {
    fn type_oid(&self) -> TypeOid { TypeOid::Bool }
    fn encode(&self, buf: &mut BytesMut) {
        buf.put_u8(if *self { 1 } else { 0 });
    }
}

impl Encode for i16 {
    fn type_oid(&self) -> TypeOid { TypeOid::Int2 }
    fn encode(&self, buf: &mut BytesMut) {
        buf.put_i16(*self);
    }
}

impl Encode for i32 {
    fn type_oid(&self) -> TypeOid { TypeOid::Int4 }
    fn encode(&self, buf: &mut BytesMut) {
        buf.put_i32(*self);
    }
}

impl Encode for i64 {
    fn type_oid(&self) -> TypeOid { TypeOid::Int8 }
    fn encode(&self, buf: &mut BytesMut) {
        buf.put_i64(*self);
    }
}

impl Encode for f32 {
    fn type_oid(&self) -> TypeOid { TypeOid::Float4 }
    fn encode(&self, buf: &mut BytesMut) {
        buf.put_f32(*self);
    }
}

impl Encode for f64 {
    fn type_oid(&self) -> TypeOid { TypeOid::Float8 }
    fn encode(&self, buf: &mut BytesMut) {
        buf.put_f64(*self);
    }
}

impl Encode for str {
    fn type_oid(&self) -> TypeOid { TypeOid::Text }
    fn encode(&self, buf: &mut BytesMut) {
        buf.put_slice(self.as_bytes());
    }
}

impl Encode for String {
    fn type_oid(&self) -> TypeOid { TypeOid::Text }
    fn encode(&self, buf: &mut BytesMut) {
        buf.put_slice(self.as_bytes());
    }
}

impl Encode for &str {
    fn type_oid(&self) -> TypeOid { TypeOid::Text }
    fn encode(&self, buf: &mut BytesMut) {
        buf.put_slice(self.as_bytes());
    }
}

impl Encode for [u8] {
    fn type_oid(&self) -> TypeOid { TypeOid::Bytea }
    fn encode(&self, buf: &mut BytesMut) {
        buf.put_slice(self);
    }
}

impl Encode for Vec<u8> {
    fn type_oid(&self) -> TypeOid { TypeOid::Bytea }
    fn encode(&self, buf: &mut BytesMut) {
        buf.put_slice(self);
    }
}

// ---------------------------------------------------------------------------
// Newtype wrappers (numeric, inet)
// ---------------------------------------------------------------------------

impl Encode for crate::newtypes::PgNumeric {
    fn type_oid(&self) -> TypeOid { TypeOid::Numeric }
    fn encode(&self, buf: &mut BytesMut) {
        // Send as text — PG accepts text-format numeric in binary protocol
        // when the OID is set to numeric.
        buf.put_slice(self.0.as_bytes());
    }
}

impl Encode for crate::newtypes::PgTimestamp {
    fn type_oid(&self) -> TypeOid { TypeOid::Timestamp }
    fn encode(&self, buf: &mut BytesMut) {
        match self {
            crate::newtypes::PgTimestamp::Value(us) => buf.put_i64(*us),
            crate::newtypes::PgTimestamp::Infinity => buf.put_i64(i64::MAX),
            crate::newtypes::PgTimestamp::NegInfinity => buf.put_i64(i64::MIN),
        }
    }
}

impl Encode for crate::newtypes::PgDate {
    fn type_oid(&self) -> TypeOid { TypeOid::Date }
    fn encode(&self, buf: &mut BytesMut) {
        match self {
            crate::newtypes::PgDate::Value(d) => buf.put_i32(*d),
            crate::newtypes::PgDate::Infinity => buf.put_i32(i32::MAX),
            crate::newtypes::PgDate::NegInfinity => buf.put_i32(i32::MIN),
        }
    }
}

impl Encode for crate::newtypes::PgInet {
    fn type_oid(&self) -> TypeOid { TypeOid::Inet }
    fn encode(&self, buf: &mut BytesMut) {
        buf.put_slice(self.0.as_bytes());
    }
}

// ---------------------------------------------------------------------------
// Array types: generic via macro for all Encode + PgType types
// ---------------------------------------------------------------------------

/// Encode a PG array header: ndim=1, has_null, element_oid, dim_len, lower_bound=1.
fn encode_array_header(buf: &mut BytesMut, has_null: bool, element_oid: u32, len: usize) {
    buf.put_i32(1); // ndim
    buf.put_i32(if has_null { 1 } else { 0 });
    buf.put_u32(element_oid);
    buf.put_i32(len as i32); // dim length
    buf.put_i32(1); // lower bound
}

macro_rules! impl_array_encode {
    ($t:ty, $array_oid_variant:ident) => {
        impl Encode for Vec<$t> {
            fn type_oid(&self) -> TypeOid { TypeOid::$array_oid_variant }
            fn encode(&self, buf: &mut BytesMut) {
                encode_array_header(
                    buf,
                    false,
                    <$t as crate::pg_type::PgType>::OID,
                    self.len(),
                );
                for v in self {
                    v.encode_param(buf);
                }
            }
        }
    };
}

impl_array_encode!(bool, BoolArray);
impl_array_encode!(i16, Int2Array);
impl_array_encode!(i32, Int4Array);
impl_array_encode!(i64, Int8Array);
impl_array_encode!(f32, Float4Array);
impl_array_encode!(f64, Float8Array);
impl_array_encode!(String, TextArray);

// ---------------------------------------------------------------------------
// Chrono types (behind "chrono" feature)
// ---------------------------------------------------------------------------

/// PG epoch: 2000-01-01 00:00:00 UTC.
/// Offset from Unix epoch in microseconds.
#[cfg(feature = "chrono")]
const PG_EPOCH_OFFSET_US: i64 = 946_684_800_000_000;

#[cfg(feature = "chrono")]
impl Encode for chrono::NaiveDateTime {
    fn type_oid(&self) -> TypeOid { TypeOid::Timestamp }
    fn encode(&self, buf: &mut BytesMut) {
        // PG stores timestamp as microseconds since 2000-01-01.
        let us = self.and_utc().timestamp_micros() - PG_EPOCH_OFFSET_US;
        buf.put_i64(us);
    }
}

#[cfg(feature = "chrono")]
impl Encode for chrono::DateTime<chrono::Utc> {
    fn type_oid(&self) -> TypeOid { TypeOid::Timestamptz }
    fn encode(&self, buf: &mut BytesMut) {
        let us = self.timestamp_micros() - PG_EPOCH_OFFSET_US;
        buf.put_i64(us);
    }
}

#[cfg(feature = "chrono")]
impl Encode for chrono::NaiveDate {
    fn type_oid(&self) -> TypeOid { TypeOid::Date }
    fn encode(&self, buf: &mut BytesMut) {
        // PG stores date as days since 2000-01-01.
        let pg_epoch = chrono::NaiveDate::from_ymd_opt(2000, 1, 1).unwrap();
        let days = (*self - pg_epoch).num_days() as i32;
        buf.put_i32(days);
    }
}

#[cfg(feature = "chrono")]
impl Encode for chrono::NaiveTime {
    fn type_oid(&self) -> TypeOid { TypeOid::Time }
    fn encode(&self, buf: &mut BytesMut) {
        // PG stores time as microseconds since midnight.
        let us = self
            .signed_duration_since(chrono::NaiveTime::from_hms_opt(0, 0, 0).unwrap())
            .num_microseconds()
            .unwrap_or(0);
        buf.put_i64(us);
    }
}

// ---------------------------------------------------------------------------
// JSON types (behind "json" feature)
// ---------------------------------------------------------------------------

#[cfg(feature = "json")]
impl Encode for serde_json::Value {
    fn type_oid(&self) -> TypeOid { TypeOid::Jsonb }
    fn encode(&self, buf: &mut BytesMut) {
        // JSONB binary format: version byte (1) + JSON text.
        buf.put_u8(1);
        let json_text = serde_json::to_string(self).unwrap_or_default();
        buf.put_slice(json_text.as_bytes());
    }
}

// ---------------------------------------------------------------------------
// UUID (behind "uuid" feature)
// ---------------------------------------------------------------------------

#[cfg(feature = "uuid")]
impl Encode for uuid::Uuid {
    fn type_oid(&self) -> TypeOid { TypeOid::Uuid }
    fn encode(&self, buf: &mut BytesMut) {
        buf.put_slice(self.as_bytes());
    }
}

// ---------------------------------------------------------------------------
// Feature-gated array types
// ---------------------------------------------------------------------------

impl_array_encode!(crate::newtypes::PgNumeric, NumericArray);
impl_array_encode!(crate::newtypes::PgInet, InetArray);

#[cfg(feature = "chrono")]
impl_array_encode!(chrono::NaiveDate, DateArray);
#[cfg(feature = "chrono")]
impl_array_encode!(chrono::NaiveTime, TimeArray);
#[cfg(feature = "chrono")]
impl_array_encode!(chrono::NaiveDateTime, TimestampArray);
#[cfg(feature = "chrono")]
impl_array_encode!(chrono::DateTime<chrono::Utc>, TimestamptzArray);

#[cfg(feature = "uuid")]
impl_array_encode!(uuid::Uuid, UuidArray);

#[cfg(feature = "json")]
impl_array_encode!(serde_json::Value, JsonbArray);

// ---------------------------------------------------------------------------
// SqlParam: trait for query parameters (supports NULL via Option<T>)
// ---------------------------------------------------------------------------

/// Trait for values that can be used as query parameters.
/// Implemented for all `T: Encode` and for `Option<T>` (NULL).
pub trait SqlParam: Sync {
    /// The PostgreSQL type OID (0 = let server infer).
    fn param_oid(&self) -> u32;
    /// Encode the value into binary format. Returns None for NULL.
    fn encode_param_value(&self) -> Option<BytesMut>;
}

impl<T: Encode + Sync> SqlParam for T {
    fn param_oid(&self) -> u32 {
        self.type_oid().as_u32()
    }
    fn encode_param_value(&self) -> Option<BytesMut> {
        let mut buf = BytesMut::new();
        self.encode(&mut buf);
        Some(buf)
    }
}

impl<T: Encode + Sync> SqlParam for Option<T> {
    fn param_oid(&self) -> u32 {
        match self {
            Some(v) => v.type_oid().as_u32(),
            None => 0,
        }
    }
    fn encode_param_value(&self) -> Option<BytesMut> {
        match self {
            Some(v) => {
                let mut buf = BytesMut::new();
                v.encode(&mut buf);
                Some(buf)
            }
            None => None,
        }
    }
}
