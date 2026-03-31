//! PostgreSQL type metadata trait.
//!
//! Provides OID constants for Rust types, enabling generic array
//! encoding/decoding and derive macro code generation.

/// Trait providing PostgreSQL type OID metadata for a Rust type.
pub trait PgType {
    /// The OID of this scalar type.
    const OID: u32;
    /// The OID of the array form of this type (0 if none).
    const ARRAY_OID: u32;
}

// ---------------------------------------------------------------------------
// Primitive types
// ---------------------------------------------------------------------------

impl PgType for bool {
    const OID: u32 = 16;
    const ARRAY_OID: u32 = 1000;
}

impl PgType for i16 {
    const OID: u32 = 21;
    const ARRAY_OID: u32 = 1005;
}

impl PgType for i32 {
    const OID: u32 = 23;
    const ARRAY_OID: u32 = 1007;
}

impl PgType for i64 {
    const OID: u32 = 20;
    const ARRAY_OID: u32 = 1016;
}

impl PgType for f32 {
    const OID: u32 = 700;
    const ARRAY_OID: u32 = 1021;
}

impl PgType for f64 {
    const OID: u32 = 701;
    const ARRAY_OID: u32 = 1022;
}

impl PgType for String {
    const OID: u32 = 25;
    const ARRAY_OID: u32 = 1009;
}

impl PgType for Vec<u8> {
    const OID: u32 = 17; // bytea
    const ARRAY_OID: u32 = 1001;
}

// ---------------------------------------------------------------------------
// Newtype wrappers
// ---------------------------------------------------------------------------

impl PgType for crate::newtypes::PgNumeric {
    const OID: u32 = 1700;
    const ARRAY_OID: u32 = 1231;
}

impl PgType for crate::newtypes::PgInet {
    const OID: u32 = 869;
    const ARRAY_OID: u32 = 1041;
}

// ---------------------------------------------------------------------------
// Chrono types (behind "chrono" feature)
// ---------------------------------------------------------------------------

#[cfg(feature = "chrono")]
impl PgType for chrono::NaiveDate {
    const OID: u32 = 1082;
    const ARRAY_OID: u32 = 1182;
}

#[cfg(feature = "chrono")]
impl PgType for chrono::NaiveTime {
    const OID: u32 = 1083;
    const ARRAY_OID: u32 = 1183;
}

#[cfg(feature = "chrono")]
impl PgType for chrono::NaiveDateTime {
    const OID: u32 = 1114;
    const ARRAY_OID: u32 = 1115;
}

#[cfg(feature = "chrono")]
impl PgType for chrono::DateTime<chrono::Utc> {
    const OID: u32 = 1184;
    const ARRAY_OID: u32 = 1185;
}

// ---------------------------------------------------------------------------
// UUID (behind "uuid" feature)
// ---------------------------------------------------------------------------

#[cfg(feature = "uuid")]
impl PgType for uuid::Uuid {
    const OID: u32 = 2950;
    const ARRAY_OID: u32 = 2951;
}

// ---------------------------------------------------------------------------
// JSON (behind "json" feature)
// ---------------------------------------------------------------------------

#[cfg(feature = "json")]
impl PgType for serde_json::Value {
    const OID: u32 = 3802; // jsonb
    const ARRAY_OID: u32 = 3807;
}

// ---------------------------------------------------------------------------
// Array type PgType impls (Vec<T> where T is a known scalar)
// ---------------------------------------------------------------------------

macro_rules! impl_array_pg_type {
    ($t:ty) => {
        impl PgType for Vec<$t> {
            const OID: u32 = <$t as PgType>::ARRAY_OID;
            const ARRAY_OID: u32 = 0;
        }
    };
}

impl_array_pg_type!(bool);
impl_array_pg_type!(i16);
impl_array_pg_type!(i32);
impl_array_pg_type!(i64);
impl_array_pg_type!(f32);
impl_array_pg_type!(f64);
impl_array_pg_type!(String);
impl_array_pg_type!(crate::newtypes::PgNumeric);
impl_array_pg_type!(crate::newtypes::PgInet);

#[cfg(feature = "chrono")]
impl_array_pg_type!(chrono::NaiveDate);
#[cfg(feature = "chrono")]
impl_array_pg_type!(chrono::NaiveTime);
#[cfg(feature = "chrono")]
impl_array_pg_type!(chrono::NaiveDateTime);
#[cfg(feature = "chrono")]
impl_array_pg_type!(chrono::DateTime<chrono::Utc>);

#[cfg(feature = "uuid")]
impl_array_pg_type!(uuid::Uuid);

#[cfg(feature = "json")]
impl_array_pg_type!(serde_json::Value);
