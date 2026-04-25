//! PostgreSQL type OID constants.
//!
//! These match the OIDs in pg_type. When sending Bind with binary params,
//! the OID tells PostgreSQL how to interpret the bytes.

/// Known PostgreSQL type OIDs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u32)]
#[non_exhaustive]
pub enum TypeOid {
    /// OID 0: let the server infer the type from context.
    Unspecified = 0,
    /// `bool` (1 byte).
    Bool = 16,
    /// `bytea` (variable-length byte array).
    Bytea = 17,
    /// `int8` / `bigint` (8-byte signed integer).
    Int8 = 20,
    /// `int2` / `smallint` (2-byte signed integer).
    Int2 = 21,
    /// `int4` / `integer` (4-byte signed integer).
    Int4 = 23,
    /// `text` (variable-length UTF-8 string, no length limit).
    Text = 25,
    /// `oid` (4-byte unsigned object identifier).
    Oid = 26,
    /// `float4` / `real` (4-byte floating point).
    Float4 = 700,
    /// `float8` / `double precision` (8-byte floating point).
    Float8 = 701,
    /// `varchar(n)` (variable-length UTF-8 string with optional length cap).
    Varchar = 1043,
    /// `char` (1-byte fixed-length character).
    Char = 18,
    /// `name` (63-byte identifier, used in catalog tables).
    Name = 19,
    /// `timestamp` (8-byte timestamp without timezone).
    Timestamp = 1114,
    /// `timestamptz` (8-byte timestamp with timezone).
    Timestamptz = 1184,
    /// `date` (4-byte calendar date).
    Date = 1082,
    /// `time` (8-byte time of day).
    Time = 1083,
    /// `interval` (16-byte duration).
    Interval = 1186,
    /// `uuid` (16-byte UUID).
    Uuid = 2950,
    /// `json` (text-format JSON).
    Json = 114,
    /// `jsonb` (binary-format JSON, recommended for new schemas).
    Jsonb = 3802,
    /// `inet` (IPv4 or IPv6 host or network address).
    Inet = 869,
    /// `cidr` (IPv4 or IPv6 network address).
    Cidr = 650,
    /// `numeric` / `decimal` (arbitrary-precision exact decimal).
    Numeric = 1700,
    // Array types
    /// `bool[]`.
    BoolArray = 1000,
    /// `bytea[]`.
    ByteaArray = 1001,
    /// `int2[]`.
    Int2Array = 1005,
    /// `int4[]`.
    Int4Array = 1007,
    /// `text[]`.
    TextArray = 1009,
    /// `varchar[]`.
    VarcharArray = 1015,
    /// `int8[]`.
    Int8Array = 1016,
    /// `float4[]`.
    Float4Array = 1021,
    /// `float8[]`.
    Float8Array = 1022,
    /// `inet[]`.
    InetArray = 1041,
    /// `timestamp[]`.
    TimestampArray = 1115,
    /// `date[]`.
    DateArray = 1182,
    /// `time[]`.
    TimeArray = 1183,
    /// `timestamptz[]`.
    TimestamptzArray = 1185,
    /// `numeric[]`.
    NumericArray = 1231,
    /// `uuid[]`.
    UuidArray = 2951,
    /// `jsonb[]`.
    JsonbArray = 3807,
}

impl TypeOid {
    /// Convert this variant to its raw `pg_type.oid` value.
    pub fn as_u32(self) -> u32 {
        self as u32
    }

    /// Try to convert a raw u32 OID to a known TypeOid variant.
    pub fn from_u32(oid: u32) -> Option<Self> {
        Some(match oid {
            0 => Self::Unspecified,
            16 => Self::Bool,
            17 => Self::Bytea,
            18 => Self::Char,
            19 => Self::Name,
            20 => Self::Int8,
            21 => Self::Int2,
            23 => Self::Int4,
            25 => Self::Text,
            26 => Self::Oid,
            114 => Self::Json,
            650 => Self::Cidr,
            700 => Self::Float4,
            701 => Self::Float8,
            869 => Self::Inet,
            1000 => Self::BoolArray,
            1001 => Self::ByteaArray,
            1005 => Self::Int2Array,
            1007 => Self::Int4Array,
            1009 => Self::TextArray,
            1015 => Self::VarcharArray,
            1016 => Self::Int8Array,
            1021 => Self::Float4Array,
            1022 => Self::Float8Array,
            1041 => Self::InetArray,
            1043 => Self::Varchar,
            1082 => Self::Date,
            1083 => Self::Time,
            1114 => Self::Timestamp,
            1115 => Self::TimestampArray,
            1182 => Self::DateArray,
            1183 => Self::TimeArray,
            1184 => Self::Timestamptz,
            1185 => Self::TimestamptzArray,
            1186 => Self::Interval,
            1231 => Self::NumericArray,
            1700 => Self::Numeric,
            2950 => Self::Uuid,
            2951 => Self::UuidArray,
            3802 => Self::Jsonb,
            3807 => Self::JsonbArray,
            _ => return None,
        })
    }
}

impl From<TypeOid> for u32 {
    fn from(oid: TypeOid) -> u32 {
        oid as u32
    }
}
