//! PostgreSQL type OID constants.
//!
//! These match the OIDs in pg_type. When sending Bind with binary params,
//! the OID tells PostgreSQL how to interpret the bytes.

/// Known PostgreSQL type OIDs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u32)]
pub enum TypeOid {
    /// OID 0: let the server infer the type from context.
    Unspecified = 0,
    Bool = 16,
    Bytea = 17,
    Int8 = 20,
    Int2 = 21,
    Int4 = 23,
    Text = 25,
    Oid = 26,
    Float4 = 700,
    Float8 = 701,
    Varchar = 1043,
    Char = 18,
    Name = 19,
    Timestamp = 1114,
    Timestamptz = 1184,
    Date = 1082,
    Time = 1083,
    Interval = 1186,
    Uuid = 2950,
    Json = 114,
    Jsonb = 3802,
    Inet = 869,
    Cidr = 650,
    Numeric = 1700,
    // Array types
    BoolArray = 1000,
    ByteaArray = 1001,
    Int2Array = 1005,
    Int4Array = 1007,
    TextArray = 1009,
    VarcharArray = 1015,
    Int8Array = 1016,
    Float4Array = 1021,
    Float8Array = 1022,
    InetArray = 1041,
    TimestampArray = 1115,
    DateArray = 1182,
    TimeArray = 1183,
    TimestamptzArray = 1185,
    NumericArray = 1231,
    UuidArray = 2951,
    JsonbArray = 3807,
}

impl TypeOid {
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
