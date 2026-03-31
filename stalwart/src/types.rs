//! Type metadata and registry.

/// Metadata about a PostgreSQL type.
#[derive(Debug, Clone)]
pub struct TypeInfo {
    pub oid: u32,
    pub name: &'static str,
    pub size: i16, // -1 = variable length
}

/// Look up type info by OID.
#[allow(dead_code)]
pub fn type_info(oid: u32) -> Option<TypeInfo> {
    match oid {
        16 => Some(TypeInfo { oid: 16, name: "bool", size: 1 }),
        17 => Some(TypeInfo { oid: 17, name: "bytea", size: -1 }),
        18 => Some(TypeInfo { oid: 18, name: "char", size: 1 }),
        19 => Some(TypeInfo { oid: 19, name: "name", size: 64 }),
        20 => Some(TypeInfo { oid: 20, name: "int8", size: 8 }),
        21 => Some(TypeInfo { oid: 21, name: "int2", size: 2 }),
        23 => Some(TypeInfo { oid: 23, name: "int4", size: 4 }),
        25 => Some(TypeInfo { oid: 25, name: "text", size: -1 }),
        26 => Some(TypeInfo { oid: 26, name: "oid", size: 4 }),
        114 => Some(TypeInfo { oid: 114, name: "json", size: -1 }),
        700 => Some(TypeInfo { oid: 700, name: "float4", size: 4 }),
        701 => Some(TypeInfo { oid: 701, name: "float8", size: 8 }),
        1043 => Some(TypeInfo { oid: 1043, name: "varchar", size: -1 }),
        1082 => Some(TypeInfo { oid: 1082, name: "date", size: 4 }),
        1083 => Some(TypeInfo { oid: 1083, name: "time", size: 8 }),
        1114 => Some(TypeInfo { oid: 1114, name: "timestamp", size: 8 }),
        1184 => Some(TypeInfo { oid: 1184, name: "timestamptz", size: 8 }),
        1186 => Some(TypeInfo { oid: 1186, name: "interval", size: 16 }),
        2950 => Some(TypeInfo { oid: 2950, name: "uuid", size: 16 }),
        3802 => Some(TypeInfo { oid: 3802, name: "jsonb", size: -1 }),
        _ => None,
    }
}

/// Get the Rust-side type name for a PostgreSQL OID (for error messages).
#[allow(dead_code)]
pub fn rust_type_for_oid(oid: u32) -> &'static str {
    match oid {
        16 => "bool",
        20 => "i64",
        21 => "i16",
        23 => "i32",
        25 | 1043 => "String",
        700 => "f32",
        701 => "f64",
        17 => "Vec<u8>",
        _ => "unknown",
    }
}
