//! Derive macro for PostgreSQL enum types.
//!
//! String-based enums (PostgreSQL CREATE TYPE ... AS ENUM):
//! ```ignore
//! #[derive(PgEnum)]
//! #[pg_type(rename_all = "snake_case")]  // default
//! enum Mood {
//!     Happy,
//!     Sad,
//!     #[pg_type(rename = "so-so")]
//!     SoSo,
//! }
//! ```
//!
//! Integer-backed enums (stored as int2/int4/int8 in PostgreSQL):
//! ```ignore
//! #[derive(PgEnum)]
//! #[repr(i32)]
//! enum Status {
//!     Active = 1,
//!     Inactive = 2,
//!     Deleted = 3,
//! }
//! ```

use proc_macro::TokenStream;
use quote::quote;
use syn::{Data, DeriveInput, LitStr};

pub fn derive(input: DeriveInput) -> TokenStream {
    let variants = match &input.data {
        Data::Enum(data) => &data.variants,
        _ => panic!("PgEnum can only be derived for enums"),
    };

    // Validate: all variants must be unit (no fields).
    for v in variants {
        if !v.fields.is_empty() {
            panic!(
                "PgEnum: variant `{}` has fields — only unit variants are supported",
                v.ident
            );
        }
    }

    if let Some(repr) = get_repr_int(&input.attrs) {
        derive_int_enum(&input, &repr)
    } else {
        derive_string_enum(&input)
    }
}

/// Generate impls for string-based PostgreSQL enum types.
fn derive_string_enum(input: &DeriveInput) -> TokenStream {
    let name = &input.ident;
    let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();

    let variants = match &input.data {
        Data::Enum(data) => &data.variants,
        _ => unreachable!(),
    };

    let rename_all = get_container_rename_all(&input.attrs);
    let (custom_oid, custom_array_oid) = get_custom_oids(&input.attrs);

    let encode_arms: Vec<_> = variants
        .iter()
        .map(|v| {
            let ident = &v.ident;
            let label = get_variant_label(v, &rename_all);
            quote! { #name::#ident => #label }
        })
        .collect();

    let decode_arms: Vec<_> = variants
        .iter()
        .map(|v| {
            let ident = &v.ident;
            let label = get_variant_label(v, &rename_all);
            quote! { #label => Ok(#name::#ident) }
        })
        .collect();

    let name_str = name.to_string();

    let expanded = quote! {
        impl #impl_generics resolute::Encode for #name #ty_generics #where_clause {
            fn type_oid(&self) -> resolute::TypeOid {
                resolute::TypeOid::Unspecified
            }

            fn encode(&self, buf: &mut resolute::BytesMut) {
                let label: &str = match self {
                    #(#encode_arms,)*
                };
                buf.extend_from_slice(label.as_bytes());
            }
        }

        impl #impl_generics resolute::Decode for #name #ty_generics #where_clause {
            fn decode(buf: &[u8]) -> Result<Self, resolute::TypedError> {
                let s = std::str::from_utf8(buf).map_err(|e| resolute::TypedError::Decode {
                    column: 0,
                    message: format!("enum: invalid UTF-8: {e}"),
                })?;
                match s {
                    #(#decode_arms,)*
                    other => Err(resolute::TypedError::Decode {
                        column: 0,
                        message: format!("unknown {} variant: {:?}", #name_str, other),
                    }),
                }
            }
        }

        impl #impl_generics resolute::DecodeText for #name #ty_generics #where_clause {
            fn decode_text(s: &str) -> Result<Self, resolute::TypedError> {
                match s {
                    #(#decode_arms,)*
                    other => Err(resolute::TypedError::Decode {
                        column: 0,
                        message: format!("unknown {} variant: {:?}", #name_str, other),
                    }),
                }
            }
        }

        impl #impl_generics resolute::PgType for #name #ty_generics #where_clause {
            const OID: u32 = #custom_oid;
            const ARRAY_OID: u32 = #custom_array_oid;
        }
    };

    TokenStream::from(expanded)
}

/// Generate impls for integer-backed enums (`#[repr(i16/i32/i64)]`).
fn derive_int_enum(input: &DeriveInput, repr: &str) -> TokenStream {
    let name = &input.ident;
    let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();

    let variants = match &input.data {
        Data::Enum(data) => &data.variants,
        _ => unreachable!(),
    };

    let repr_type: proc_macro2::TokenStream = repr.parse().unwrap();
    let name_str = name.to_string();

    // Determine OIDs based on repr type, with custom overrides.
    let (default_oid, default_array_oid, byte_len) = match repr {
        "i16" => (21u32, 1005u32, 2usize),
        "i32" => (23u32, 1007u32, 4usize),
        "i64" => (20u32, 1016u32, 8usize),
        _ => unreachable!(),
    };
    let (custom_oid, custom_array_oid) = get_custom_oids(&input.attrs);
    let oid = if custom_oid != 0 { custom_oid } else { default_oid };
    let array_oid = if custom_array_oid != 0 { custom_array_oid } else { default_array_oid };

    // Validate all variants have explicit discriminants.
    for v in variants {
        if v.discriminant.is_none() {
            panic!(
                "PgEnum with #[repr({repr})]: variant `{}` must have an explicit discriminant (e.g., = 1)",
                v.ident
            );
        }
    }

    let encode_arms: Vec<_> = variants
        .iter()
        .map(|v| {
            let ident = &v.ident;
            let (_, expr) = v.discriminant.as_ref().unwrap();
            quote! { #name::#ident => (#expr) as #repr_type }
        })
        .collect();

    let decode_arms: Vec<_> = variants
        .iter()
        .map(|v| {
            let ident = &v.ident;
            let (_, expr) = v.discriminant.as_ref().unwrap();
            quote! { x if x == (#expr) as #repr_type => Ok(#name::#ident) }
        })
        .collect();

    let decode_text_arms: Vec<_> = variants
        .iter()
        .map(|v| {
            let ident = &v.ident;
            let (_, expr) = v.discriminant.as_ref().unwrap();
            quote! { x if x == (#expr) as #repr_type => Ok(#name::#ident) }
        })
        .collect();

    let expanded = quote! {
        impl #impl_generics resolute::Encode for #name #ty_generics #where_clause {
            fn type_oid(&self) -> resolute::TypeOid {
                <#repr_type as resolute::Encode>::type_oid(&(0 as #repr_type))
            }

            fn encode(&self, buf: &mut resolute::BytesMut) {
                let val: #repr_type = match self {
                    #(#encode_arms,)*
                };
                resolute::Encode::encode(&val, buf);
            }
        }

        impl #impl_generics resolute::Decode for #name #ty_generics #where_clause {
            fn decode(buf: &[u8]) -> Result<Self, resolute::TypedError> {
                if buf.len() < #byte_len {
                    return Err(resolute::TypedError::Decode {
                        column: 0,
                        message: format!("{}: expected {} bytes, got {}", #name_str, #byte_len, buf.len()),
                    });
                }
                let val = <#repr_type as resolute::Decode>::decode(buf)?;
                match val {
                    #(#decode_arms,)*
                    other => Err(resolute::TypedError::Decode {
                        column: 0,
                        message: format!("unknown {} discriminant: {}", #name_str, other),
                    }),
                }
            }
        }

        impl #impl_generics resolute::DecodeText for #name #ty_generics #where_clause {
            fn decode_text(s: &str) -> Result<Self, resolute::TypedError> {
                let val: #repr_type = s.parse().map_err(|e| resolute::TypedError::Decode {
                    column: 0,
                    message: format!("{}: failed to parse integer: {}", #name_str, e),
                })?;
                match val {
                    #(#decode_text_arms,)*
                    other => Err(resolute::TypedError::Decode {
                        column: 0,
                        message: format!("unknown {} discriminant: {}", #name_str, other),
                    }),
                }
            }
        }

        impl #impl_generics resolute::PgType for #name #ty_generics #where_clause {
            const OID: u32 = #oid;
            const ARRAY_OID: u32 = #array_oid;
        }
    };

    TokenStream::from(expanded)
}

/// Parse optional `#[pg_type(oid = N)]` and `#[pg_type(array_oid = N)]`.
/// Returns (oid, array_oid) with defaults of 0 if not specified.
fn get_custom_oids(attrs: &[syn::Attribute]) -> (u32, u32) {
    let mut oid: u32 = 0;
    let mut array_oid: u32 = 0;
    for attr in attrs {
        if !attr.path().is_ident("pg_type") {
            continue;
        }
        let _ = attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("oid") {
                let value = meta.value()?;
                let lit: syn::LitInt = value.parse()?;
                oid = lit.base10_parse().unwrap_or(0);
            } else if meta.path.is_ident("array_oid") {
                let value = meta.value()?;
                let lit: syn::LitInt = value.parse()?;
                array_oid = lit.base10_parse().unwrap_or(0);
            }
            Ok(())
        });
    }
    (oid, array_oid)
}

/// Check for `#[repr(i16)]`, `#[repr(i32)]`, or `#[repr(i64)]`.
fn get_repr_int(attrs: &[syn::Attribute]) -> Option<String> {
    for attr in attrs {
        if !attr.path().is_ident("repr") {
            continue;
        }
        let mut repr_type = None;
        let _ = attr.parse_nested_meta(|meta| {
            for candidate in &["i16", "i32", "i64"] {
                if meta.path.is_ident(candidate) {
                    repr_type = Some(candidate.to_string());
                }
            }
            Ok(())
        });
        if let Some(r) = repr_type {
            return Some(r);
        }
    }
    None
}

// ---------------------------------------------------------------------------
// Attribute helpers
// ---------------------------------------------------------------------------

fn get_container_rename_all(attrs: &[syn::Attribute]) -> String {
    for attr in attrs {
        if !attr.path().is_ident("pg_type") {
            continue;
        }
        let mut value = None;
        let _ = attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("rename_all") {
                let v = meta.value()?;
                let s: LitStr = v.parse()?;
                value = Some(s.value());
            }
            Ok(())
        });
        if let Some(v) = value {
            return v;
        }
    }
    "snake_case".to_string()
}

fn get_variant_label(variant: &syn::Variant, rename_all: &str) -> String {
    // Check for per-variant #[pg_type(rename = "...")].
    for attr in &variant.attrs {
        if !attr.path().is_ident("pg_type") {
            continue;
        }
        let mut value = None;
        let _ = attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("rename") {
                let v = meta.value()?;
                let s: LitStr = v.parse()?;
                value = Some(s.value());
            }
            Ok(())
        });
        if let Some(v) = value {
            return v;
        }
    }
    apply_rename_rule(&variant.ident.to_string(), rename_all)
}

fn apply_rename_rule(name: &str, rule: &str) -> String {
    match rule {
        "snake_case" => to_snake_case(name),
        "lowercase" => name.to_lowercase(),
        "UPPERCASE" => name.to_uppercase(),
        "SCREAMING_SNAKE_CASE" => to_snake_case(name).to_uppercase(),
        "camelCase" => {
            let s = to_snake_case(name);
            let mut out = String::new();
            let mut capitalize_next = false;
            for (i, c) in s.chars().enumerate() {
                if c == '_' {
                    capitalize_next = true;
                } else if capitalize_next {
                    out.extend(c.to_uppercase());
                    capitalize_next = false;
                } else if i == 0 {
                    out.extend(c.to_lowercase());
                } else {
                    out.push(c);
                }
            }
            out
        }
        "PascalCase" => name.to_string(),
        "kebab-case" => to_snake_case(name).replace('_', "-"),
        _ => to_snake_case(name),
    }
}

fn to_snake_case(s: &str) -> String {
    let mut result = String::with_capacity(s.len() + 4);
    let chars: Vec<char> = s.chars().collect();
    for (i, &c) in chars.iter().enumerate() {
        if c.is_uppercase() {
            if i > 0 {
                let prev = chars[i - 1];
                if prev.is_lowercase()
                    || (prev.is_uppercase()
                        && i + 1 < chars.len()
                        && chars[i + 1].is_lowercase())
                {
                    result.push('_');
                }
            }
            result.extend(c.to_lowercase());
        } else {
            result.push(c);
        }
    }
    result
}
