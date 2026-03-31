//! Derive macro for PostgreSQL enum types.
//!
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

use proc_macro::TokenStream;
use quote::quote;
use syn::{Data, DeriveInput, LitStr};

pub fn derive(input: DeriveInput) -> TokenStream {
    let name = &input.ident;
    let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();

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

    let rename_all = get_container_rename_all(&input.attrs);

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
        impl #impl_generics stalwart::Encode for #name #ty_generics #where_clause {
            fn type_oid(&self) -> stalwart::TypeOid {
                stalwart::TypeOid::Unspecified
            }

            fn encode(&self, buf: &mut stalwart::BytesMut) {
                let label: &str = match self {
                    #(#encode_arms,)*
                };
                buf.extend_from_slice(label.as_bytes());
            }
        }

        impl #impl_generics stalwart::Decode for #name #ty_generics #where_clause {
            fn decode(buf: &[u8]) -> Result<Self, stalwart::TypedError> {
                let s = std::str::from_utf8(buf).map_err(|e| stalwart::TypedError::Decode {
                    column: 0,
                    message: format!("enum: invalid UTF-8: {e}"),
                })?;
                match s {
                    #(#decode_arms,)*
                    other => Err(stalwart::TypedError::Decode {
                        column: 0,
                        message: format!("unknown {} variant: {:?}", #name_str, other),
                    }),
                }
            }
        }

        impl #impl_generics stalwart::DecodeText for #name #ty_generics #where_clause {
            fn decode_text(s: &str) -> Result<Self, stalwart::TypedError> {
                match s {
                    #(#decode_arms,)*
                    other => Err(stalwart::TypedError::Decode {
                        column: 0,
                        message: format!("unknown {} variant: {:?}", #name_str, other),
                    }),
                }
            }
        }

        impl #impl_generics stalwart::PgType for #name #ty_generics #where_clause {
            const OID: u32 = 0;
            const ARRAY_OID: u32 = 0;
        }
    };

    TokenStream::from(expanded)
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
