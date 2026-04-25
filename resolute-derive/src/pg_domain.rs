//! Derive macro for PostgreSQL domain types (newtypes over a base type).
//!
//! ```ignore
//! #[derive(PgDomain)]
//! struct Email(String);
//! ```
//!
//! The inner type must implement Encode, Decode, DecodeText, and PgType.
//! All trait impls delegate to the inner type, with `type_oid` returning
//! `Unspecified` so the server infers the domain's OID from context.
//!
//! Array support is automatic: `Vec<Email>` works if the inner type has
//! a non-zero ARRAY_OID (e.g., String → text[] OID 1009).

use proc_macro::TokenStream;
use quote::quote;
use syn::{Data, DeriveInput, Fields};

pub fn derive(input: DeriveInput) -> TokenStream {
    match derive_inner(input) {
        Ok(tokens) => tokens.into(),
        Err(err) => err.to_compile_error().into(),
    }
}

fn derive_inner(input: DeriveInput) -> syn::Result<proc_macro2::TokenStream> {
    let name = &input.ident;
    let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();
    let (custom_oid, custom_array_oid) = get_custom_oids(&input.attrs)?;

    let inner_type = match &input.data {
        Data::Struct(data) => match &data.fields {
            Fields::Unnamed(fields) if fields.unnamed.len() == 1 => {
                &fields.unnamed.first().unwrap().ty
            }
            _ => {
                return Err(syn::Error::new_spanned(
                    &input,
                    format!(
                        "PgDomain requires a tuple struct with exactly one field, e.g. `struct {name}(String)`"
                    ),
                ));
            }
        },
        _ => {
            return Err(syn::Error::new_spanned(
                &input,
                "PgDomain only supports tuple structs",
            ));
        }
    };

    Ok(quote! {
        impl #impl_generics resolute::Encode for #name #ty_generics #where_clause {
            fn type_oid(&self) -> resolute::TypeOid {
                resolute::TypeOid::Unspecified
            }

            fn encode(&self, buf: &mut resolute::BytesMut) {
                resolute::Encode::encode(&self.0, buf);
            }
        }

        impl #impl_generics resolute::Decode for #name #ty_generics #where_clause {
            fn decode(buf: &[u8]) -> Result<Self, resolute::TypedError> {
                Ok(Self(<#inner_type as resolute::Decode>::decode(buf)?))
            }
        }

        impl #impl_generics resolute::DecodeText for #name #ty_generics #where_clause {
            fn decode_text(s: &str) -> Result<Self, resolute::TypedError> {
                Ok(Self(<#inner_type as resolute::DecodeText>::decode_text(s)?))
            }
        }

        impl #impl_generics resolute::PgType for #name #ty_generics #where_clause {
            const OID: u32 = #custom_oid;
            const ARRAY_OID: u32 = if #custom_array_oid != 0 {
                #custom_array_oid
            } else {
                <#inner_type as resolute::PgType>::ARRAY_OID
            };
        }

    })
}

/// Parse optional `#[pg_type(oid = N)]` and `#[pg_type(array_oid = N)]`.
fn get_custom_oids(attrs: &[syn::Attribute]) -> syn::Result<(u32, u32)> {
    let mut oid: u32 = 0;
    let mut array_oid: u32 = 0;
    for attr in attrs {
        if !attr.path().is_ident("pg_type") {
            continue;
        }
        attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("oid") {
                let value = meta.value()?;
                let lit: syn::LitInt = value.parse()?;
                oid = lit.base10_parse()?;
            } else if meta.path.is_ident("array_oid") {
                let value = meta.value()?;
                let lit: syn::LitInt = value.parse()?;
                array_oid = lit.base10_parse()?;
            } else {
                crate::consume_unknown_meta_value(&meta)?;
            }
            Ok(())
        })?;
    }
    Ok((oid, array_oid))
}
