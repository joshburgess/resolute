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
    let name = &input.ident;
    let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();

    let inner_type = match &input.data {
        Data::Struct(data) => match &data.fields {
            Fields::Unnamed(fields) if fields.unnamed.len() == 1 => {
                &fields.unnamed.first().unwrap().ty
            }
            _ => panic!(
                "PgDomain requires a tuple struct with exactly one field, e.g. `struct {}(String)`",
                name
            ),
        },
        _ => panic!("PgDomain only supports tuple structs"),
    };

    let expanded = quote! {
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
            const OID: u32 = 0;
            const ARRAY_OID: u32 = <#inner_type as resolute::PgType>::ARRAY_OID;
        }

    };

    TokenStream::from(expanded)
}
