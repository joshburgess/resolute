//! Derive macro for PostgreSQL composite types.
//!
//! ```ignore
//! #[derive(PgComposite)]
//! struct Address {
//!     street: String,
//!     city: String,
//!     #[pg_type(rename = "zip_code")]
//!     zip: String,
//!     notes: Option<String>,
//! }
//! ```

use proc_macro::TokenStream;
use quote::quote;
use syn::{Data, DeriveInput, Fields};

pub fn derive(input: DeriveInput) -> TokenStream {
    let name = &input.ident;
    let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();

    let fields = match &input.data {
        Data::Struct(data) => match &data.fields {
            Fields::Named(fields) => &fields.named,
            _ => panic!("PgComposite only supports structs with named fields"),
        },
        _ => panic!("PgComposite only supports structs"),
    };

    let field_count = fields.len() as i32;
    let name_str = name.to_string();

    // -- Encode --
    let encode_fields: Vec<_> = fields
        .iter()
        .map(|f| {
            let field_name = f.ident.as_ref().unwrap();
            let field_type = &f.ty;

            if let Some(inner_type) = extract_option_inner(field_type) {
                quote! {
                    buf.extend_from_slice(&(<#inner_type as resolute::PgType>::OID).to_be_bytes());
                    match &self.#field_name {
                        Some(v) => resolute::Encode::encode_param(v, buf),
                        None => buf.extend_from_slice(&(-1i32).to_be_bytes()),
                    }
                }
            } else {
                quote! {
                    buf.extend_from_slice(&(<#field_type as resolute::PgType>::OID).to_be_bytes());
                    resolute::Encode::encode_param(&self.#field_name, buf);
                }
            }
        })
        .collect();

    // -- Decode --
    let decode_fields: Vec<_> = fields
        .iter()
        .enumerate()
        .map(|(idx, f)| {
            let field_name = f.ident.as_ref().unwrap();
            let field_type = &f.ty;

            let read_header = quote! {
                if __offset + 8 > buf.len() {
                    return Err(resolute::TypedError::Decode {
                        column: #idx,
                        message: format!("{}: truncated field header", #name_str),
                    });
                }
                let _oid = u32::from_be_bytes([
                    buf[__offset], buf[__offset + 1], buf[__offset + 2], buf[__offset + 3],
                ]);
                __offset += 4;
                let __field_len = i32::from_be_bytes([
                    buf[__offset], buf[__offset + 1], buf[__offset + 2], buf[__offset + 3],
                ]);
                __offset += 4;
            };

            if let Some(inner_type) = extract_option_inner(field_type) {
                quote! {
                    #read_header
                    let #field_name = if __field_len == -1 {
                        None
                    } else {
                        let __l = __field_len as usize;
                        if __offset + __l > buf.len() {
                            return Err(resolute::TypedError::Decode {
                                column: #idx,
                                message: format!("{}: field data truncated", #name_str),
                            });
                        }
                        let __val = <#inner_type as resolute::Decode>::decode(
                            &buf[__offset..__offset + __l],
                        )?;
                        __offset += __l;
                        Some(__val)
                    };
                }
            } else {
                quote! {
                    #read_header
                    let #field_name = if __field_len == -1 {
                        return Err(resolute::TypedError::UnexpectedNull(#idx));
                    } else {
                        let __l = __field_len as usize;
                        if __offset + __l > buf.len() {
                            return Err(resolute::TypedError::Decode {
                                column: #idx,
                                message: format!("{}: field data truncated", #name_str),
                            });
                        }
                        let __val = <#field_type as resolute::Decode>::decode(
                            &buf[__offset..__offset + __l],
                        )?;
                        __offset += __l;
                        __val
                    };
                }
            }
        })
        .collect();

    let field_names: Vec<_> = fields.iter().map(|f| f.ident.as_ref().unwrap()).collect();

    let expanded = quote! {
        impl #impl_generics resolute::Encode for #name #ty_generics #where_clause {
            fn type_oid(&self) -> resolute::TypeOid {
                resolute::TypeOid::Unspecified
            }

            fn encode(&self, buf: &mut resolute::BytesMut) {
                // Composite binary format: nfields(i32), then per field: oid(u32) + len(i32) + data.
                buf.extend_from_slice(&(#field_count).to_be_bytes());
                #(#encode_fields)*
            }
        }

        impl #impl_generics resolute::Decode for #name #ty_generics #where_clause {
            fn decode(buf: &[u8]) -> Result<Self, resolute::TypedError> {
                if buf.len() < 4 {
                    return Err(resolute::TypedError::Decode {
                        column: 0,
                        message: format!("{}: buffer too short for composite header", #name_str),
                    });
                }
                let _nfields = i32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]);
                let mut __offset: usize = 4;
                #(#decode_fields)*
                Ok(Self { #(#field_names,)* })
            }
        }

        impl #impl_generics resolute::DecodeText for #name #ty_generics #where_clause {
            fn decode_text(_s: &str) -> Result<Self, resolute::TypedError> {
                Err(resolute::TypedError::Decode {
                    column: 0,
                    message: format!(
                        "text-format decoding not supported for composite type {}",
                        #name_str,
                    ),
                })
            }
        }

        impl #impl_generics resolute::PgType for #name #ty_generics #where_clause {
            const OID: u32 = 0;
            const ARRAY_OID: u32 = 0;
        }
    };

    TokenStream::from(expanded)
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Extract the inner type `T` from `Option<T>`, or `None` if not an Option.
fn extract_option_inner(ty: &syn::Type) -> Option<&syn::Type> {
    if let syn::Type::Path(type_path) = ty {
        if let Some(seg) = type_path.path.segments.last() {
            if seg.ident == "Option" {
                if let syn::PathArguments::AngleBracketed(args) = &seg.arguments {
                    if let Some(syn::GenericArgument::Type(inner)) = args.args.first() {
                        return Some(inner);
                    }
                }
            }
        }
    }
    None
}
