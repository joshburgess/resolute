//! Derive macros for resolute: `FromRow`, `PgEnum`, `PgComposite`, `PgDomain`.

mod pg_enum;
mod pg_composite;
mod pg_domain;

use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, DeriveInput, Data, Fields, LitStr};

/// Derive `FromRow` for structs with named fields.
///
/// # Field attributes
///
/// - `#[from_row(rename = "col")]` — use a different column name
/// - `#[from_row(skip)]` — skip the field, use `Default::default()`
/// - `#[from_row(default)]` — use `Default::default()` if column is NULL or missing
/// - `#[from_row(json)]` — deserialize a JSON/JSONB column via serde
/// - `#[from_row(try_from = "SourceType")]` — decode as SourceType, then `TryFrom` convert
/// - `#[from_row(flatten)]` — call `FromRow::from_row` on a nested struct
#[proc_macro_derive(FromRow, attributes(from_row))]
pub fn derive_from_row(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let name = &input.ident;
    let generics = &input.generics;
    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();

    let fields = match &input.data {
        Data::Struct(data) => match &data.fields {
            Fields::Named(fields) => &fields.named,
            _ => panic!("FromRow only supports structs with named fields"),
        },
        _ => panic!("FromRow only supports structs"),
    };

    let field_extractions = fields.iter().map(|field| {
        let field_name = field.ident.as_ref().unwrap();
        let field_type = &field.ty;
        let attrs = FromRowFieldAttrs::parse(field);

        let col_name = attrs.rename
            .unwrap_or_else(|| field_name.to_string());

        if attrs.skip {
            return quote! { #field_name: Default::default() };
        }

        if attrs.flatten {
            return quote! {
                #field_name: <#field_type as resolute::FromRow>::from_row(row)?
            };
        }

        if let Some(ref source_type) = attrs.try_from {
            if is_option_type(field_type) {
                return quote! {
                    #field_name: {
                        let __opt: Option<#source_type> = row.get_opt_by_name(#col_name)?;
                        match __opt {
                            Some(__src) => Some(
                                <_ as std::convert::TryFrom<#source_type>>::try_from(__src)
                                    .map_err(|e| resolute::TypedError::Decode {
                                        column: 0,
                                        message: format!("try_from({}): {}", #col_name, e),
                                    })?
                            ),
                            None => None,
                        }
                    }
                };
            } else {
                return quote! {
                    #field_name: {
                        let __src: #source_type = row.get_by_name(#col_name)?;
                        <#field_type as std::convert::TryFrom<#source_type>>::try_from(__src)
                            .map_err(|e| resolute::TypedError::Decode {
                                column: 0,
                                message: format!("try_from({}): {}", #col_name, e),
                            })?
                    }
                };
            }
        }

        if attrs.json {
            if is_option_type(field_type) {
                return quote! {
                    #field_name: {
                        let __opt: Option<serde_json::Value> = row.get_opt_by_name(#col_name)?;
                        match __opt {
                            Some(__v) => Some(
                                serde_json::from_value(__v).map_err(|e| resolute::TypedError::Decode {
                                    column: 0,
                                    message: format!("json({}): {}", #col_name, e),
                                })?
                            ),
                            None => None,
                        }
                    }
                };
            } else {
                return quote! {
                    #field_name: {
                        let __v: serde_json::Value = row.get_by_name(#col_name)?;
                        serde_json::from_value(__v).map_err(|e| resolute::TypedError::Decode {
                            column: 0,
                            message: format!("json({}): {}", #col_name, e),
                        })?
                    }
                };
            }
        }

        if attrs.default {
            if is_option_type(field_type) {
                return quote! {
                    #field_name: if row.has_column(#col_name) {
                        row.get_opt_by_name(#col_name)?
                    } else {
                        None
                    }
                };
            } else {
                return quote! {
                    #field_name: if row.has_column(#col_name) {
                        match row.get_by_name(#col_name) {
                            Ok(v) => v,
                            Err(resolute::TypedError::UnexpectedNull(_)) => Default::default(),
                            Err(e) => return Err(e),
                        }
                    } else {
                        Default::default()
                    }
                };
            }
        }

        // Normal field — no special attributes.
        if is_option_type(field_type) {
            quote! { #field_name: row.get_opt_by_name(#col_name)? }
        } else {
            quote! { #field_name: row.get_by_name(#col_name)? }
        }
    });

    let expanded = quote! {
        impl #impl_generics resolute::FromRow for #name #ty_generics #where_clause {
            fn from_row(row: &resolute::Row) -> Result<Self, resolute::TypedError> {
                Ok(Self {
                    #(#field_extractions,)*
                })
            }
        }
    };

    TokenStream::from(expanded)
}

// ---------------------------------------------------------------------------
// FromRow attribute parsing
// ---------------------------------------------------------------------------

/// Parsed attributes for a single field in `#[derive(FromRow)]`.
struct FromRowFieldAttrs {
    rename: Option<String>,
    skip: bool,
    default: bool,
    json: bool,
    try_from: Option<syn::Type>,
    flatten: bool,
}

impl FromRowFieldAttrs {
    fn parse(field: &syn::Field) -> Self {
        let mut attrs = Self {
            rename: None,
            skip: false,
            default: false,
            json: false,
            try_from: None,
            flatten: false,
        };

        for attr in &field.attrs {
            if !attr.path().is_ident("from_row") {
                continue;
            }
            attr.parse_nested_meta(|meta| {
                if meta.path.is_ident("rename") {
                    let value = meta.value()?;
                    let s: LitStr = value.parse()?;
                    attrs.rename = Some(s.value());
                } else if meta.path.is_ident("skip") {
                    attrs.skip = true;
                } else if meta.path.is_ident("default") {
                    attrs.default = true;
                } else if meta.path.is_ident("json") {
                    attrs.json = true;
                } else if meta.path.is_ident("try_from") {
                    let value = meta.value()?;
                    let s: LitStr = value.parse()?;
                    let ty: syn::Type = syn::parse_str(&s.value())
                        .expect("from_row(try_from = \"...\") must be a valid Rust type");
                    attrs.try_from = Some(ty);
                } else if meta.path.is_ident("flatten") {
                    attrs.flatten = true;
                }
                Ok(())
            }).ok();
        }

        // Validate incompatible combinations.
        if attrs.skip && (attrs.rename.is_some() || attrs.default || attrs.json || attrs.try_from.is_some() || attrs.flatten) {
            panic!("from_row(skip) cannot be combined with other attributes");
        }
        if attrs.flatten && (attrs.rename.is_some() || attrs.json || attrs.try_from.is_some()) {
            panic!("from_row(flatten) cannot be combined with rename, json, or try_from");
        }
        if attrs.json && attrs.try_from.is_some() {
            panic!("from_row(json) cannot be combined with try_from");
        }

        attrs
    }
}

/// Check if a type is `Option<T>`.
fn is_option_type(ty: &syn::Type) -> bool {
    if let syn::Type::Path(type_path) = ty {
        if let Some(seg) = type_path.path.segments.last() {
            return seg.ident == "Option";
        }
    }
    false
}

/// Derive `Encode`, `Decode`, `DecodeText`, and `PgType` for a Rust enum
/// representing a PostgreSQL enum type.
#[proc_macro_derive(PgEnum, attributes(pg_type))]
pub fn derive_pg_enum(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    pg_enum::derive(input)
}

/// Derive `Encode`, `Decode`, `DecodeText`, and `PgType` for a Rust struct
/// representing a PostgreSQL composite type.
#[proc_macro_derive(PgComposite, attributes(pg_type))]
pub fn derive_pg_composite(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    pg_composite::derive(input)
}

/// Derive `Encode`, `Decode`, `DecodeText`, and `PgType` for a newtype struct
/// representing a PostgreSQL domain type.
#[proc_macro_derive(PgDomain, attributes(pg_type))]
pub fn derive_pg_domain(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    pg_domain::derive(input)
}

/// Attribute macro for database-backed tests.
///
/// Creates a temporary database, optionally runs migrations, provides a
/// `Client` argument, and drops the database after the test completes.
///
/// ```ignore
/// #[resolute::test]
/// async fn my_test(client: resolute::Client) {
///     client.simple_query("CREATE TABLE t (id int)").await.unwrap();
///     client.execute("INSERT INTO t VALUES ($1)", &[&1i32]).await.unwrap();
/// }
///
/// #[resolute::test(migrations = "migrations")]
/// async fn with_migrations(client: resolute::Client) {
///     // migrations have already been applied
/// }
/// ```
#[proc_macro_attribute]
pub fn test(attr: TokenStream, item: TokenStream) -> TokenStream {
    let attr_str = attr.to_string();
    let input_fn = parse_macro_input!(item as syn::ItemFn);
    let fn_name = &input_fn.sig.ident;
    let fn_block = &input_fn.block;
    let fn_vis = &input_fn.vis;
    let fn_attrs = &input_fn.attrs;

    // Parse optional migrations path from attribute.
    let migrations = if attr_str.contains("migrations") {
        // Extract: migrations = "path"
        let path = attr_str
            .split('"')
            .nth(1)
            .unwrap_or("migrations");
        Some(path.to_string())
    } else {
        None
    };

    // Default connection params from env or hardcoded defaults.
    let create_db = if let Some(mig_path) = &migrations {
        quote! {
            let __test_db = resolute::test_db::TestDb::create_with_migrations(
                &__addr, &__user, &__pass, #mig_path,
            ).await.expect("failed to create test database");
        }
    } else {
        quote! {
            let __test_db = resolute::test_db::TestDb::create(
                &__addr, &__user, &__pass,
            ).await.expect("failed to create test database");
        }
    };

    let expanded = quote! {
        #(#fn_attrs)*
        #[tokio::test]
        #fn_vis async fn #fn_name() {
            let __addr = std::env::var("PG_TEST_ADDR").unwrap_or_else(|_| "127.0.0.1:54322".into());
            let __user = std::env::var("PG_TEST_USER").unwrap_or_else(|_| "postgres".into());
            let __pass = std::env::var("PG_TEST_PASS").unwrap_or_else(|_| "postgres".into());

            #create_db

            let client = __test_db.client().await.expect("failed to connect to test database");

            // Run the user's test body.
            let __result = async { #fn_block }.await;

            // Cleanup: drop the test database.
            drop(client);
            let _ = __test_db.drop_db().await;
        }
    };

    TokenStream::from(expanded)
}

