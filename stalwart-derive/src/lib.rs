//! Derive macros for stalwart: `FromRow`, `PgEnum`, `PgComposite`, `PgDomain`.

mod pg_enum;
mod pg_composite;
mod pg_domain;

use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, DeriveInput, Data, Fields, LitStr};

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

        // Check for #[from_row(rename = "...")] attribute.
        let col_name = get_rename_attr(field)
            .unwrap_or_else(|| field_name.to_string());

        // Check if the field type is Option<T>.
        if is_option_type(field_type) {
            quote! {
                #field_name: row.get_opt_by_name(#col_name)?
            }
        } else {
            quote! {
                #field_name: row.get_by_name(#col_name)?
            }
        }
    });

    let expanded = quote! {
        impl #impl_generics stalwart::FromRow for #name #ty_generics #where_clause {
            fn from_row(row: &stalwart::Row) -> Result<Self, stalwart::TypedError> {
                Ok(Self {
                    #(#field_extractions,)*
                })
            }
        }
    };

    TokenStream::from(expanded)
}

/// Extract the `rename = "..."` value from `#[from_row(rename = "...")]`.
fn get_rename_attr(field: &syn::Field) -> Option<String> {
    for attr in &field.attrs {
        if !attr.path().is_ident("from_row") {
            continue;
        }
        let mut rename_value = None;
        attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("rename") {
                let value = meta.value()?;
                let s: LitStr = value.parse()?;
                rename_value = Some(s.value());
            }
            Ok(())
        }).ok();
        if let Some(v) = rename_value {
            return Some(v);
        }
    }
    None
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
/// #[stalwart::test]
/// async fn my_test(client: stalwart::Client) {
///     client.simple_query("CREATE TABLE t (id int)").await.unwrap();
///     client.execute("INSERT INTO t VALUES ($1)", &[&1i32]).await.unwrap();
/// }
///
/// #[stalwart::test(migrations = "migrations")]
/// async fn with_migrations(client: stalwart::Client) {
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
            let __test_db = stalwart::test_db::TestDb::create_with_migrations(
                &__addr, &__user, &__pass, #mig_path,
            ).await.expect("failed to create test database");
        }
    } else {
        quote! {
            let __test_db = stalwart::test_db::TestDb::create(
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

