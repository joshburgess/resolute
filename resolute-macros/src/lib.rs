//! Compile-time checked SQL query macros with offline cache support.
//!
//! Connects to PostgreSQL at compile time via pg-wired to validate SQL
//! and generate typed result structs.
//!
//! # Modes
//!
//! - **Online (default):** Connects to DB via `DATABASE_URL`, caches results to `.resolute/`
//! - **Offline:** Set `RESOLUTE_OFFLINE=true` to use cached metadata only (no DB needed)
//! - **Prepare:** Run `resolute-cli prepare` to populate the cache from source files
//!
//! ```ignore
//! let rows = resolute::query!("SELECT id, name FROM users WHERE id = $1", user_id)
//!     .fetch_all(&client)
//!     .await?;
//! ```

use proc_macro::TokenStream;
use quote::{format_ident, quote};
use syn::parse::{Parse, ParseStream};
use syn::{parse_macro_input, Expr, LitStr, Token};

mod cache;

/// Input to the query! macro: SQL literal + optional comma-separated params.
/// Supports both positional (`query!("... $1", val)`) and named (`query!("... :name", name = val)`) params.
struct QueryInput {
    sql: LitStr,
    params: Vec<Expr>,
    /// If named params were used, this holds (name, expr) pairs.
    named: Vec<(syn::Ident, Expr)>,
}

impl Parse for QueryInput {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let sql: LitStr = input.parse()?;
        let mut params = Vec::new();
        let mut named = Vec::new();
        let mut mode: Option<bool> = None; // None=unknown, Some(true)=named, Some(false)=positional

        while input.peek(Token![,]) {
            input.parse::<Token![,]>()?;
            if input.is_empty() {
                break;
            }

            // Try to detect named param: `ident = expr` (but not `ident == expr`)
            let is_named_param = {
                let fork = input.fork();
                fork.parse::<syn::Ident>().is_ok()
                    && fork.parse::<Token![=]>().is_ok()
                    && !fork.peek(Token![=])
            };

            if is_named_param && mode != Some(false) {
                let name: syn::Ident = input.parse()?;
                input.parse::<Token![=]>()?;
                let expr: Expr = input.parse()?;
                named.push((name, expr));
                mode = Some(true);
            } else if mode != Some(true) {
                params.push(input.parse()?);
                mode = Some(false);
            } else {
                return Err(input.error("cannot mix positional and named parameters"));
            }
        }

        Ok(QueryInput { sql, params, named })
    }
}

/// If named params are present, rewrite SQL and reorder params.
/// Otherwise pass through unchanged.
fn resolve_named(
    sql_str: String,
    params: Vec<Expr>,
    named: &[(syn::Ident, Expr)],
    sql_span: &LitStr,
) -> Result<(String, Vec<Expr>), TokenStream> {
    if named.is_empty() {
        return Ok((sql_str, params));
    }
    let (rewritten, names) = rewrite_named_params(&sql_str);
    let mut ordered = Vec::with_capacity(names.len());
    for name in &names {
        match named.iter().find(|(n, _)| n == name) {
            Some((_, expr)) => ordered.push(expr.clone()),
            None => {
                let msg = format!("named parameter `:{name}` in SQL has no binding");
                return Err(syn::Error::new_spanned(sql_span, msg)
                    .to_compile_error()
                    .into());
            }
        }
    }
    for (n, _) in named {
        if !names.iter().any(|name| name == &n.to_string()) {
            let msg = format!("binding `{}` does not match any `:{}` in SQL", n, n);
            return Err(syn::Error::new_spanned(n, msg).to_compile_error().into());
        }
    }
    Ok((rewritten, ordered))
}

/// Resolve query metadata: try cache first, then live DB, then update cache.
fn resolve_metadata(sql: &str) -> Result<(Vec<u32>, Vec<cache::CachedColumn>), String> {
    let sql_hash = hash_sql(sql);
    let offline = std::env::var("RESOLUTE_OFFLINE")
        .map(|v| v == "true" || v == "1")
        .unwrap_or(false);

    // 1. Try the cache first.
    if let Some(cached) = cache::read_cache(sql_hash) {
        return Ok((cached.param_oids, cached.columns));
    }

    // 2. If offline mode, fail — cache is required.
    if offline {
        return Err(format!(
            "RESOLUTE_OFFLINE=true but no cached metadata for query (hash {sql_hash:x}). \
             Run `resolute-cli prepare` to populate the cache."
        ));
    }

    // 3. Connect to PG and describe.
    let (param_oids, columns) = describe_live(sql)?;

    // 4. Write to cache for future offline builds.
    let entry = cache::CacheEntry {
        sql: sql.to_string(),
        hash: sql_hash,
        param_oids: param_oids.clone(),
        columns: columns.clone(),
    };
    if let Err(e) = cache::write_cache(&entry) {
        // Cache write failure is non-fatal — just warn.
        eprintln!("resolute: warning: failed to write cache: {e}");
    }

    Ok((param_oids, columns))
}

/// Connect to PG via pg-wired and describe the statement.
fn describe_live(sql: &str) -> Result<(Vec<u32>, Vec<cache::CachedColumn>), String> {
    let db_url = std::env::var("DATABASE_URL").map_err(|_| {
        "DATABASE_URL not set and no cached metadata found. \
         Set DATABASE_URL or run `resolute-cli prepare`."
            .to_string()
    })?;

    let (user, password, host, port, database) = parse_pg_uri(&db_url)
        .ok_or_else(|| "Invalid DATABASE_URL (could not parse as postgres:// URI)".to_string())?;
    let addr = format!("{host}:{port}");

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| format!("Failed to create tokio runtime: {e}"))?;

    rt.block_on(async {
        let mut conn = pg_wired::WireConn::connect(&addr, &user, &password, &database)
            .await
            .map_err(|e| format!("Failed to connect to database: {e}"))?;

        let (param_oids, fields) = conn
            .describe_statement(sql)
            .await
            .map_err(|e| format!("SQL error: {e}"))?;

        // Detect nullable columns by querying pg_attribute for real table columns.
        // Batch all table_oid/column_id pairs into one query.
        let mut columns: Vec<cache::CachedColumn> = fields
            .iter()
            .map(|f| cache::CachedColumn {
                name: f.name.clone(),
                type_oid: f.type_oid,
                nullable: true, // Default: assume nullable.
            })
            .collect();

        // Collect non-null info for columns that come from real tables.
        let table_cols: Vec<(usize, u32, i16)> = fields
            .iter()
            .enumerate()
            .filter(|(_, f)| f.table_oid != 0 && f.column_id > 0)
            .map(|(i, f)| (i, f.table_oid, f.column_id))
            .collect();

        if !table_cols.is_empty() {
            // Build a single query to check all columns at once.
            let conditions: Vec<String> = table_cols
                .iter()
                .map(|(_, oid, col)| format!("(attrelid={oid} AND attnum={col})"))
                .collect();
            let null_sql = format!(
                "SELECT attrelid, attnum, attnotnull FROM pg_attribute WHERE {}",
                conditions.join(" OR ")
            );

            // Send as simple query and collect rows.
            let mut buf = bytes::BytesMut::new();
            pg_wired::protocol::frontend::encode_message(
                &pg_wired::protocol::types::FrontendMsg::Query(null_sql.as_bytes()),
                &mut buf,
            );
            if conn.send_raw(&buf).await.is_ok() {
                if let Ok((rows, _)) = conn.collect_rows().await {
                    for row in &rows {
                        let oid: u32 = row
                            .cell(0)
                            .and_then(|b| std::str::from_utf8(b).ok())
                            .and_then(|s| s.parse().ok())
                            .unwrap_or(0);
                        let col: i16 = row
                            .cell(1)
                            .and_then(|b| std::str::from_utf8(b).ok())
                            .and_then(|s| s.parse().ok())
                            .unwrap_or(0);
                        let notnull: bool = row
                            .cell(2)
                            .map(|b| b == b"t".as_ref())
                            .unwrap_or(false);

                        // Find the matching column and mark it non-nullable.
                        for &(idx, t_oid, t_col) in &table_cols {
                            if t_oid == oid && t_col == col && notnull {
                                columns[idx].nullable = false;
                            }
                        }
                    }
                }
            }
        }

        Ok((param_oids, columns))
    })
}

/// Map a PostgreSQL type OID to a Rust type token.
///
/// Returns `Err` if the OID maps to a Rust type behind a disabled feature
/// (`chrono`, `json`, or `uuid`). Callers surface the error as a
/// `syn::Error` pointing at the SQL literal.
fn oid_to_rust_type(oid: u32) -> Result<proc_macro2::TokenStream, String> {
    let ty = match oid {
        // Scalar types
        16 => quote! { bool },
        18 | 19 | 25 | 1042 | 1043 => quote! { String },
        20 => quote! { i64 },
        21 => quote! { i16 },
        23 | 26 => quote! { i32 },
        700 => quote! { f32 },
        701 => quote! { f64 },
        17 => quote! { Vec<u8> },
        869 => quote! { resolute::PgInet },
        1700 => quote! { resolute::PgNumeric },
        // Array types
        1000 => quote! { Vec<bool> },
        1005 => quote! { Vec<i16> },
        1007 => quote! { Vec<i32> },
        1009 | 1015 => quote! { Vec<String> },
        1016 => quote! { Vec<i64> },
        1021 => quote! { Vec<f32> },
        1022 => quote! { Vec<f64> },
        1041 => quote! { Vec<resolute::PgInet> },
        1231 => quote! { Vec<resolute::PgNumeric> },
        // Range types
        3904 => quote! { resolute::PgRange<i32> },
        3926 => quote! { resolute::PgRange<i64> },
        3906 => quote! { resolute::PgRange<resolute::PgNumeric> },
        // JSON (feature = "json")
        #[cfg(feature = "json")]
        114 | 3802 => quote! { serde_json::Value },
        #[cfg(feature = "json")]
        3807 => quote! { Vec<serde_json::Value> },
        #[cfg(not(feature = "json"))]
        114 | 3802 | 3807 => {
            return Err(format!(
                "column type `{}` requires the `json` feature, which is disabled. \
                 Enable `resolute/json` in your Cargo.toml to use JSON/JSONB columns.",
                oid_to_type_name(oid)
            ));
        }
        // chrono (feature = "chrono")
        #[cfg(feature = "chrono")]
        1082 => quote! { chrono::NaiveDate },
        #[cfg(feature = "chrono")]
        1083 => quote! { chrono::NaiveTime },
        #[cfg(feature = "chrono")]
        1114 => quote! { chrono::NaiveDateTime },
        #[cfg(feature = "chrono")]
        1184 => quote! { chrono::DateTime<chrono::Utc> },
        #[cfg(feature = "chrono")]
        1115 => quote! { Vec<chrono::NaiveDateTime> },
        #[cfg(feature = "chrono")]
        1182 => quote! { Vec<chrono::NaiveDate> },
        #[cfg(feature = "chrono")]
        1183 => quote! { Vec<chrono::NaiveTime> },
        #[cfg(feature = "chrono")]
        1185 => quote! { Vec<chrono::DateTime<chrono::Utc>> },
        #[cfg(feature = "chrono")]
        3912 => quote! { resolute::PgRange<chrono::NaiveDate> },
        #[cfg(feature = "chrono")]
        3908 => quote! { resolute::PgRange<chrono::NaiveDateTime> },
        #[cfg(feature = "chrono")]
        3910 => quote! { resolute::PgRange<chrono::DateTime<chrono::Utc>> },
        #[cfg(not(feature = "chrono"))]
        1082 | 1083 | 1114 | 1184 | 1115 | 1182 | 1183 | 1185 | 3912 | 3908 | 3910 => {
            return Err(format!(
                "column type `{}` requires the `chrono` feature, which is disabled. \
                 Enable `resolute/chrono` in your Cargo.toml to use date/time columns.",
                oid_to_type_name(oid)
            ));
        }
        // uuid (feature = "uuid")
        #[cfg(feature = "uuid")]
        2950 => quote! { uuid::Uuid },
        #[cfg(feature = "uuid")]
        2951 => quote! { Vec<uuid::Uuid> },
        #[cfg(not(feature = "uuid"))]
        2950 | 2951 => {
            return Err(format!(
                "column type `{}` requires the `uuid` feature, which is disabled. \
                 Enable `resolute/uuid` in your Cargo.toml to use UUID columns.",
                oid_to_type_name(oid)
            ));
        }
        _ => quote! { Vec<u8> },
    };
    Ok(ty)
}

/// `query!("SQL", param1, param2, ...)` — compile-time checked SQL query.
#[proc_macro]
pub fn query(input: TokenStream) -> TokenStream {
    let parsed = parse_macro_input!(input as QueryInput);
    query_impl(parsed)
}

fn query_impl(input: QueryInput) -> TokenStream {
    let QueryInput { sql, params, named } = input;
    let sql_str = sql.value();

    let (sql_str, params) = match resolve_named(sql_str, params, &named, &sql) {
        Ok(v) => v,
        Err(ts) => return ts,
    };

    let (param_oids, column_infos) = match resolve_metadata(&sql_str) {
        Ok(result) => result,
        Err(e) => {
            return syn::Error::new_spanned(&sql, e).to_compile_error().into();
        }
    };

    if params.len() != param_oids.len() {
        let msg = format!(
            "expected {} parameter(s), got {}",
            param_oids.len(),
            params.len()
        );
        return syn::Error::new_spanned(&sql, msg).to_compile_error().into();
    }

    // Generate compile-time param type checks. The check verifies the param
    // implements `Encode`/`SqlParam`; downstream trait impls enforce that the
    // Rust type matches the PostgreSQL OID expected by the server.
    let param_type_checks: Vec<_> = params
        .iter()
        .map(|param| {
            quote! {
                {
                    fn __resolute_check_param<T: resolute::Encode + Sync>(_: &T) {}
                    __resolute_check_param(&#param);
                    let _ = &#param as &dyn resolute::SqlParam;
                }
            }
        })
        .collect();

    // Parse type overrides from column names (e.g., "id: UserId").
    let overrides: Vec<_> = column_infos
        .iter()
        .map(|c| parse_type_override(&c.name))
        .collect();

    let field_names: Vec<_> = overrides
        .iter()
        .map(|(name, _)| format_ident!("{}", sanitize_ident(name)))
        .collect();
    let field_types: Vec<_> = match column_infos
        .iter()
        .zip(overrides.iter())
        .map(
            |(c, (_, type_override))| -> Result<proc_macro2::TokenStream, String> {
                let base = if let Some(ref custom) = type_override {
                    custom.clone()
                } else {
                    oid_to_rust_type(c.type_oid)?
                };
                Ok(if c.nullable {
                    quote! { Option<#base> }
                } else {
                    base
                })
            },
        )
        .collect::<Result<Vec<_>, String>>()
    {
        Ok(v) => v,
        Err(e) => return syn::Error::new_spanned(&sql, e).to_compile_error().into(),
    };
    let _field_indices: Vec<_> = (0..column_infos.len()).collect::<Vec<_>>();
    let field_getters: Vec<_> = column_infos
        .iter()
        .enumerate()
        .map(|(i, c)| {
            if c.nullable {
                quote! { row.get_opt(#i)? }
            } else {
                quote! { row.get(#i)? }
            }
        })
        .collect();

    let struct_name = format_ident!("__QueryResult_{}", hash_sql(&sql_str));

    let param_refs: Vec<_> = params
        .iter()
        .map(|p| quote! { &#p as &dyn resolute::SqlParam })
        .collect();

    // Use rewritten SQL (with $1,$2) in generated code, not the original :name SQL.
    let sql_lit_rewritten = LitStr::new(&sql_str, sql.span());

    let expanded = quote! {
        {
            // Compile-time parameter type assertions.
            #(#param_type_checks)*

            #[allow(non_camel_case_types)]
            #[derive(Debug)]
            struct #struct_name {
                #(pub #field_names: #field_types,)*
            }

            resolute::CheckedQuery::<#struct_name> {
                sql: #sql_lit_rewritten,
                params: vec![#(#param_refs),*],
                _marker: std::marker::PhantomData,
                mapper: |row: &resolute::Row| -> Result<#struct_name, resolute::TypedError> {
                    Ok(#struct_name {
                        #(#field_names: #field_getters,)*
                    })
                },
            }
        }
    };

    TokenStream::from(expanded)
}

/// `query_as!(Type, "SQL", param1, param2, ...)` — compile-time checked query
/// mapped to an existing struct via FromRow.
#[proc_macro]
pub fn query_as(input: TokenStream) -> TokenStream {
    let parsed = parse_macro_input!(input as QueryAsInput);
    query_as_impl(parsed)
}

fn query_as_impl(input: QueryAsInput) -> TokenStream {
    let QueryAsInput {
        target_type,
        sql,
        params,
        named,
    } = input;
    let sql_str = sql.value();

    let (sql_str, params) = match resolve_named(sql_str, params, &named, &sql) {
        Ok(v) => v,
        Err(ts) => return ts,
    };

    let (param_oids, _column_infos) = match resolve_metadata(&sql_str) {
        Ok(result) => result,
        Err(e) => {
            return syn::Error::new_spanned(&sql, e).to_compile_error().into();
        }
    };

    if params.len() != param_oids.len() {
        let msg = format!(
            "expected {} parameter(s), got {}",
            param_oids.len(),
            params.len()
        );
        return syn::Error::new_spanned(&sql, msg).to_compile_error().into();
    }

    let param_refs: Vec<_> = params
        .iter()
        .map(|p| quote! { &#p as &dyn resolute::SqlParam })
        .collect();
    let sql_lit_rewritten = LitStr::new(&sql_str, sql.span());

    let expanded = quote! {
        {
            resolute::CheckedQuery::<#target_type> {
                sql: #sql_lit_rewritten,
                params: vec![#(#param_refs),*],
                _marker: std::marker::PhantomData,
                mapper: |row: &resolute::Row| -> Result<#target_type, resolute::TypedError> {
                    <#target_type as resolute::FromRow>::from_row(row)
                },
            }
        }
    };

    TokenStream::from(expanded)
}

/// `query_scalar!("SQL", param1, ...)` — compile-time checked single-value query.
#[proc_macro]
pub fn query_scalar(input: TokenStream) -> TokenStream {
    let parsed = parse_macro_input!(input as QueryInput);
    query_scalar_impl(parsed)
}

fn query_scalar_impl(input: QueryInput) -> TokenStream {
    let QueryInput { sql, params, named } = input;
    let sql_str = sql.value();

    let (sql_str, params) = match resolve_named(sql_str, params, &named, &sql) {
        Ok(v) => v,
        Err(ts) => return ts,
    };

    let (param_oids, column_infos) = match resolve_metadata(&sql_str) {
        Ok(result) => result,
        Err(e) => {
            return syn::Error::new_spanned(&sql, e).to_compile_error().into();
        }
    };

    if params.len() != param_oids.len() {
        let msg = format!(
            "expected {} parameter(s), got {}",
            param_oids.len(),
            params.len()
        );
        return syn::Error::new_spanned(&sql, msg).to_compile_error().into();
    }

    if column_infos.len() != 1 {
        let msg = format!(
            "query_scalar! requires exactly 1 column, got {}",
            column_infos.len()
        );
        return syn::Error::new_spanned(&sql, msg).to_compile_error().into();
    }

    let scalar_type = {
        let (_, type_override) = parse_type_override(&column_infos[0].name);
        match type_override {
            Some(ty) => ty,
            None => match oid_to_rust_type(column_infos[0].type_oid) {
                Ok(ty) => ty,
                Err(e) => return syn::Error::new_spanned(&sql, e).to_compile_error().into(),
            },
        }
    };
    let param_refs: Vec<_> = params
        .iter()
        .map(|p| quote! { &#p as &dyn resolute::SqlParam })
        .collect();
    let sql_lit_rewritten = LitStr::new(&sql_str, sql.span());

    let expanded = quote! {
        {
            resolute::CheckedQuery::<#scalar_type> {
                sql: #sql_lit_rewritten,
                params: vec![#(#param_refs),*],
                _marker: std::marker::PhantomData,
                mapper: |row: &resolute::Row| -> Result<#scalar_type, resolute::TypedError> {
                    row.get(0)
                },
            }
        }
    };

    TokenStream::from(expanded)
}

/// Input to query_as!: Type, "SQL", params...
struct QueryAsInput {
    target_type: syn::Type,
    sql: LitStr,
    params: Vec<Expr>,
    named: Vec<(syn::Ident, Expr)>,
}

impl Parse for QueryAsInput {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let target_type: syn::Type = input.parse()?;
        input.parse::<Token![,]>()?;
        let sql: LitStr = input.parse()?;
        let mut params = Vec::new();
        let mut named = Vec::new();
        let mut mode: Option<bool> = None;
        while input.peek(Token![,]) {
            input.parse::<Token![,]>()?;
            if input.is_empty() {
                break;
            }
            let is_named_param = {
                let fork = input.fork();
                fork.parse::<syn::Ident>().is_ok()
                    && fork.parse::<Token![=]>().is_ok()
                    && !fork.peek(Token![=])
            };
            if is_named_param && mode != Some(false) {
                let name: syn::Ident = input.parse()?;
                input.parse::<Token![=]>()?;
                let expr: Expr = input.parse()?;
                named.push((name, expr));
                mode = Some(true);
            } else if mode != Some(true) {
                params.push(input.parse()?);
                mode = Some(false);
            } else {
                return Err(input.error("cannot mix positional and named parameters"));
            }
        }
        Ok(QueryAsInput {
            target_type,
            sql,
            params,
            named,
        })
    }
}

/// `query_file!("path/to/query.sql", param1, param2, ...)` — like query! but reads SQL from a file.
#[proc_macro]
pub fn query_file(input: TokenStream) -> TokenStream {
    let QueryInput {
        sql: path_lit,
        params,
        named: _,
    } = parse_macro_input!(input as QueryInput);
    let file_path = path_lit.value();

    let sql_str = match read_sql_file(&file_path) {
        Ok(s) => s,
        Err(e) => {
            return syn::Error::new_spanned(&path_lit, e)
                .to_compile_error()
                .into();
        }
    };

    // Reuse the query! logic with the file contents.
    let sql_lit = LitStr::new(&sql_str, path_lit.span());
    let inner = QueryInput {
        sql: sql_lit,
        params,
        named: Vec::new(),
    };
    query_impl(inner)
}

/// `query_file_as!(Type, "path/to/query.sql", param1, ...)` — like query_as! but reads SQL from a file.
#[proc_macro]
pub fn query_file_as(input: TokenStream) -> TokenStream {
    let QueryAsInput {
        target_type,
        sql: path_lit,
        params,
        named: _,
    } = parse_macro_input!(input as QueryAsInput);
    let file_path = path_lit.value();

    let sql_str = match read_sql_file(&file_path) {
        Ok(s) => s,
        Err(e) => {
            return syn::Error::new_spanned(&path_lit, e)
                .to_compile_error()
                .into();
        }
    };

    let sql_lit = LitStr::new(&sql_str, path_lit.span());
    let inner = QueryAsInput {
        target_type,
        sql: sql_lit,
        params,
        named: Vec::new(),
    };
    query_as_impl(inner)
}

/// `query_file_scalar!("path/to/query.sql", param1, ...)` — file-based scalar query.
#[proc_macro]
pub fn query_file_scalar(input: TokenStream) -> TokenStream {
    let QueryInput {
        sql: path_lit,
        params,
        named: _,
    } = parse_macro_input!(input as QueryInput);
    let file_path = path_lit.value();

    let sql_str = match read_sql_file(&file_path) {
        Ok(s) => s,
        Err(e) => {
            return syn::Error::new_spanned(&path_lit, e)
                .to_compile_error()
                .into();
        }
    };

    let sql_lit = LitStr::new(&sql_str, path_lit.span());
    let inner = QueryInput {
        sql: sql_lit,
        params,
        named: Vec::new(),
    };
    query_scalar_impl(inner)
}

/// `query_unchecked!("SQL", param1, ...)` — skip compile-time validation.
/// Useful when DATABASE_URL is unavailable and no cache exists.
/// Params are passed as-is; no type or count checking.
#[proc_macro]
pub fn query_unchecked(input: TokenStream) -> TokenStream {
    let QueryInput {
        sql,
        params,
        named: _,
    } = parse_macro_input!(input as QueryInput);

    let param_refs: Vec<_> = params
        .iter()
        .map(|p| quote! { &#p as &dyn resolute::SqlParam })
        .collect();
    let sql_literal = &sql;

    let expanded = quote! {
        {
            resolute::UncheckedQuery {
                sql: #sql_literal,
                params: vec![#(#param_refs),*],
            }
        }
    };

    TokenStream::from(expanded)
}

/// Read a SQL file relative to CARGO_MANIFEST_DIR.
fn read_sql_file(path: &str) -> Result<String, String> {
    let manifest_dir = std::env::var("CARGO_MANIFEST_DIR").unwrap_or_else(|_| ".".into());
    let full_path = std::path::Path::new(&manifest_dir).join(path);
    std::fs::read_to_string(&full_path)
        .map_err(|e| format!("Failed to read SQL file {}: {e}", full_path.display()))
        .map(|s| s.trim().to_string())
}

/// Human-readable PG type name for error messages.
#[allow(dead_code)]
fn oid_to_type_name(oid: u32) -> &'static str {
    match oid {
        16 => "bool",
        18 | 19 | 25 | 1042 | 1043 => "text",
        20 => "int8",
        21 => "int2",
        23 => "int4",
        26 => "oid",
        700 => "float4",
        701 => "float8",
        17 => "bytea",
        114 => "json",
        869 => "inet",
        1700 => "numeric",
        3802 => "jsonb",
        1082 => "date",
        1083 => "time",
        1114 => "timestamp",
        1184 => "timestamptz",
        2950 => "uuid",
        // Array types
        1000 => "bool[]",
        1005 => "int2[]",
        1007 => "int4[]",
        1009 | 1015 => "text[]",
        1016 => "int8[]",
        1021 => "float4[]",
        1022 => "float8[]",
        1041 => "inet[]",
        1115 => "timestamp[]",
        1182 => "date[]",
        1183 => "time[]",
        1185 => "timestamptz[]",
        1231 => "numeric[]",
        2951 => "uuid[]",
        3807 => "jsonb[]",
        // Range types
        3904 => "int4range",
        3926 => "int8range",
        3906 => "numrange",
        3912 => "daterange",
        3908 => "tsrange",
        3910 => "tstzrange",
        _ => "unknown",
    }
}

fn sanitize_ident(name: &str) -> String {
    let s: String = name
        .chars()
        .map(|c| {
            if c.is_alphanumeric() || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect();
    if s.starts_with(|c: char| c.is_ascii_digit()) {
        format!("_{s}")
    } else if s.is_empty() {
        "column".to_string()
    } else {
        s
    }
}

/// Parse a type override from a column name.
///
/// Supports `"col_name: RustType"` syntax (e.g., `"id: UserId"`).
/// Skips `::` (PostgreSQL casts) to avoid false positives.
/// Returns `(actual_name, Some(type))` if override found, or `(original_name, None)`.
fn parse_type_override(column_name: &str) -> (String, Option<proc_macro2::TokenStream>) {
    let bytes = column_name.as_bytes();
    for (i, &b) in bytes.iter().enumerate() {
        if b == b':' {
            // Skip `::` (PostgreSQL cast syntax).
            let prev_colon = i > 0 && bytes[i - 1] == b':';
            let next_colon = i + 1 < bytes.len() && bytes[i + 1] == b':';
            if prev_colon || next_colon {
                continue;
            }
            let name = column_name[..i].trim();
            let type_str = column_name[i + 1..].trim();
            if !type_str.is_empty() {
                if let Ok(ty) = syn::parse_str::<syn::Type>(type_str) {
                    return (name.to_string(), Some(quote! { #ty }));
                }
            }
        }
    }
    (column_name.to_string(), None)
}

/// FNV-1a hash.
pub(crate) fn hash_sql(sql: &str) -> u64 {
    let mut h: u64 = 0xcbf29ce484222325;
    for b in sql.bytes() {
        h ^= b as u64;
        h = h.wrapping_mul(0x100000001b3);
    }
    h
}

/// Rewrite `:name` named params to `$N` positional params.
///
/// Honours PostgreSQL token boundaries so `:name` tokens inside comments,
/// string literals, quoted identifiers, and dollar-quoted bodies are left
/// alone. Duplicate names reuse the same positional index. Returns
/// `(rewritten_sql, ordered_param_names)`.
fn rewrite_named_params(sql: &str) -> (String, Vec<String>) {
    let mut result = String::with_capacity(sql.len());
    let mut names: Vec<String> = Vec::new();
    let mut positions: std::collections::HashMap<String, usize> = std::collections::HashMap::new();
    let chars: Vec<char> = sql.chars().collect();
    let len = chars.len();
    let mut i = 0;

    while i < len {
        // -- line comment: passes through verbatim, but any `:name` inside
        // is treated as text, not a placeholder.
        if i + 1 < len && chars[i] == '-' && chars[i + 1] == '-' {
            while i < len && chars[i] != '\n' {
                result.push(chars[i]);
                i += 1;
            }
            continue;
        }

        // /* block comment */: same deal, pass through unchanged.
        if i + 1 < len && chars[i] == '/' && chars[i + 1] == '*' {
            result.push('/');
            result.push('*');
            i += 2;
            while i + 1 < len && !(chars[i] == '*' && chars[i + 1] == '/') {
                result.push(chars[i]);
                i += 1;
            }
            if i + 1 < len {
                result.push('*');
                result.push('/');
                i += 2;
            }
            continue;
        }

        // 'string literal' with '' escaping.
        if chars[i] == '\'' {
            result.push('\'');
            i += 1;
            while i < len {
                result.push(chars[i]);
                if chars[i] == '\'' {
                    if i + 1 < len && chars[i + 1] == '\'' {
                        result.push('\'');
                        i += 2;
                    } else {
                        i += 1;
                        break;
                    }
                } else {
                    i += 1;
                }
            }
            continue;
        }

        // "quoted identifier": skip contents.
        if chars[i] == '"' {
            result.push('"');
            i += 1;
            while i < len {
                result.push(chars[i]);
                if chars[i] == '"' {
                    i += 1;
                    break;
                }
                i += 1;
            }
            continue;
        }

        // $tag$dollar-quoted body$tag$: skip contents (also handles $$...$$).
        if chars[i] == '$' {
            let tag_start = i;
            i += 1;
            while i < len && (chars[i].is_alphanumeric() || chars[i] == '_') {
                i += 1;
            }
            if i < len && chars[i] == '$' {
                let tag: String = chars[tag_start..=i].iter().collect();
                for c in tag.chars() {
                    result.push(c);
                }
                i += 1;
                let tag_chars: Vec<char> = tag.chars().collect();
                let tag_len = tag_chars.len();
                loop {
                    if i >= len {
                        break;
                    }
                    if chars[i] == '$' && i + tag_len <= len {
                        let matches = chars[i..i + tag_len]
                            .iter()
                            .zip(tag_chars.iter())
                            .all(|(a, b)| a == b);
                        if matches {
                            for c in &tag_chars {
                                result.push(*c);
                            }
                            i += tag_len;
                            break;
                        }
                    }
                    result.push(chars[i]);
                    i += 1;
                }
                continue;
            } else {
                // Bare `$` followed by non-tag (e.g. `$1` positional param).
                i = tag_start;
                result.push(chars[i]);
                i += 1;
                continue;
            }
        }

        // :: cast: pass through.
        if chars[i] == ':' && i + 1 < len && chars[i + 1] == ':' {
            result.push(':');
            result.push(':');
            i += 2;
            continue;
        }

        // :name — named parameter.
        if chars[i] == ':' && i + 1 < len && (chars[i + 1].is_alphabetic() || chars[i + 1] == '_') {
            i += 1;
            let start = i;
            while i < len && (chars[i].is_alphanumeric() || chars[i] == '_') {
                i += 1;
            }
            let name: String = chars[start..i].iter().collect();
            let pos = if let Some(&existing) = positions.get(&name) {
                existing
            } else {
                names.push(name.clone());
                let pos = names.len();
                positions.insert(name, pos);
                pos
            };
            result.push('$');
            result.push_str(&pos.to_string());
            continue;
        }

        result.push(chars[i]);
        i += 1;
    }

    (result, names)
}

fn parse_pg_uri(uri: &str) -> Option<(String, String, String, u16, String)> {
    let rest = uri
        .strip_prefix("postgres://")
        .or_else(|| uri.strip_prefix("postgresql://"))?;
    let (auth, hostdb) = rest.split_once('@').unwrap_or(("postgres:postgres", rest));
    let (user, password) = auth.split_once(':').unwrap_or((auth, ""));
    let (hostport, database) = hostdb.split_once('/').unwrap_or((hostdb, "postgres"));
    let (host, port_str) = hostport.split_once(':').unwrap_or((hostport, "5432"));
    let port: u16 = port_str.parse().unwrap_or(5432);
    Some((
        user.to_string(),
        password.to_string(),
        host.to_string(),
        port,
        database.to_string(),
    ))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // -- parse_type_override tests --

    #[test]
    fn test_type_override_basic() {
        let (name, ty) = parse_type_override("id: UserId");
        assert_eq!(name, "id");
        assert!(ty.is_some());
    }

    #[test]
    fn test_type_override_with_module_path() {
        let (name, ty) = parse_type_override("id: crate::types::UserId");
        assert_eq!(name, "id");
        assert!(ty.is_some());
    }

    #[test]
    fn test_type_override_no_override() {
        let (name, ty) = parse_type_override("user_name");
        assert_eq!(name, "user_name");
        assert!(ty.is_none());
    }

    #[test]
    fn test_type_override_skips_double_colon_cast() {
        let (name, ty) = parse_type_override("created_at::text");
        assert_eq!(name, "created_at::text");
        assert!(ty.is_none(), ":: should not trigger type override");
    }

    #[test]
    fn test_type_override_invalid_type_string() {
        let (name, ty) = parse_type_override("col: 123invalid");
        assert_eq!(name, "col: 123invalid");
        assert!(ty.is_none(), "invalid Rust type should fall back");
    }

    #[test]
    fn test_type_override_empty_after_colon() {
        let (name, ty) = parse_type_override("col:");
        assert_eq!(name, "col:");
        assert!(ty.is_none());
    }

    #[test]
    fn test_type_override_with_spaces() {
        let (name, ty) = parse_type_override("  id  :  UserId  ");
        assert_eq!(name, "id");
        assert!(ty.is_some());
    }

    #[test]
    fn test_type_override_option_type() {
        let (name, ty) = parse_type_override("email: Option<String>");
        assert_eq!(name, "email");
        assert!(ty.is_some());
    }

    #[test]
    fn test_type_override_vec_type() {
        let (name, ty) = parse_type_override("tags: Vec<String>");
        assert_eq!(name, "tags");
        assert!(ty.is_some());
    }

    // -- rewrite_named_params tests --

    #[test]
    fn test_named_params_basic() {
        let (sql, names) = rewrite_named_params("SELECT :id, :name");
        assert_eq!(sql, "SELECT $1, $2");
        assert_eq!(names, vec!["id", "name"]);
    }

    #[test]
    fn test_named_params_duplicate() {
        let (sql, names) = rewrite_named_params("SELECT :id WHERE :id > 0");
        assert_eq!(sql, "SELECT $1 WHERE $1 > 0");
        assert_eq!(names, vec!["id"]);
    }

    #[test]
    fn test_named_params_with_cast() {
        let (sql, names) = rewrite_named_params("SELECT :val::int4");
        assert_eq!(sql, "SELECT $1::int4");
        assert_eq!(names, vec!["val"]);
    }

    #[test]
    fn test_named_params_in_string_literal() {
        let (sql, names) = rewrite_named_params("SELECT ':not_a_param'");
        assert_eq!(sql, "SELECT ':not_a_param'");
        assert!(names.is_empty());
    }

    #[test]
    fn test_named_params_empty() {
        let (sql, names) = rewrite_named_params("SELECT 1");
        assert_eq!(sql, "SELECT 1");
        assert!(names.is_empty());
    }

    #[test]
    fn test_named_params_underscore_prefix() {
        let (sql, names) = rewrite_named_params("SELECT :_private");
        assert_eq!(sql, "SELECT $1");
        assert_eq!(names, vec!["_private"]);
    }

    // -- parse_pg_uri tests --

    #[test]
    fn test_parse_uri_full() {
        let (u, p, h, port, db) = parse_pg_uri("postgres://user:pass@host:1234/mydb").unwrap();
        assert_eq!(u, "user");
        assert_eq!(p, "pass");
        assert_eq!(h, "host");
        assert_eq!(port, 1234);
        assert_eq!(db, "mydb");
    }

    #[test]
    fn test_parse_uri_defaults() {
        let (u, p, h, port, db) = parse_pg_uri("postgres://user:pass@localhost/mydb").unwrap();
        assert_eq!(h, "localhost");
        assert_eq!(port, 5432);
        assert_eq!(u, "user");
        assert_eq!(p, "pass");
        assert_eq!(db, "mydb");
    }

    #[test]
    fn test_parse_uri_invalid() {
        assert!(parse_pg_uri("mysql://user:pass@host/db").is_none());
    }

    #[test]
    fn test_parse_uri_postgresql_scheme() {
        let parsed = parse_pg_uri("postgresql://user:pass@host:5433/mydb").unwrap();
        assert_eq!(parsed.0, "user");
        assert_eq!(parsed.3, 5433);
        assert_eq!(parsed.4, "mydb");
    }

    #[test]
    fn test_parse_uri_empty_password() {
        let parsed = parse_pg_uri("postgres://user@host/db").unwrap();
        assert_eq!(parsed.0, "user");
        assert_eq!(parsed.1, "");
        assert_eq!(parsed.2, "host");
    }

    #[test]
    fn test_parse_uri_unset_database_defaults_to_postgres() {
        let parsed = parse_pg_uri("postgres://user:pass@host").unwrap();
        assert_eq!(parsed.4, "postgres");
    }

    // -- rewrite_named_params: comments and dollar quotes --

    #[test]
    fn test_named_params_line_comment_skipped() {
        let (sql, names) = rewrite_named_params("SELECT :id -- :bogus\nFROM t");
        assert_eq!(sql, "SELECT $1 -- :bogus\nFROM t");
        assert_eq!(names, vec!["id"]);
    }

    #[test]
    fn test_named_params_block_comment_skipped() {
        let (sql, names) = rewrite_named_params("SELECT :id /* :bogus */ FROM t");
        assert_eq!(sql, "SELECT $1 /* :bogus */ FROM t");
        assert_eq!(names, vec!["id"]);
    }

    #[test]
    fn test_named_params_dollar_quoted_body_skipped() {
        let (sql, names) = rewrite_named_params("SELECT $$ :ignored $$ WHERE id = :id");
        assert_eq!(sql, "SELECT $$ :ignored $$ WHERE id = $1");
        assert_eq!(names, vec!["id"]);
    }

    #[test]
    fn test_named_params_tagged_dollar_quote_skipped() {
        let (sql, names) = rewrite_named_params("SELECT $tag$ :ignored $tag$ WHERE id = :id");
        assert_eq!(sql, "SELECT $tag$ :ignored $tag$ WHERE id = $1");
        assert_eq!(names, vec!["id"]);
    }

    #[test]
    fn test_named_params_quoted_identifier_skipped() {
        let (sql, names) = rewrite_named_params(r#"SELECT ":col" FROM t WHERE id = :id"#);
        assert_eq!(sql, r#"SELECT ":col" FROM t WHERE id = $1"#);
        assert_eq!(names, vec!["id"]);
    }

    #[test]
    fn test_named_params_positional_dollar_param_passthrough() {
        let (sql, names) = rewrite_named_params("SELECT $1, :id FROM t");
        assert_eq!(sql, "SELECT $1, $1 FROM t");
        assert_eq!(names, vec!["id"]);
    }

    #[test]
    fn test_named_params_escaped_single_quote_inside_literal() {
        let (sql, names) = rewrite_named_params("SELECT 'it''s :nothing' , :real");
        assert_eq!(sql, "SELECT 'it''s :nothing' , $1");
        assert_eq!(names, vec!["real"]);
    }

    // -- hash_sql --

    #[test]
    fn test_hash_sql_stable() {
        let sql = "SELECT id FROM t WHERE x = $1";
        assert_eq!(hash_sql(sql), hash_sql(sql));
    }

    #[test]
    fn test_hash_sql_differs_by_content() {
        assert_ne!(hash_sql("SELECT 1"), hash_sql("SELECT 2"));
    }

    #[test]
    fn test_hash_sql_empty() {
        // FNV-1a offset basis for the empty input.
        assert_eq!(hash_sql(""), 0xcbf29ce484222325);
    }

    // -- cache round-trip --

    #[test]
    fn test_cache_roundtrip() {
        let tmp = std::env::temp_dir().join(format!(
            "resolute-macros-cache-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&tmp).unwrap();
        let path = tmp.join("query.json");

        let entry = cache::CacheEntry {
            sql: "SELECT 1::int4 AS n".into(),
            hash: 0xdeadbeef_cafebabe,
            param_oids: vec![23, 25],
            columns: vec![cache::CachedColumn {
                name: "n".into(),
                type_oid: 23,
                nullable: true,
            }],
        };

        let json = serde_json::to_string_pretty(&entry).unwrap();
        std::fs::write(&path, &json).unwrap();
        let raw = std::fs::read_to_string(&path).unwrap();
        let decoded: cache::CacheEntry = serde_json::from_str(&raw).unwrap();

        assert_eq!(decoded.sql, entry.sql);
        assert_eq!(decoded.hash, entry.hash);
        assert_eq!(decoded.param_oids, entry.param_oids);
        assert_eq!(decoded.columns.len(), 1);
        assert_eq!(decoded.columns[0].name, "n");
        assert_eq!(decoded.columns[0].type_oid, 23);
        assert!(decoded.columns[0].nullable);

        std::fs::remove_dir_all(&tmp).ok();
    }

    #[test]
    fn test_cache_entry_missing_nullable_defaults_to_false() {
        // Old cache files written before the `nullable` field existed must
        // still deserialize — the field is `#[serde(default)]`.
        let legacy = r#"{
            "sql": "SELECT 1",
            "hash": 1,
            "param_oids": [],
            "columns": [{"name": "n", "type_oid": 23}]
        }"#;
        let entry: cache::CacheEntry = serde_json::from_str(legacy).unwrap();
        assert_eq!(entry.columns.len(), 1);
        assert!(!entry.columns[0].nullable);
    }
}
