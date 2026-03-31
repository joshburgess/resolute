//! The `Executor` trait: a unified interface for query execution.
//!
//! Unlike sqlx's `Executor` which consumes `self` (preventing multi-query reuse),
//! this trait uses `&self` methods. Write generic functions once, call them with
//! any executor type:
//!
//! ```ignore
//! async fn get_user(db: &impl Executor, id: i32) -> Result<User, TypedError> {
//!     let rows = db.query("SELECT * FROM users WHERE id = $1", &[&id]).await?;
//!     User::from_row(&rows[0])
//! }
//!
//! // Works with Client, Transaction, or PooledTypedClient:
//! get_user(&client, 1).await?;
//! get_user(&txn, 1).await?;
//! get_user(&pooled, 1).await?;
//! ```

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::encode::SqlParam;
use crate::error::TypedError;
use crate::row::Row;

static SAVEPOINT_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Trait for types that can execute PostgreSQL queries.
///
/// All methods take `&self` — no consuming, no lifetime gymnastics.
/// Only `query` and `execute` need to be implemented; the rest are
/// provided as default methods.
///
/// # Examples
///
/// Write generic functions that work with any executor:
///
/// ```ignore
/// async fn count_users(db: &impl Executor) -> Result<i64, TypedError> {
///     let row = db.query_one("SELECT count(*) FROM users", &[]).await?;
///     row.get::<i64>(0)
/// }
///
/// // Call with a Client, Transaction, or PooledTypedClient:
/// let n = count_users(&client).await?;
/// let n = count_users(&txn).await?;
/// let n = count_users(&pooled).await?;
/// ```
pub trait Executor: Send + Sync {
    /// Execute a query and return all result rows.
    fn query<'a>(
        &'a self,
        sql: &'a str,
        params: &'a [&'a dyn SqlParam],
    ) -> impl Future<Output = Result<Vec<Row>, TypedError>> + Send + 'a;

    /// Execute a statement (INSERT/UPDATE/DELETE) and return affected row count.
    fn execute<'a>(
        &'a self,
        sql: &'a str,
        params: &'a [&'a dyn SqlParam],
    ) -> impl Future<Output = Result<u64, TypedError>> + Send + 'a;

    /// Execute a query and return exactly one row.
    fn query_one<'a>(
        &'a self,
        sql: &'a str,
        params: &'a [&'a dyn SqlParam],
    ) -> impl Future<Output = Result<Row, TypedError>> + Send + 'a {
        async move {
            let rows = self.query(sql, params).await?;
            if rows.len() != 1 {
                return Err(TypedError::NotExactlyOne(rows.len()));
            }
            Ok(rows.into_iter().next().unwrap())
        }
    }

    /// Execute a query and return an optional single row.
    fn query_opt<'a>(
        &'a self,
        sql: &'a str,
        params: &'a [&'a dyn SqlParam],
    ) -> impl Future<Output = Result<Option<Row>, TypedError>> + Send + 'a {
        async move {
            let rows = self.query(sql, params).await?;
            match rows.len() {
                0 => Ok(None),
                1 => Ok(Some(rows.into_iter().next().unwrap())),
                n => Err(TypedError::NotExactlyOne(n)),
            }
        }
    }

    /// Execute a query with named parameters (`:name` syntax).
    fn query_named<'a>(
        &'a self,
        sql: &'a str,
        params: &'a [(&'a str, &'a dyn SqlParam)],
    ) -> impl Future<Output = Result<Vec<Row>, TypedError>> + Send + 'a {
        async move {
            let (rewritten, names) = crate::named_params::rewrite(sql);
            let ordered = resolve_named(&names, params)?;
            self.query(&rewritten, &ordered).await
        }
    }

    /// Execute a named-param statement. Returns affected row count.
    fn execute_named<'a>(
        &'a self,
        sql: &'a str,
        params: &'a [(&'a str, &'a dyn SqlParam)],
    ) -> impl Future<Output = Result<u64, TypedError>> + Send + 'a {
        async move {
            let (rewritten, names) = crate::named_params::rewrite(sql);
            let ordered = resolve_named(&names, params)?;
            self.execute(&rewritten, &ordered).await
        }
    }

    /// Run a closure with guaranteed atomicity.
    ///
    /// - **Client / PooledTypedClient**: wraps in `BEGIN` / `COMMIT` (or `ROLLBACK` on error).
    /// - **Transaction**: uses a `SAVEPOINT` (nested transaction), so calling `atomic`
    ///   inside an existing transaction is safe and composes correctly.
    ///
    /// This lets you write functions that always run atomically, regardless of
    /// whether the caller already has a transaction open:
    ///
    /// ```ignore
    /// async fn create_order(db: &impl Executor, items: &[Item]) -> Result<Order, TypedError> {
    ///     db.atomic(|db| Box::pin(async move {
    ///         let order = insert_order(db).await?;
    ///         for item in items {
    ///             insert_line_item(db, order.id, item).await?;
    ///         }
    ///         Ok(order)
    ///     })).await
    /// }
    ///
    /// // Without a transaction — atomic() creates one:
    /// create_order(&client, &items).await?;
    ///
    /// // Inside an existing transaction — atomic() uses a savepoint:
    /// let txn = client.begin().await?;
    /// create_order(&txn, &items).await?;  // savepoint, not a nested BEGIN
    /// do_other_stuff(&txn).await?;
    /// txn.commit().await?;
    /// ```
    fn atomic<'a, T: Send + 'a>(
        &'a self,
        f: impl FnOnce(&'a Self) -> Pin<Box<dyn Future<Output = Result<T, TypedError>> + Send + 'a>> + Send + 'a,
    ) -> impl Future<Output = Result<T, TypedError>> + Send + 'a;

    /// Ping the database to verify the connection is healthy.
    fn ping<'a>(&'a self) -> impl Future<Output = Result<(), TypedError>> + Send + 'a {
        async move {
            self.query("SELECT 1", &[]).await?;
            Ok(())
        }
    }

    /// Bulk-load data via COPY FROM STDIN. Returns the number of rows copied.
    fn copy_in<'a>(
        &'a self,
        copy_sql: &'a str,
        data: &'a [u8],
    ) -> impl Future<Output = Result<u64, TypedError>> + Send + 'a;

    /// Export data via COPY TO STDOUT. Returns all the data.
    fn copy_out<'a>(
        &'a self,
        copy_sql: &'a str,
    ) -> impl Future<Output = Result<Vec<u8>, TypedError>> + Send + 'a;
}

/// Resolve named params to positional order.
fn resolve_named<'a>(
    names: &[String],
    params: &[(&str, &'a dyn SqlParam)],
) -> Result<Vec<&'a dyn SqlParam>, TypedError> {
    names
        .iter()
        .map(|name| {
            params
                .iter()
                .find(|(n, _)| *n == name.as_str())
                .map(|(_, p)| *p)
                .ok_or_else(|| TypedError::MissingParam(name.to_string()))
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Implementations
// ---------------------------------------------------------------------------

#[allow(clippy::manual_async_fn)]
impl Executor for crate::query::Client {
    fn query<'a>(
        &'a self,
        sql: &'a str,
        params: &'a [&'a dyn SqlParam],
    ) -> impl Future<Output = Result<Vec<Row>, TypedError>> + Send + 'a {
        crate::query::Client::query(self, sql, params)
    }

    fn execute<'a>(
        &'a self,
        sql: &'a str,
        params: &'a [&'a dyn SqlParam],
    ) -> impl Future<Output = Result<u64, TypedError>> + Send + 'a {
        crate::query::Client::execute(self, sql, params)
    }

    fn copy_in<'a>(
        &'a self,
        copy_sql: &'a str,
        data: &'a [u8],
    ) -> impl Future<Output = Result<u64, TypedError>> + Send + 'a {
        crate::query::Client::copy_in(self, copy_sql, data)
    }

    fn copy_out<'a>(
        &'a self,
        copy_sql: &'a str,
    ) -> impl Future<Output = Result<Vec<u8>, TypedError>> + Send + 'a {
        crate::query::Client::copy_out(self, copy_sql)
    }

    fn atomic<'a, T: Send + 'a>(
        &'a self,
        f: impl FnOnce(&'a Self) -> Pin<Box<dyn Future<Output = Result<T, TypedError>> + Send + 'a>> + Send + 'a,
    ) -> impl Future<Output = Result<T, TypedError>> + Send + 'a {
        async move {
            self.simple_query("BEGIN").await?;
            match f(self).await {
                Ok(val) => {
                    self.simple_query("COMMIT").await?;
                    Ok(val)
                }
                Err(e) => {
                    let _ = self.simple_query("ROLLBACK").await;
                    Err(e)
                }
            }
        }
    }
}

#[allow(clippy::manual_async_fn)]
impl Executor for crate::query::Transaction<'_> {
    fn query<'a>(
        &'a self,
        sql: &'a str,
        params: &'a [&'a dyn SqlParam],
    ) -> impl Future<Output = Result<Vec<Row>, TypedError>> + Send + 'a {
        self.client.query(sql, params)
    }

    fn execute<'a>(
        &'a self,
        sql: &'a str,
        params: &'a [&'a dyn SqlParam],
    ) -> impl Future<Output = Result<u64, TypedError>> + Send + 'a {
        self.client.execute(sql, params)
    }

    fn copy_in<'a>(
        &'a self,
        copy_sql: &'a str,
        data: &'a [u8],
    ) -> impl Future<Output = Result<u64, TypedError>> + Send + 'a {
        self.client.copy_in(copy_sql, data)
    }

    fn copy_out<'a>(
        &'a self,
        copy_sql: &'a str,
    ) -> impl Future<Output = Result<Vec<u8>, TypedError>> + Send + 'a {
        self.client.copy_out(copy_sql)
    }

    fn atomic<'a, T: Send + 'a>(
        &'a self,
        f: impl FnOnce(&'a Self) -> Pin<Box<dyn Future<Output = Result<T, TypedError>> + Send + 'a>> + Send + 'a,
    ) -> impl Future<Output = Result<T, TypedError>> + Send + 'a {
        async move {
            let id = SAVEPOINT_COUNTER.fetch_add(1, Ordering::Relaxed);
            let sp = format!("stalwart_sp_{id}");
            self.client.simple_query(&format!("SAVEPOINT {sp}")).await?;
            match f(self).await {
                Ok(val) => {
                    self.client
                        .simple_query(&format!("RELEASE SAVEPOINT {sp}"))
                        .await?;
                    Ok(val)
                }
                Err(e) => {
                    let _ = self
                        .client
                        .simple_query(&format!("ROLLBACK TO SAVEPOINT {sp}"))
                        .await;
                    Err(e)
                }
            }
        }
    }
}

#[allow(clippy::manual_async_fn)]
impl Executor for crate::pooled::PooledTypedClient {
    fn query<'a>(
        &'a self,
        sql: &'a str,
        params: &'a [&'a dyn SqlParam],
    ) -> impl Future<Output = Result<Vec<Row>, TypedError>> + Send + 'a {
        crate::pooled::PooledTypedClient::query(self, sql, params)
    }

    fn execute<'a>(
        &'a self,
        sql: &'a str,
        params: &'a [&'a dyn SqlParam],
    ) -> impl Future<Output = Result<u64, TypedError>> + Send + 'a {
        crate::pooled::PooledTypedClient::execute(self, sql, params)
    }

    fn copy_in<'a>(
        &'a self,
        copy_sql: &'a str,
        data: &'a [u8],
    ) -> impl Future<Output = Result<u64, TypedError>> + Send + 'a {
        crate::pooled::PooledTypedClient::copy_in(self, copy_sql, data)
    }

    fn copy_out<'a>(
        &'a self,
        copy_sql: &'a str,
    ) -> impl Future<Output = Result<Vec<u8>, TypedError>> + Send + 'a {
        crate::pooled::PooledTypedClient::copy_out(self, copy_sql)
    }

    fn atomic<'a, T: Send + 'a>(
        &'a self,
        f: impl FnOnce(&'a Self) -> Pin<Box<dyn Future<Output = Result<T, TypedError>> + Send + 'a>> + Send + 'a,
    ) -> impl Future<Output = Result<T, TypedError>> + Send + 'a {
        async move {
            self.simple_query("BEGIN").await?;
            match f(self).await {
                Ok(val) => {
                    self.simple_query("COMMIT").await?;
                    Ok(val)
                }
                Err(e) => {
                    let _ = self.simple_query("ROLLBACK").await;
                    Err(e)
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Closure-based transaction API
// ---------------------------------------------------------------------------

impl crate::query::Client {
    /// Run a closure inside a transaction. Commits on `Ok`, rolls back on `Err`.
    ///
    /// The closure receives the `Client` reference (which is inside a BEGIN..COMMIT
    /// block), so any `&impl Executor` function works inside it.
    ///
    /// ```ignore
    /// let user_id = client.with_transaction(|db| Box::pin(async move {
    ///     let id = create_user(db, "Alice").await?;
    ///     create_profile(db, id).await?;
    ///     Ok(id)
    /// })).await?;
    /// ```
    pub async fn with_transaction<'a, T: Send + 'a>(
        &'a self,
        f: impl FnOnce(&'a Self) -> Pin<Box<dyn Future<Output = Result<T, TypedError>> + Send + 'a>>,
    ) -> Result<T, TypedError> {
        self.simple_query("BEGIN").await?;
        match f(self).await {
            Ok(val) => {
                self.simple_query("COMMIT").await?;
                Ok(val)
            }
            Err(e) => {
                let _ = self.simple_query("ROLLBACK").await;
                Err(e)
            }
        }
    }
}
