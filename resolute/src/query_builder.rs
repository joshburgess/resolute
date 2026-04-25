//! Runtime query builder for the fluent `bind` / `bind_named` style.
//!
//! This is one of three ways to run a runtime (non-macro) query:
//!
//! 1. Slice form: `client.query(sql, &[&x, &y])` or the raw named form
//!    `client.query_named(sql, &[("a", &x as &dyn SqlParam), ...])`.
//! 2. Macros: `params![x, y]` and `params_named![("a", x), ("b", y)]`.
//! 3. Builder: `resolute::sql(sql).bind(x).bind(y).fetch_all(&client).await?`
//!    or `resolute::sql(sql).bind_named("a", x).bind_named("b", y)...`.
//!
//! All three forms are equivalent at runtime. Pick whichever reads best for
//! your call site. For compile-time checked queries use the `query!` macro
//! family instead.

use crate::encode::SqlParam;
use crate::error::TypedError;
use crate::executor::Executor;
use crate::row::Row;

enum BuilderParams {
    Empty,
    Positional(Vec<Box<dyn SqlParam + Send + Sync>>),
    Named(Vec<(String, Box<dyn SqlParam + Send + Sync>)>),
    Invalid(String),
}

/// Fluent builder for a runtime SQL query.
///
/// Construct with [`sql`]. Bind parameters with [`bind`](Self::bind) (positional)
/// or [`bind_named`](Self::bind_named). Execute with `fetch_all`, `fetch_one`,
/// `fetch_opt`, or `execute`. Each terminator takes `&impl Executor` so the
/// same builder chain works against a `Client`, `Transaction`, or pool handle.
///
/// Mixing positional and named binds in the same builder is a misuse. The
/// builder records the error and surfaces it as a `TypedError::Config` from
/// the terminator call, rather than panicking at the offending `bind`.
#[must_use = "QueryBuilder does nothing until a terminator like .fetch_all() or .execute() is awaited"]
pub struct QueryBuilder {
    sql: String,
    params: BuilderParams,
}

/// Start a new runtime query builder.
///
/// # Example
///
/// ```no_run
/// # async fn _doctest() -> Result<(), Box<dyn std::error::Error>> {
/// # let client: resolute::Client = unimplemented!();
/// # let org_id: i32 = 0;
/// # let user_id: i32 = 0;
/// let rows = resolute::sql("SELECT * FROM users WHERE org = $1 AND id = $2")
///     .bind(org_id)
///     .bind(user_id)
///     .fetch_all(&client)
///     .await?;
/// # let _: Vec<resolute::Row> = rows;
/// # Ok(()) }
/// ```
pub fn sql(sql: impl Into<String>) -> QueryBuilder {
    QueryBuilder {
        sql: sql.into(),
        params: BuilderParams::Empty,
    }
}

impl QueryBuilder {
    /// Bind a positional parameter. Order of `bind` calls determines `$1`, `$2`, ...
    ///
    /// Mixing with [`bind_named`](Self::bind_named) records an error that is
    /// returned from the terminator call.
    pub fn bind<T: SqlParam + Send + Sync + 'static>(mut self, value: T) -> Self {
        self.params = match self.params {
            BuilderParams::Empty => BuilderParams::Positional(vec![Box::new(value)]),
            BuilderParams::Positional(mut v) => {
                v.push(Box::new(value));
                BuilderParams::Positional(v)
            }
            BuilderParams::Named(_) => BuilderParams::Invalid(
                "cannot mix bind() and bind_named() on the same QueryBuilder".into(),
            ),
            BuilderParams::Invalid(msg) => BuilderParams::Invalid(msg),
        };
        self
    }

    /// Bind a named parameter. The SQL must reference the param as `:name`.
    ///
    /// Mixing with [`bind`](Self::bind) records an error that is returned
    /// from the terminator call.
    pub fn bind_named<T: SqlParam + Send + Sync + 'static>(
        mut self,
        name: impl Into<String>,
        value: T,
    ) -> Self {
        self.params = match self.params {
            BuilderParams::Empty => BuilderParams::Named(vec![(name.into(), Box::new(value))]),
            BuilderParams::Named(mut v) => {
                v.push((name.into(), Box::new(value)));
                BuilderParams::Named(v)
            }
            BuilderParams::Positional(_) => BuilderParams::Invalid(
                "cannot mix bind() and bind_named() on the same QueryBuilder".into(),
            ),
            BuilderParams::Invalid(msg) => BuilderParams::Invalid(msg),
        };
        self
    }

    fn materialize(&self) -> Result<(String, Vec<&dyn SqlParam>), TypedError> {
        match &self.params {
            BuilderParams::Empty => Ok((self.sql.clone(), Vec::new())),
            BuilderParams::Positional(v) => {
                let refs: Vec<&dyn SqlParam> =
                    v.iter().map(|b| b.as_ref() as &dyn SqlParam).collect();
                Ok((self.sql.clone(), refs))
            }
            BuilderParams::Named(v) => {
                let (rewritten, order) = crate::named_params::rewrite(&self.sql);
                let mut ordered: Vec<&dyn SqlParam> = Vec::with_capacity(order.len());
                for name in &order {
                    let found = v
                        .iter()
                        .find(|(n, _)| n == name)
                        .ok_or_else(|| TypedError::MissingParam(name.clone()))?;
                    ordered.push(found.1.as_ref() as &dyn SqlParam);
                }
                Ok((rewritten, ordered))
            }
            BuilderParams::Invalid(msg) => Err(TypedError::Config(msg.clone())),
        }
    }

    /// Execute the query and return all rows.
    pub async fn fetch_all(self, db: &impl Executor) -> Result<Vec<Row>, TypedError> {
        let (sql, params) = self.materialize()?;
        db.query(&sql, &params).await
    }

    /// Execute the query and return exactly one row.
    pub async fn fetch_one(self, db: &impl Executor) -> Result<Row, TypedError> {
        let (sql, params) = self.materialize()?;
        db.query_one(&sql, &params).await
    }

    /// Execute the query and return an optional row.
    pub async fn fetch_opt(self, db: &impl Executor) -> Result<Option<Row>, TypedError> {
        let (sql, params) = self.materialize()?;
        db.query_opt(&sql, &params).await
    }

    /// Execute a statement (INSERT / UPDATE / DELETE), return affected row count.
    pub async fn execute(self, db: &impl Executor) -> Result<u64, TypedError> {
        let (sql, params) = self.materialize()?;
        db.execute(&sql, &params).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_positional_materializes_cleanly() {
        let b = sql("SELECT 1");
        let (s, p) = b.materialize().unwrap();
        assert_eq!(s, "SELECT 1");
        assert_eq!(p.len(), 0);
    }

    #[test]
    fn positional_binds_preserve_order() {
        let b = sql("SELECT $1, $2, $3")
            .bind(1_i32)
            .bind("hello".to_string())
            .bind(true);
        let (s, p) = b.materialize().unwrap();
        assert_eq!(s, "SELECT $1, $2, $3");
        assert_eq!(p.len(), 3);
    }

    #[test]
    fn named_binds_reorder_to_match_sql() {
        let b = sql("SELECT * FROM t WHERE b = :b AND a = :a")
            .bind_named("a", 1_i32)
            .bind_named("b", 2_i32);
        let (rewritten, p) = b.materialize().unwrap();
        // Rewriter numbers :b first, :a second, because :b appears first in SQL.
        assert_eq!(rewritten, "SELECT * FROM t WHERE b = $1 AND a = $2");
        assert_eq!(p.len(), 2);
    }

    #[test]
    fn duplicate_named_params_bind_once() {
        let b = sql("SELECT * FROM t WHERE id = :id OR parent_id = :id").bind_named("id", 42_i32);
        let (rewritten, p) = b.materialize().unwrap();
        assert_eq!(rewritten, "SELECT * FROM t WHERE id = $1 OR parent_id = $1");
        assert_eq!(p.len(), 1);
    }

    #[test]
    fn mixing_positional_then_named_is_config_error() {
        let b = sql("SELECT $1, :name")
            .bind(1_i32)
            .bind_named("name", 2_i32);
        let err = match b.materialize() {
            Ok(_) => panic!("expected error"),
            Err(e) => e,
        };
        assert!(
            matches!(err, TypedError::Config(ref m) if m.contains("cannot mix")),
            "expected Config error, got {err:?}"
        );
    }

    #[test]
    fn mixing_named_then_positional_is_config_error() {
        let b = sql("SELECT :name, $1")
            .bind_named("name", 1_i32)
            .bind(2_i32);
        let err = match b.materialize() {
            Ok(_) => panic!("expected error"),
            Err(e) => e,
        };
        assert!(
            matches!(err, TypedError::Config(ref m) if m.contains("cannot mix")),
            "expected Config error, got {err:?}"
        );
    }

    #[test]
    fn missing_named_value_returns_error() {
        let b = sql("SELECT :a, :b").bind_named("a", 1_i32);
        let err = match b.materialize() {
            Ok(_) => panic!("expected error"),
            Err(e) => e,
        };
        assert!(
            matches!(err, TypedError::MissingParam(ref n) if n == "b"),
            "expected MissingParam(\"b\"), got {err:?}"
        );
    }

    #[test]
    fn invalid_state_sticks_across_subsequent_binds() {
        // Once mismatched, further binds do not clear the error.
        let b = sql("SELECT $1, :name")
            .bind(1_i32)
            .bind_named("name", 2_i32)
            .bind(3_i32)
            .bind_named("other", 4_i32);
        let err = match b.materialize() {
            Ok(_) => panic!("expected error"),
            Err(e) => e,
        };
        assert!(matches!(err, TypedError::Config(_)));
    }
}
