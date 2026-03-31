//! Compile-time checked PostgreSQL queries with binary-format performance.
//!
//! stalwart validates SQL against a live database at compile time (or offline
//! via cached metadata), generates typed result structs, and executes queries
//! using PostgreSQL's binary wire format for zero-overhead type mapping.
//!
//! # Quick start
//!
//! ```ignore
//! use stalwart::{Client, query};
//!
//! let client = Client::connect("127.0.0.1:5432", "user", "pass", "mydb").await?;
//!
//! // Compile-time checked with positional params:
//! let row = query!("SELECT id, name FROM users WHERE id = $1", user_id)
//!     .fetch_one(&client)
//!     .await?;
//! println!("{}: {}", row.id, row.name);
//!
//! // Named parameters (unique to stalwart, not available in sqlx):
//! let row = query!("SELECT id, name FROM users WHERE id = :id", id = user_id)
//!     .fetch_one(&client)
//!     .await?;
//! ```
//!
//! # Named parameters
//!
//! Both compile-time macros and runtime methods support `:name` syntax.
//! Named params are rewritten to `$1, $2, ...` before hitting PostgreSQL.
//! Duplicate names reuse the same positional slot. `::` casts, string
//! literals, and comments are handled correctly.
//!
//! ```ignore
//! // Compile-time:
//! query!("SELECT :val::int4 WHERE :val > 0", val = my_var)
//!
//! // Runtime:
//! client.query_named(
//!     "SELECT :id::int4 AS n",
//!     &[("id", &42i32 as &dyn SqlParam)],
//! ).await?;
//! ```
//!
//! # Executor trait — generic over connection types
//!
//! Write functions once with `&impl Executor`. Unlike sqlx (which consumes `self`),
//! stalwart's Executor uses `&self` — multi-query reuse just works.
//!
//! ```ignore
//! async fn create_user(db: &impl Executor, name: &str) -> Result<i32, TypedError> {
//!     let rows = db.query("INSERT INTO users (name) VALUES ($1) RETURNING id", &[&name.to_string()]).await?;
//!     rows[0].get(0)
//! }
//!
//! create_user(&client, "Alice").await?;  // Client
//! create_user(&txn, "Alice").await?;     // Transaction
//! create_user(&pooled, "Alice").await?;  // Pool
//! ```
//!
//! # Context-aware atomicity
//!
//! `db.atomic(|db| ...)` does `BEGIN/COMMIT` on Client, `SAVEPOINT/RELEASE` on
//! Transaction. Same function, correct behavior in any context.
//!
//! ```ignore
//! async fn transfer(db: &impl Executor, from: i32, to: i32) -> Result<(), TypedError> {
//!     db.atomic(|db| Box::pin(async move {
//!         db.execute("UPDATE accounts SET balance = balance - 100 WHERE id = $1", &[&from]).await?;
//!         db.execute("UPDATE accounts SET balance = balance + 100 WHERE id = $1", &[&to]).await?;
//!         Ok(())
//!     })).await
//! }
//! ```
//!
//! # Custom PostgreSQL types
//!
//! ```ignore
//! #[derive(PgEnum)]
//! #[pg_type(rename_all = "snake_case")]
//! enum Mood { Happy, Sad }
//!
//! #[derive(PgComposite)]
//! struct Address { street: String, city: String, zip: Option<String> }
//!
//! #[derive(PgDomain)]
//! struct Email(String);
//! ```
//!
//! # Performance over sqlx
//!
//! - Binary format: PG sends raw bytes, no text-to-number parsing
//! - Message coalescing: multiple queries batched into one write() syscall
//! - Statement caching: Parse once, Bind+Execute on reuse
//! - Generic array encode/decode for all types via `Vec<T>`

mod checked;
mod decode;
mod encode;
mod error;
mod executor;
mod listener;
pub mod metrics;
pub mod migrate;
pub mod named_params;
pub mod newtypes;
mod oid;
pub mod pg_type;
mod pooled;
mod query;
pub mod reconnect;
pub mod retry;
mod row;
pub mod test_db;
mod types;

pub use checked::{CheckedQuery, UncheckedQuery};
pub use decode::{Decode, DecodeText};
pub use encode::{Encode, SqlParam};
pub use error::TypedError;
pub use executor::Executor;
pub use oid::TypeOid;
pub use pg_type::PgType;
pub use query::{Client, PipelineResult, RowStream, Transaction};
pub use row::{Row, FromRow};
pub use bytes::BytesMut;
/// Derive macro for `FromRow`. Use `#[derive(stalwart::FromRow)]` on structs.
pub use stalwart_derive::FromRow;
/// Derive macro for PostgreSQL enum types.
pub use stalwart_derive::PgEnum;
/// Derive macro for PostgreSQL composite types.
pub use stalwart_derive::PgComposite;
/// Derive macro for PostgreSQL domain types (newtypes over a base type).
pub use stalwart_derive::PgDomain;
/// Attribute macro for database-backed tests. Auto-creates a temp DB,
/// runs migrations, injects a `Client`, and drops the DB on completion.
pub use stalwart_derive::test;
/// Compile-time checked query macro. Requires `DATABASE_URL` env var.
pub use stalwart_macros::query;
/// Compile-time checked query mapped to an existing struct via FromRow.
pub use stalwart_macros::query_as;
/// Compile-time checked single-scalar query.
pub use stalwart_macros::query_scalar;
/// Like query! but reads SQL from a file.
pub use stalwart_macros::query_file;
/// Like query_as! but reads SQL from a file.
pub use stalwart_macros::query_file_as;
/// Like query_scalar! but reads SQL from a file.
pub use stalwart_macros::query_file_scalar;
/// Skip compile-time checking (no DATABASE_URL or cache needed).
pub use stalwart_macros::query_unchecked;
pub use types::TypeInfo;
pub use newtypes::{PgNumeric, PgInet, PgTimestamp, PgDate};
pub use listener::{PgListener, Notification};
pub use pooled::{TypedPool, PooledTypedClient};
pub use pg_wire::CancelToken;
