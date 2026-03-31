# stalwart

Compile-time checked PostgreSQL queries for Rust with binary-format performance.

stalwart validates SQL against a live database at compile time (or offline via cached metadata), generates typed result structs, and executes queries using PostgreSQL's binary wire format.

## Features

- **7 query macros**: `query!`, `query_as!`, `query_scalar!`, `query_file!`, `query_file_as!`, `query_file_scalar!`, `query_unchecked!`
- **Named parameters**: `:name` syntax in both macros and runtime API (not available in sqlx)
- **`Executor` trait**: Write generic functions that work with Client, Transaction, or Pool — no sqlx lifetime gymnastics
- **`atomic()` with savepoint nesting**: Auto-BEGIN on Client, auto-SAVEPOINT on Transaction — same function, correct behavior in any context
- **Custom PG types**: `#[derive(PgEnum)]`, `#[derive(PgComposite)]`, `#[derive(PgDomain)]`
- **Generic arrays**: `Vec<T>` for all Encode/Decode types (bool, i16, i32, i64, f32, f64, String, UUID, chrono types, JSON, numeric, inet)
- **Offline builds**: `.sqlx/` cache + `stalwart-cli prepare` for CI/Docker
- **Connection pooling**: `TypedPool` with typed checkout
- **LISTEN/NOTIFY**: `PgListener` for real-time notifications
- **Migrations**: Embedded runner + CLI (create, run, revert, status)
- **Database lifecycle**: `stalwart-cli database create/drop`
- **Nullable detection**: Automatic `Option<T>` for nullable columns via `pg_attribute` introspection
- **2-5x faster than sqlx**: Binary encode is 4-5x faster, query latency 2.3-2.5x faster (benchmarked)

## Quick start

```rust
use stalwart::{Client, query};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = Client::connect("127.0.0.1:5432", "user", "pass", "mydb").await?;

    // Compile-time checked query (requires DATABASE_URL env var):
    let authors = query!("SELECT id, name FROM authors WHERE id = $1", 1i32)
        .fetch_all(&client)
        .await?;

    for a in &authors {
        println!("{}: {}", a.id, a.name);
    }
    Ok(())
}
```

## Named parameters

Use `:name` instead of `$1, $2, ...`. Duplicates reuse the same positional slot. `::` casts, string literals, and comments are handled correctly.

```rust
// Compile-time macro:
let row = query!(
    "SELECT * FROM users WHERE org = :org AND id = :id",
    org = org_id,
    id = user_id,
).fetch_one(&client).await?;

// Runtime API:
let rows = client.query_named(
    "SELECT * FROM users WHERE org = :org AND id = :id",
    &[
        ("org", &org_id as &dyn SqlParam),
        ("id", &user_id as &dyn SqlParam),
    ],
).await?;

// Duplicates — :id appears twice, bound once:
let rows = client.query_named(
    "SELECT * FROM t WHERE id = :id OR parent_id = :id",
    &[("id", &42i32 as &dyn SqlParam)],
).await?;
```

## Executor trait — generic over Client, Transaction, and Pool

Write functions once with `&impl Executor`. They work everywhere — no sqlx lifetime gymnastics, no consuming `self`, multiple queries on the same generic executor.

```rust
use stalwart::Executor;

async fn create_user(db: &impl Executor, name: &str) -> Result<i32, stalwart::TypedError> {
    let rows = db.query(
        "INSERT INTO users (name) VALUES ($1) RETURNING id",
        &[&name.to_string()],
    ).await?;
    rows[0].get(0)
}

// All of these work:
create_user(&client, "Alice").await?;
create_user(&txn, "Alice").await?;
create_user(&pooled_client, "Alice").await?;
```

## Transactions

### Manual transactions

```rust
let txn = client.begin().await?;
create_user(&txn, "Alice").await?;
create_profile(&txn, user_id).await?;
txn.commit().await?;
```

### Closure-based transactions

```rust
client.with_transaction(|db| Box::pin(async move {
    create_user(db, "Alice").await?;
    create_profile(db, user_id).await?;
    Ok(user_id)
})).await?;  // auto-commit on Ok, auto-rollback on Err
```

### `atomic()` — context-aware atomicity

Write functions that always run atomically, regardless of whether the caller already has a transaction:

```rust
async fn transfer(db: &impl Executor, from: i32, to: i32, amount: i64) -> Result<(), stalwart::TypedError> {
    db.atomic(|db| Box::pin(async move {
        db.execute("UPDATE accounts SET balance = balance - $1 WHERE id = $2", &[&amount, &from]).await?;
        db.execute("UPDATE accounts SET balance = balance + $1 WHERE id = $2", &[&amount, &to]).await?;
        Ok(())
    })).await
}

// Called with Client → uses BEGIN/COMMIT:
transfer(&client, 1, 2, 100).await?;

// Called inside a transaction → uses SAVEPOINT (nested, composable):
let txn = client.begin().await?;
transfer(&txn, 1, 2, 100).await?;  // SAVEPOINT, not a nested BEGIN
other_work(&txn).await?;
txn.commit().await?;
```

## Custom PostgreSQL types

### Enums

```rust
#[derive(PgEnum)]
#[pg_type(rename_all = "snake_case")]  // default
enum Mood {
    Happy,
    Sad,
    #[pg_type(rename = "so-so")]
    SoSo,
}
```

Supported `rename_all` strategies: `snake_case`, `lowercase`, `UPPERCASE`, `SCREAMING_SNAKE_CASE`, `camelCase`, `PascalCase`, `kebab-case`.

### Composite types

```rust
#[derive(PgComposite)]
struct Address {
    street: String,
    city: String,
    zip: Option<String>,  // nullable fields use Option<T>
}
```

### Domain types

```rust
#[derive(PgDomain)]
struct Email(String);

#[derive(PgDomain)]
struct PositiveInt(i32);
```

## FromRow derive

```rust
#[derive(FromRow)]
struct Author {
    id: i32,
    name: String,
    #[from_row(rename = "email_address")]
    email: String,
    bio: Option<String>,
}
```

## Array types

All types with Encode + Decode support generic `Vec<T>` arrays:

```rust
let tags: Vec<String> = vec!["rust".into(), "postgres".into()];
let rows = client.query("SELECT $1::text[] AS arr", &[&tags]).await?;
let result: Vec<String> = rows[0].get(0)?;
```

Supported: `Vec<bool>`, `Vec<i16>`, `Vec<i32>`, `Vec<i64>`, `Vec<f32>`, `Vec<f64>`, `Vec<String>`, `Vec<uuid::Uuid>`, `Vec<chrono::NaiveDate>`, `Vec<chrono::NaiveTime>`, `Vec<chrono::NaiveDateTime>`, `Vec<chrono::DateTime<Utc>>`, `Vec<serde_json::Value>`, `Vec<PgNumeric>`, `Vec<PgInet>`.

## Connection pool

```rust
let pool = TypedPool::connect("127.0.0.1:5432", "user", "pass", "mydb", 10).await?;
let client = pool.get().await?;
let rows = client.query("SELECT 1::int4 AS n", &[]).await?;
// Named params work through the pool too:
let rows = client.query_named("SELECT :id::int4", &[("id", &1i32 as &dyn SqlParam)]).await?;
```

## Streaming queries

Process large result sets row-by-row without buffering:

```rust
use tokio_stream::StreamExt;

let mut stream = client.query_stream("SELECT * FROM large_table", &[]).await?;
while let Some(row) = stream.next().await {
    let row = row?;
    let id: i32 = row.get(0)?;
    // process row...
}
```

## Timeouts and cancellation

```rust
use std::time::Duration;

// Auto-cancel via CancelRequest if timeout exceeded:
let rows = client.query_timeout("SELECT pg_sleep(60)", &[], Duration::from_secs(5)).await;

// Manual cancellation from another task:
let token = client.cancel_token();
tokio::spawn(async move { token.cancel().await.ok(); });
```

## Pipelining

Batch multiple queries in one network round-trip:

```rust
let results = client.pipeline()
    .query("SELECT 1::int4", &[])
    .execute("INSERT INTO t VALUES ($1)", &[&42i32])
    .query("SELECT count(*)::int4 FROM t", &[])
    .run()
    .await?;
```

## Bulk data loading (COPY)

```rust
// COPY IN: bulk import from CSV
let csv = b"1,Alice\n2,Bob\n";
let count = client.copy_in("COPY users FROM STDIN WITH (FORMAT csv)", csv).await?;

// COPY OUT: bulk export
let data = client.copy_out("COPY users TO STDOUT WITH (FORMAT csv)").await?;
```

## Auto-reconnecting client

```rust
use stalwart::reconnect::ReconnectingClient;

let client = ReconnectingClient::new(
    "127.0.0.1:5432", "user", "pass", "mydb",
    vec!["SET search_path TO app".into()],
).await?;
// Queries auto-reconnect if the connection drops:
let rows = client.query("SELECT 1", &[]).await?;
```

## Retry policy

```rust
use stalwart::retry::RetryPolicy;
use std::time::Duration;

let policy = RetryPolicy::new(3, Duration::from_millis(100));
let rows = policy.execute(&client, |db| Box::pin(async move {
    db.query("SELECT * FROM orders", &[]).await
})).await?;
```

## Infinity handling

PostgreSQL supports `'infinity'` and `'-infinity'` for dates and timestamps. Use `PgTimestamp` and `PgDate` instead of chrono types when your data may contain these:

```rust
let rows = client.query("SELECT 'infinity'::timestamp AS ts", &[]).await?;
let ts: PgTimestamp = rows[0].get(0)?;
assert_eq!(ts, PgTimestamp::Infinity);
```

## Pool warm-up and metrics

```rust
let pool = TypedPool::connect("127.0.0.1:5432", "user", "pass", "mydb", 10).await?;
pool.warm_up(5).await;  // pre-create 5 connections

// Application metrics (Prometheus format):
let output = stalwart::metrics::gather();
```

## Test helper

```rust
use stalwart::test_db::TestDb;

let db = TestDb::create("127.0.0.1:5432", "postgres", "postgres").await?;
let client = db.client().await?;
// ... run tests ...
db.drop_db().await?;

// Or use the attribute macro:
#[stalwart::test]
async fn my_test(client: stalwart::Client) {
    // temp database created and dropped automatically
}
```

## Offline builds

```bash
# Populate cache from source files (run with DB available):
stalwart-cli prepare --database-url postgres://user:pass@localhost/mydb

# Build without DB (CI/Docker):
STALWART_OFFLINE=true cargo build

# Verify cache is up to date:
stalwart-cli check --database-url postgres://user:pass@localhost/mydb
```

## Migrations

```bash
stalwart-cli migrate create add_users        # creates timestamped .up.sql + .down.sql
stalwart-cli migrate run --database-url ...   # apply pending migrations
stalwart-cli migrate revert --database-url ... # revert last migration
stalwart-cli migrate status --database-url ... # show applied/pending

stalwart-cli database create --database-url ... # create database
stalwart-cli database drop --database-url ...   # drop database (--force to kill sessions)
```

Or embed in your application:

```rust
stalwart::migrate::run("postgres://user:pass@localhost/mydb", "migrations").await?;
```

## Feature flags

| Feature | Default | Enables |
|---------|---------|---------|
| `chrono` | yes | `NaiveDate`, `NaiveTime`, `NaiveDateTime`, `DateTime<Utc>` |
| `json` | yes | `serde_json::Value` for JSON/JSONB |
| `uuid` | yes | `uuid::Uuid` |
