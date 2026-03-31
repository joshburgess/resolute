# resolute

Compile-time checked PostgreSQL queries for Rust with binary-format performance.

resolute validates SQL against a live database at compile time (or offline via cached metadata), generates typed result structs, and executes queries using PostgreSQL's binary wire format.

## Features

- **7 query macros**: `query!`, `query_as!`, `query_scalar!`, `query_file!`, `query_file_as!`, `query_file_scalar!`, `query_unchecked!`
- **Named parameters**: `:name` syntax in both macros and runtime API (not available in sqlx)
- **`Executor` trait**: Write generic functions that work with Client, Transaction, or Pool — no sqlx lifetime gymnastics
- **`atomic()` with savepoint nesting**: Auto-BEGIN on Client, auto-SAVEPOINT on Transaction — same function, correct behavior in any context
- **Custom PG types**: `#[derive(PgEnum)]`, `#[derive(PgComposite)]`, `#[derive(PgDomain)]`
- **Integer-backed enums**: `#[repr(i32)]` on PgEnum for integer column storage
- **Domain type arrays**: `PgDomain` newtypes inherit array OIDs from their inner type
- **Query type overrides**: `"col: CustomType"` syntax in query macros for custom type mapping
- **Rich FromRow derive**: `skip`, `default`, `json`, `try_from`, `flatten` attributes
- **Generic arrays**: `Vec<T>` for all Encode/Decode types (bool, i16, i32, i64, f32, f64, String, UUID, chrono types, JSON, numeric, inet)
- **Pool lifecycle hooks**: `before_acquire`, `on_create`, `on_checkout`, `on_checkin`, `after_release`, `on_destroy`
- **Offline builds**: `.sqlx/` cache + `resolute-cli prepare` for CI/Docker
- **Connection pooling**: `TypedPool` with typed checkout
- **LISTEN/NOTIFY**: `PgListener` for real-time notifications
- **Migrations**: Embedded runner + CLI (create, run, revert, status)
- **Database lifecycle**: `resolute-cli database create/drop`
- **Nullable detection**: Automatic `Option<T>` for nullable columns via `pg_attribute` introspection
- **2-5x faster than sqlx**: Binary encode is 4-5x faster, query latency 2.3-2.5x faster (benchmarked)

## Quick start

```rust
use resolute::{Client, query};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = Client::connect("127.0.0.1:5432", "user", "pass", "mydb").await?;

    // Compile-time checked query (requires DATABASE_URL env var):
    let authors = query!("SELECT id, name FROM authors WHERE id = $1", 1)
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

## Query type overrides

Use `"column_name: RustType"` syntax in SELECT aliases to override the inferred Rust type in `query!` and `query_scalar!` macros. This is useful for mapping columns to custom newtypes:

```rust
#[derive(PgDomain)]
struct UserId(i32);

// Without override: id would be inferred as i32
// With override: id field is typed as UserId
let row = query!(r#"SELECT id as "id: UserId" FROM users WHERE id = $1"#, 1)
    .fetch_one(&client)
    .await?;

// row.id is UserId, not i32
let user_id: UserId = row.id;
```

Type overrides work with nullable columns too — if the column is nullable, the field becomes `Option<UserId>`.

PostgreSQL casts (`::`) work normally and are not affected — `SELECT created_at::text` is a cast, not a type override. The override syntax uses a single `:` inside a quoted alias.

## Executor trait — generic over Client, Transaction, and Pool

Write functions once with `&impl Executor`. They work everywhere — no sqlx lifetime gymnastics, no consuming `self`, multiple queries on the same generic executor.

```rust
use resolute::Executor;

async fn create_user(db: &impl Executor, name: &str) -> Result<i32, resolute::TypedError> {
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
async fn transfer(db: &impl Executor, from: i32, to: i32, amount: i64) -> Result<(), resolute::TypedError> {
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

### String enums

Map to PostgreSQL `CREATE TYPE ... AS ENUM` types:

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

### Integer-backed enums

Store enum values as integers in PostgreSQL (`int2`, `int4`, or `int8` columns):

```rust
#[derive(PgEnum)]
#[repr(i32)]
enum Status {
    Active = 1,
    Inactive = 2,
    Deleted = 3,
}

// Encodes as int4 (4 bytes, big-endian). PgType::OID = 23, ARRAY_OID = 1007.
let mut buf = BytesMut::new();
Status::Active.encode(&mut buf);  // encodes as 1 (i32)

// Decodes from binary or text:
let decoded = Status::decode(&buf)?;      // from binary int4
let decoded = Status::decode_text("2")?;  // from text → Inactive
```

Supported repr types: `#[repr(i16)]` (int2), `#[repr(i32)]` (int4), `#[repr(i64)]` (int8). All variants must have explicit discriminants. Negative values are supported.

**Design note:** sqlx allows `#[sqlx(transparent)]` on `#[repr(i32)]` enums without explicit discriminants, relying on Rust's auto-incrementing discriminant behavior. Resolute requires explicit discriminants intentionally — implicit discriminants are fragile (reordering variants silently changes database values), and the explicitness makes the database mapping unambiguous and auditable.

### Composite types

```rust
#[derive(PgComposite)]
struct Address {
    street: String,
    city: String,
    zip: Option<String>,  // nullable fields use Option<T>
}
```

### Domain types (newtypes)

Transparent wrappers over base PostgreSQL types. All encoding/decoding delegates to the inner type:

```rust
#[derive(PgDomain)]
struct Email(String);

#[derive(PgDomain)]
struct UserId(i64);
```

Domain types automatically inherit the array OID from their inner type, so PostgreSQL knows how to handle them in array context:

```rust
use resolute::PgType;

// Email wraps String (text) → ARRAY_OID = 1009 (text[])
assert_eq!(<Email as PgType>::ARRAY_OID, 1009);

// UserId wraps i64 (int8) → ARRAY_OID = 1016 (int8[])
assert_eq!(<UserId as PgType>::ARRAY_OID, 1016);
```

## FromRow derive

Basic usage with rename and nullable fields:

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

### FromRow attributes

#### `skip` — ignore field, use `Default::default()`

```rust
#[derive(FromRow)]
struct UserView {
    id: i32,
    name: String,
    #[from_row(skip)]
    computed_field: String,  // not read from the row, defaults to ""
}
```

#### `default` — fall back to `Default::default()` if column is missing or NULL

```rust
#[derive(FromRow)]
struct Config {
    id: i32,
    #[from_row(default)]
    retries: i32,  // 0 if column is missing or NULL
    #[from_row(default)]
    label: Option<String>,  // None if column is missing
}
```

#### `json` — deserialize a JSON/JSONB column via serde

```rust
#[derive(FromRow)]
struct Event {
    id: i32,
    #[from_row(json)]
    payload: MyPayload,  // deserialized from jsonb column via serde_json
    #[from_row(json)]
    metadata: Option<Metadata>,  // None if NULL, deserialized if present
}
```

#### `try_from` — decode as one type, convert via `TryFrom`

```rust
struct NonZeroId(i32);

impl TryFrom<i32> for NonZeroId {
    type Error = String;
    fn try_from(v: i32) -> Result<Self, Self::Error> {
        if v == 0 { Err("must be non-zero".into()) }
        else { Ok(NonZeroId(v)) }
    }
}

#[derive(FromRow)]
struct User {
    #[from_row(try_from = "i32")]
    id: NonZeroId,  // decoded as i32, then TryFrom::try_from
    name: String,
}
```

#### `flatten` — embed a nested `FromRow` struct

```rust
#[derive(FromRow)]
struct Address {
    street: String,
    city: String,
}

#[derive(FromRow)]
struct UserWithAddress {
    id: i32,
    name: String,
    #[from_row(flatten)]
    address: Address,  // reads street, city from the same row
}
```

`flatten` shares the same row — the nested struct's column names must not conflict with the outer struct's columns.

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
let user_id: i32 = 1;
let rows = client.query_named("SELECT :id::int4", &[("id", &user_id as &dyn SqlParam)]).await?;
```

## Pool lifecycle hooks

Customize pool behavior with lifecycle hooks. Connection-aware hooks receive a `&C` reference:

```rust
use pg_pool::LifecycleHooks;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

let checkout_count = Arc::new(AtomicU64::new(0));
let cc = Arc::clone(&checkout_count);

let release_count = Arc::new(AtomicU64::new(0));
let rc = Arc::clone(&release_count);

let hooks = LifecycleHooks {
    on_create: Some(Box::new(|_conn| {
        println!("new connection created");
    })),
    before_acquire: Some(Box::new(|| {
        println!("about to check out a connection");
    })),
    on_checkout: Some(Box::new(move |_conn| {
        cc.fetch_add(1, Ordering::Relaxed);
    })),
    on_checkin: Some(Box::new(|_conn| {
        println!("connection returned to pool");
    })),
    after_release: Some(Box::new(move || {
        rc.fetch_add(1, Ordering::Relaxed);
    })),
    on_destroy: Some(Box::new(|| {
        println!("connection destroyed");
    })),
};

let pool = TypedPool::new(config, hooks).await?;
```

| Hook | Parameter | When |
|------|-----------|------|
| `before_acquire` | none | Before checkout starts |
| `on_create` | `&C` | After a new connection is created |
| `on_checkout` | `&C` | When a connection is handed to the caller |
| `on_checkin` | `&C` | When a connection passes health checks on return |
| `after_release` | none | After a connection is fully released (all paths) |
| `on_destroy` | none | When a connection is destroyed (expired/invalid/drain) |

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
use resolute::reconnect::ReconnectingClient;

let client = ReconnectingClient::new(
    "127.0.0.1:5432", "user", "pass", "mydb",
    vec!["SET search_path TO app".into()],
).await?;
// Queries auto-reconnect if the connection drops:
let rows = client.query("SELECT 1", &[]).await?;
```

## Retry policy

```rust
use resolute::retry::RetryPolicy;
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
let output = resolute::metrics::gather();
```

## Test helper

```rust
use resolute::test_db::TestDb;

let db = TestDb::create("127.0.0.1:5432", "postgres", "postgres").await?;
let client = db.client().await?;
// ... run tests ...
db.drop_db().await?;

// Or use the attribute macro:
#[resolute::test]
async fn my_test(client: resolute::Client) {
    // temp database created and dropped automatically
}
```

## Offline builds

```bash
# Populate cache from source files (run with DB available):
resolute-cli prepare --database-url postgres://user:pass@localhost/mydb

# Build without DB (CI/Docker):
RESOLUTE_OFFLINE=true cargo build

# Verify cache is up to date:
resolute-cli check --database-url postgres://user:pass@localhost/mydb
```

## Migrations

```bash
resolute-cli migrate create add_users        # creates timestamped .up.sql + .down.sql
resolute-cli migrate run --database-url ...   # apply pending migrations
resolute-cli migrate revert --database-url ... # revert last migration
resolute-cli migrate status --database-url ... # show applied/pending

resolute-cli database create --database-url ... # create database
resolute-cli database drop --database-url ...   # drop database (--force to kill sessions)
```

Or embed in your application:

```rust
resolute::migrate::run("postgres://user:pass@localhost/mydb", "migrations").await?;
```

## Feature flags

| Feature | Default | Enables |
|---------|---------|---------|
| `chrono` | yes | `NaiveDate`, `NaiveTime`, `NaiveDateTime`, `DateTime<Utc>` |
| `json` | yes | `serde_json::Value` for JSON/JSONB |
| `uuid` | yes | `uuid::Uuid` |

## Design decisions

**PostgreSQL only.** Resolute does not have an `Any` database abstraction or multi-database support. It is built from the ground up for PostgreSQL — the wire protocol, type system, OID mappings, and query semantics are all PostgreSQL-specific. This is intentional: a single-database library can leverage PostgreSQL features fully (range types, advisory locks, LISTEN/NOTIFY, custom enums, composite types, binary protocol) without lowest-common-denominator abstractions.

**Explicit integer enum discriminants.** Integer-backed enums require `= N` on every variant. This prevents silent breakage when variants are reordered or inserted.

**OID = 0 for custom types.** `PgEnum`, `PgComposite`, and `PgDomain` all set `OID = 0` (Unspecified), letting PostgreSQL infer the type from context (column type, cast, etc.). This avoids requiring users to know or hardcode OIDs, at the cost of less precise error messages when types mismatch.

**Non-consuming Executor.** The `Executor` trait uses `&self` instead of consuming `self`. This is a deliberate departure from sqlx, enabling natural multi-query reuse in generic functions without lifetime gymnastics.
