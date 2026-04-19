//! Unit tests for resolute components that don't require a database.

use resolute::{Decode, DecodeText, Encode, PgType};

// ---------------------------------------------------------------------------
// PgRange encode/decode
// ---------------------------------------------------------------------------

use resolute::PgRange;

#[test]
fn test_range_empty_encode_decode() {
    let r: PgRange<i32> = PgRange::empty();
    let mut buf = bytes::BytesMut::new();
    r.encode(&mut buf);
    let decoded = PgRange::<i32>::decode(&buf).unwrap();
    assert_eq!(decoded, PgRange::Empty);
}

#[test]
fn test_range_inclusive_exclusive() {
    let r = PgRange::new(Some(1i32), Some(10i32), true, false);
    let mut buf = bytes::BytesMut::new();
    r.encode(&mut buf);
    let decoded = PgRange::<i32>::decode(&buf).unwrap();
    assert_eq!(decoded, r);
}

#[test]
fn test_range_both_inclusive() {
    let r = PgRange::new(Some(5i64), Some(100i64), true, true);
    let mut buf = bytes::BytesMut::new();
    r.encode(&mut buf);
    let decoded = PgRange::<i64>::decode(&buf).unwrap();
    assert_eq!(decoded, r);
}

#[test]
fn test_range_both_exclusive() {
    let r = PgRange::new(Some(-10i32), Some(10i32), false, false);
    let mut buf = bytes::BytesMut::new();
    r.encode(&mut buf);
    let decoded = PgRange::<i32>::decode(&buf).unwrap();
    assert_eq!(decoded, r);
}

#[test]
fn test_range_unbounded_lower() {
    let r: PgRange<i32> = PgRange::new(None, Some(100), false, true);
    let mut buf = bytes::BytesMut::new();
    r.encode(&mut buf);
    let decoded = PgRange::<i32>::decode(&buf).unwrap();
    assert_eq!(decoded, r);
}

#[test]
fn test_range_unbounded_upper() {
    let r: PgRange<i32> = PgRange::new(Some(0), None, true, false);
    let mut buf = bytes::BytesMut::new();
    r.encode(&mut buf);
    let decoded = PgRange::<i32>::decode(&buf).unwrap();
    assert_eq!(decoded, r);
}

#[test]
fn test_range_fully_unbounded() {
    let r: PgRange<i64> = PgRange::new(None, None, false, false);
    let mut buf = bytes::BytesMut::new();
    r.encode(&mut buf);
    let decoded = PgRange::<i64>::decode(&buf).unwrap();
    assert_eq!(decoded, r);
}

#[test]
fn test_range_is_empty() {
    assert!(PgRange::<i32>::empty().is_empty());
    assert!(!PgRange::new(Some(1i32), Some(10i32), true, false).is_empty());
}

#[test]
fn test_range_decode_text_empty() {
    let r = PgRange::<i32>::decode_text("empty").unwrap();
    assert_eq!(r, PgRange::Empty);
}

#[test]
fn test_range_decode_text_inclusive_exclusive() {
    let r = PgRange::<i32>::decode_text("[1,10)").unwrap();
    if let PgRange::Range {
        lower,
        upper,
        lower_inclusive,
        upper_inclusive,
    } = r
    {
        assert_eq!(lower, Some(1));
        assert_eq!(upper, Some(10));
        assert!(lower_inclusive);
        assert!(!upper_inclusive);
    } else {
        panic!("expected Range");
    }
}

#[test]
fn test_range_decode_text_both_inclusive() {
    let r = PgRange::<i32>::decode_text("[1,10]").unwrap();
    if let PgRange::Range {
        lower_inclusive,
        upper_inclusive,
        ..
    } = r
    {
        assert!(lower_inclusive);
        assert!(upper_inclusive);
    } else {
        panic!("expected Range");
    }
}

#[test]
fn test_range_decode_text_unbounded() {
    let r = PgRange::<i32>::decode_text("[,10)").unwrap();
    if let PgRange::Range { lower, upper, .. } = r {
        assert_eq!(lower, None);
        assert_eq!(upper, Some(10));
    } else {
        panic!("expected Range");
    }
}

#[test]
fn test_range_decode_empty_buffer() {
    let err = PgRange::<i32>::decode(&[]).unwrap_err();
    let msg = format!("{err}");
    assert!(msg.contains("empty buffer"), "got: {msg}");
}

#[test]
fn test_range_pg_type_oids() {
    assert_eq!(<PgRange<i32> as PgType>::OID, 3904); // int4range
    assert_eq!(<PgRange<i64> as PgType>::OID, 3926); // int8range
}

// ---------------------------------------------------------------------------
// Metrics
// ---------------------------------------------------------------------------

#[test]
fn test_metrics_record_and_snapshot() {
    // Note: metrics are global statics, so these tests may see values
    // from other tests. We test relative changes, not absolute values.
    let before = resolute::metrics::snapshot();

    resolute::metrics::record_query(100);
    resolute::metrics::record_query(200);
    resolute::metrics::record_query_error();
    resolute::metrics::record_execute();
    resolute::metrics::record_execute_error();
    resolute::metrics::record_pool_checkout();
    resolute::metrics::record_pool_timeout();

    let after = resolute::metrics::snapshot();

    // Use >= because other tests (timed_query) may also increment query_count.
    assert!(after.query_count - before.query_count >= 2);
    assert_eq!(after.query_error_count - before.query_error_count, 1);
    assert!(after.query_duration_us_sum - before.query_duration_us_sum >= 300);
    assert!(after.execute_count - before.execute_count >= 1);
    assert_eq!(after.execute_error_count - before.execute_error_count, 1);
    assert_eq!(after.pool_checkout_count - before.pool_checkout_count, 1);
    assert_eq!(after.pool_timeout_count - before.pool_timeout_count, 1);
}

#[test]
fn test_metrics_gather_prometheus_format() {
    let output = resolute::metrics::gather();
    assert!(
        output.contains("resolute_queries_total"),
        "missing queries counter"
    );
    assert!(
        output.contains("resolute_query_errors_total"),
        "missing errors counter"
    );
    assert!(
        output.contains("resolute_query_duration_us_avg"),
        "missing duration gauge"
    );
    assert!(
        output.contains("resolute_executes_total"),
        "missing executes counter"
    );
    assert!(
        output.contains("resolute_pool_checkouts_total"),
        "missing pool checkouts"
    );
    assert!(
        output.contains("resolute_pool_timeouts_total"),
        "missing pool timeouts"
    );
    assert!(output.contains("# HELP"), "missing HELP lines");
    assert!(output.contains("# TYPE"), "missing TYPE lines");
}

#[test]
fn test_metrics_timed_query() {
    let before = resolute::metrics::snapshot();
    let result = resolute::metrics::timed_query(|| 42);
    assert_eq!(result, 42);
    let after = resolute::metrics::snapshot();
    assert_eq!(after.query_count - before.query_count, 1);
}

// ---------------------------------------------------------------------------
// RetryPolicy (unit-level — no DB)
// ---------------------------------------------------------------------------

#[test]
fn test_retry_policy_creation() {
    use resolute::retry::RetryPolicy;
    use std::time::Duration;

    let policy = RetryPolicy::new(3, Duration::from_millis(100));
    // Just verify it constructs without panic.
    let _ = policy.with_max_backoff(Duration::from_secs(5));
}

// ---------------------------------------------------------------------------
// PgComposite roundtrip
// ---------------------------------------------------------------------------

#[derive(Debug, PartialEq, resolute::PgComposite)]
struct TestComposite {
    name: String,
    value: i32,
}

#[test]
fn test_pg_composite_encode_decode() {
    let val = TestComposite {
        name: "hello".into(),
        value: 42,
    };
    let mut buf = bytes::BytesMut::new();
    val.encode(&mut buf);
    let decoded = TestComposite::decode(&buf).unwrap();
    assert_eq!(decoded, val);
}

#[derive(Debug, PartialEq, resolute::PgComposite)]
struct TestCompositeOptional {
    name: String,
    notes: Option<String>,
}

#[test]
fn test_pg_composite_with_option_some() {
    let val = TestCompositeOptional {
        name: "test".into(),
        notes: Some("note".into()),
    };
    let mut buf = bytes::BytesMut::new();
    val.encode(&mut buf);
    let decoded = TestCompositeOptional::decode(&buf).unwrap();
    assert_eq!(decoded, val);
}

#[test]
fn test_pg_composite_with_option_none() {
    let val = TestCompositeOptional {
        name: "test".into(),
        notes: None,
    };
    let mut buf = bytes::BytesMut::new();
    val.encode(&mut buf);
    let decoded = TestCompositeOptional::decode(&buf).unwrap();
    assert_eq!(decoded, val);
}

#[derive(Debug, PartialEq, resolute::PgComposite)]
struct TestCompositeMultipleOptional {
    a: Option<i32>,
    b: String,
    c: Option<String>,
}

#[test]
fn test_pg_composite_multiple_optional_fields() {
    let val = TestCompositeMultipleOptional {
        a: None,
        b: "x".into(),
        c: Some("y".into()),
    };
    let mut buf = bytes::BytesMut::new();
    val.encode(&mut buf);
    let decoded = TestCompositeMultipleOptional::decode(&buf).unwrap();
    assert_eq!(decoded, val);
}

#[test]
fn test_pg_composite_all_none() {
    let val = TestCompositeMultipleOptional {
        a: None,
        b: "".into(),
        c: None,
    };
    let mut buf = bytes::BytesMut::new();
    val.encode(&mut buf);
    let decoded = TestCompositeMultipleOptional::decode(&buf).unwrap();
    assert_eq!(decoded, val);
}

// ---------------------------------------------------------------------------
// PgEnum string-backed
// ---------------------------------------------------------------------------

#[derive(Debug, PartialEq, resolute::PgEnum)]
enum TestColor {
    Red,
    Green,
    Blue,
}

#[test]
fn test_string_enum_encode_decode() {
    for (variant, label) in [
        (TestColor::Red, "red"),
        (TestColor::Green, "green"),
        (TestColor::Blue, "blue"),
    ] {
        let mut buf = bytes::BytesMut::new();
        variant.encode(&mut buf);
        assert_eq!(&buf[..], label.as_bytes());
        assert_eq!(TestColor::decode(label.as_bytes()).unwrap(), variant);
    }
}

#[test]
fn test_string_enum_decode_text() {
    assert_eq!(TestColor::decode_text("red").unwrap(), TestColor::Red);
    assert_eq!(TestColor::decode_text("green").unwrap(), TestColor::Green);
}

#[test]
fn test_string_enum_decode_unknown() {
    let err = TestColor::decode(b"purple").unwrap_err();
    let msg = format!("{err}");
    assert!(msg.contains("unknown"), "got: {msg}");
}

#[derive(Debug, PartialEq, resolute::PgEnum)]
#[pg_type(rename_all = "UPPERCASE")]
enum TestDirection {
    North,
    South,
    East,
    West,
}

#[test]
fn test_string_enum_rename_all() {
    let mut buf = bytes::BytesMut::new();
    TestDirection::North.encode(&mut buf);
    assert_eq!(&buf[..], b"NORTH");
}

// ---------------------------------------------------------------------------
// PgDomain with custom OIDs
// ---------------------------------------------------------------------------

#[derive(Debug, PartialEq, resolute::PgDomain)]
#[pg_type(oid = 99999, array_oid = 99998)]
struct CustomOidType(String);

#[test]
fn test_pg_domain_custom_oids() {
    assert_eq!(<CustomOidType as PgType>::OID, 99999);
    assert_eq!(<CustomOidType as PgType>::ARRAY_OID, 99998);
}

#[derive(Debug, PartialEq, resolute::PgDomain)]
#[pg_type(oid = 88888)]
struct CustomOidNoArray(i32);

#[test]
fn test_pg_domain_custom_oid_inherits_array() {
    assert_eq!(<CustomOidNoArray as PgType>::OID, 88888);
    // array_oid not specified → inherits from i32 (1007)
    assert_eq!(<CustomOidNoArray as PgType>::ARRAY_OID, 1007);
}

// ---------------------------------------------------------------------------
// CheckedQuery/UncheckedQuery
// ---------------------------------------------------------------------------

#[test]
fn test_unchecked_query_construction() {
    // UncheckedQuery can be constructed without a database.
    let _q = resolute::UncheckedQuery {
        sql: "SELECT 1",
        params: vec![],
    };
}

// ---------------------------------------------------------------------------
// IsolationLevel
// ---------------------------------------------------------------------------

#[test]
fn test_isolation_level_as_sql() {
    use resolute::IsolationLevel;
    assert_eq!(IsolationLevel::ReadCommitted.as_sql(), "READ COMMITTED");
    assert_eq!(IsolationLevel::RepeatableRead.as_sql(), "REPEATABLE READ");
    assert_eq!(IsolationLevel::Serializable.as_sql(), "SERIALIZABLE");
}

// ---------------------------------------------------------------------------
// Row has_column
// ---------------------------------------------------------------------------

// Can't construct Row directly (pub(crate) fields), but tested via
// integration tests in typed_test.rs with real queries.

// ---------------------------------------------------------------------------
// parse_connection_string edge cases
// ---------------------------------------------------------------------------

#[test]
fn test_parse_connection_string_with_query_params() {
    let result = resolute::parse_connection_string(
        "postgres://user:pass@host:5432/db?sslmode=require&connect_timeout=10",
    );
    let (user, pass, host, port, db) = result.unwrap();
    assert_eq!(user, "user");
    assert_eq!(pass, "pass");
    assert_eq!(host, "host");
    assert_eq!(port, 5432);
    assert_eq!(db, "db");
}

#[test]
fn test_parse_connection_string_postgresql_scheme() {
    let result = resolute::parse_connection_string("postgresql://u:p@h/d");
    assert!(result.is_some());
    let (user, _, _, _, db) = result.unwrap();
    assert_eq!(user, "u");
    assert_eq!(db, "d");
}

#[test]
fn test_parse_connection_string_keyvalue_minimal() {
    // key=value format accepts minimal input with defaults.
    let result = resolute::parse_connection_string("dbname=mydb");
    assert!(result.is_some());
    let (_, _, _, _, db) = result.unwrap();
    assert_eq!(db, "mydb");
}
