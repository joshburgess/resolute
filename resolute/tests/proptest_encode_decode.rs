//! Property-based tests: encode → decode roundtrip for all types.
//! Generates random values and verifies that encode(decode(x)) == x.

use resolute::{Decode, DecodeText, Encode};
use proptest::prelude::*;

/// Encode then decode, assert equal.
fn roundtrip<T: Encode + Decode + PartialEq + std::fmt::Debug>(val: &T) {
    let mut buf = bytes::BytesMut::new();
    val.encode(&mut buf);
    let decoded = T::decode(&buf).expect("decode failed");
    assert_eq!(&decoded, val, "roundtrip mismatch");
}

// ---------------------------------------------------------------------------
// Primitives
// ---------------------------------------------------------------------------

proptest! {
    #[test]
    fn prop_bool(v in any::<bool>()) {
        roundtrip(&v);
    }

    #[test]
    fn prop_i16(v in any::<i16>()) {
        roundtrip(&v);
    }

    #[test]
    fn prop_i32(v in any::<i32>()) {
        roundtrip(&v);
    }

    #[test]
    fn prop_i64(v in any::<i64>()) {
        roundtrip(&v);
    }

    #[test]
    fn prop_f32(v in any::<f32>()) {
        let mut buf = bytes::BytesMut::new();
        v.encode(&mut buf);
        let decoded = f32::decode(&buf).unwrap();
        // NaN != NaN, so check bits.
        assert_eq!(v.to_bits(), decoded.to_bits(), "f32 roundtrip mismatch: {v} != {decoded}");
    }

    #[test]
    fn prop_f64(v in any::<f64>()) {
        let mut buf = bytes::BytesMut::new();
        v.encode(&mut buf);
        let decoded = f64::decode(&buf).unwrap();
        assert_eq!(v.to_bits(), decoded.to_bits(), "f64 roundtrip mismatch: {v} != {decoded}");
    }

    #[test]
    fn prop_string(v in ".*") {
        roundtrip(&v);
    }

    #[test]
    fn prop_bytes(v in proptest::collection::vec(any::<u8>(), 0..1024)) {
        roundtrip(&v);
    }
}

// ---------------------------------------------------------------------------
// Arrays
// ---------------------------------------------------------------------------

proptest! {
    #[test]
    fn prop_vec_i32(v in proptest::collection::vec(any::<i32>(), 0..100)) {
        roundtrip(&v);
    }

    #[test]
    fn prop_vec_i64(v in proptest::collection::vec(any::<i64>(), 0..100)) {
        roundtrip(&v);
    }

    #[test]
    fn prop_vec_bool(v in proptest::collection::vec(any::<bool>(), 0..100)) {
        roundtrip(&v);
    }

    #[test]
    fn prop_vec_string(v in proptest::collection::vec(".*", 0..20)) {
        roundtrip(&v);
    }
}

// ---------------------------------------------------------------------------
// Chrono types
// ---------------------------------------------------------------------------

proptest! {
    #[test]
    fn prop_naive_date(
        y in 1i32..5000,
        m in 1u32..=12,
        d in 1u32..=28,
    ) {
        if let Some(date) = chrono::NaiveDate::from_ymd_opt(y, m, d) {
            roundtrip(&date);
        }
    }

    #[test]
    fn prop_naive_time(
        h in 0u32..24,
        m in 0u32..60,
        s in 0u32..60,
        us in 0u32..1_000_000,
    ) {
        if let Some(time) = chrono::NaiveTime::from_hms_micro_opt(h, m, s, us) {
            roundtrip(&time);
        }
    }
}

// ---------------------------------------------------------------------------
// UUID
// ---------------------------------------------------------------------------

proptest! {
    #[test]
    fn prop_uuid(bytes in proptest::collection::vec(any::<u8>(), 16..=16)) {
        let arr: [u8; 16] = bytes.try_into().unwrap();
        let id = uuid::Uuid::from_bytes(arr);
        roundtrip(&id);
    }
}

// ---------------------------------------------------------------------------
// Text array parser fuzz (must not panic on arbitrary input)
// ---------------------------------------------------------------------------

proptest! {
    #[test]
    fn fuzz_text_array_parser(s in ".*") {
        // Must not panic. Errors are fine.
        let _ = Vec::<String>::decode_text(&s);
    }

    #[test]
    fn fuzz_named_param_rewriter(s in ".*") {
        // Must not panic on arbitrary SQL-like input.
        let _ = resolute::named_params::rewrite(&s);
    }
}

// ---------------------------------------------------------------------------
// PgDomain newtypes — property-based roundtrip
// ---------------------------------------------------------------------------

#[derive(Debug, PartialEq, resolute::PgDomain)]
struct TestDomainI32(i32);

#[derive(Debug, PartialEq, resolute::PgDomain)]
struct TestDomainI64(i64);

#[derive(Debug, PartialEq, resolute::PgDomain)]
struct TestDomainString(String);

#[derive(Debug, PartialEq, resolute::PgDomain)]
struct TestDomainBool(bool);

#[derive(Debug, PartialEq, resolute::PgDomain)]
struct TestDomainF64(f64);

proptest! {
    #[test]
    fn prop_domain_i32(v in any::<i32>()) {
        roundtrip(&TestDomainI32(v));
    }

    #[test]
    fn prop_domain_i64(v in any::<i64>()) {
        roundtrip(&TestDomainI64(v));
    }

    #[test]
    fn prop_domain_string(v in ".*") {
        roundtrip(&TestDomainString(v));
    }

    #[test]
    fn prop_domain_bool(v in any::<bool>()) {
        roundtrip(&TestDomainBool(v));
    }

    #[test]
    fn prop_domain_f64(v in any::<f64>()) {
        if !v.is_nan() {
            roundtrip(&TestDomainF64(v));
        }
    }
}

// ---------------------------------------------------------------------------
// Integer-backed PgEnum — property-based roundtrip
// ---------------------------------------------------------------------------

#[derive(Debug, PartialEq, resolute::PgEnum)]
#[repr(i32)]
enum PropStatus {
    A = 0,
    B = 1,
    C = -1,
    D = i32::MAX,
    E = i32::MIN,
}

proptest! {
    #[test]
    fn prop_int_enum_roundtrip(idx in 0..5usize) {
        let variants = [PropStatus::A, PropStatus::B, PropStatus::C, PropStatus::D, PropStatus::E];
        let v = &variants[idx];
        let mut buf = bytes::BytesMut::new();
        v.encode(&mut buf);
        let decoded = PropStatus::decode(&buf).unwrap();
        assert_eq!(&decoded, v);
    }

    #[test]
    fn fuzz_int_enum_decode_arbitrary(buf in proptest::collection::vec(any::<u8>(), 0..16)) {
        // Must not panic. Errors are fine.
        let _ = PropStatus::decode(&buf);
    }

    #[test]
    fn fuzz_int_enum_decode_text_arbitrary(s in ".*") {
        // Must not panic. Errors are fine.
        let _ = PropStatus::decode_text(&s);
    }
}

// ---------------------------------------------------------------------------
// PgDomain ARRAY_OID inheritance — compile-time assertions
// ---------------------------------------------------------------------------

use resolute::PgType;

#[test]
fn test_domain_array_oid_inheritance_prop() {
    // i32 has ARRAY_OID 1007
    assert_eq!(<TestDomainI32 as PgType>::ARRAY_OID, <i32 as PgType>::ARRAY_OID);
    // i64 has ARRAY_OID 1016
    assert_eq!(<TestDomainI64 as PgType>::ARRAY_OID, <i64 as PgType>::ARRAY_OID);
    // String has ARRAY_OID 1009
    assert_eq!(<TestDomainString as PgType>::ARRAY_OID, <String as PgType>::ARRAY_OID);
    // bool has ARRAY_OID 1000
    assert_eq!(<TestDomainBool as PgType>::ARRAY_OID, <bool as PgType>::ARRAY_OID);
    // f64 has ARRAY_OID 1022
    assert_eq!(<TestDomainF64 as PgType>::ARRAY_OID, <f64 as PgType>::ARRAY_OID);
}
