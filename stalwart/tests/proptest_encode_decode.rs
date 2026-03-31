//! Property-based tests: encode → decode roundtrip for all types.
//! Generates random values and verifies that encode(decode(x)) == x.

use stalwart::{Decode, DecodeText, Encode};
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
        let _ = stalwart::named_params::rewrite(&s);
    }
}
