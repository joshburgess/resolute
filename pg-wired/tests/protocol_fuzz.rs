//! Property-based fuzz tests for the PostgreSQL protocol parser.
//!
//! These tests verify that the parser never panics on arbitrary input,
//! always returns Ok or Err, and handles all edge cases gracefully.

use bytes::{BufMut, BytesMut};
use proptest::prelude::*;

/// Build a wire message from a tag byte and body.
fn make_message(tag: u8, body: &[u8]) -> BytesMut {
    let mut buf = BytesMut::new();
    buf.put_u8(tag);
    buf.put_i32((body.len() + 4) as i32);
    buf.extend_from_slice(body);
    buf
}

/// All known PostgreSQL backend message tags.
const KNOWN_TAGS: &[u8] = &[
    b'R', // Auth
    b'S', // ParameterStatus
    b'K', // BackendKeyData
    b'Z', // ReadyForQuery
    b'1', // ParseComplete
    b'2', // BindComplete
    b'3', // CloseComplete
    b'n', // NoData
    b'C', // CommandComplete
    b'D', // DataRow
    b'T', // RowDescription
    b'E', // ErrorResponse
    b'N', // NoticeResponse
    b'I', // EmptyQueryResponse
    b'A', // NotificationResponse
    b't', // ParameterDescription
    b's', // PortalSuspended
    b'G', // CopyInResponse
    b'H', // CopyOutResponse
    b'd', // CopyData
    b'c', // CopyDone
];

proptest! {
    /// Arbitrary bytes should never cause a panic in parse_message.
    #[test]
    fn fuzz_parse_arbitrary_bytes(data in proptest::collection::vec(any::<u8>(), 0..512)) {
        let mut buf = BytesMut::from(&data[..]);
        // May return Ok(None), Ok(Some(_)), or Err — must not panic.
        let _ = pg_wired::protocol::backend::parse_message(&mut buf);
    }

    /// Valid-looking messages with arbitrary bodies should not panic.
    #[test]
    fn fuzz_parse_known_tag_random_body(
        tag_idx in 0..KNOWN_TAGS.len(),
        body in proptest::collection::vec(any::<u8>(), 0..256),
    ) {
        let tag = KNOWN_TAGS[tag_idx];
        let mut buf = make_message(tag, &body);
        let _ = pg_wired::protocol::backend::parse_message(&mut buf);
    }

    /// Unknown tags with arbitrary bodies should not panic.
    #[test]
    fn fuzz_parse_unknown_tag(
        tag in any::<u8>(),
        body in proptest::collection::vec(any::<u8>(), 0..64),
    ) {
        let mut buf = make_message(tag, &body);
        let _ = pg_wired::protocol::backend::parse_message(&mut buf);
    }

    /// Messages with valid headers but truncated bodies (length claims more
    /// data than is present) should return Ok(None), not panic.
    #[test]
    fn fuzz_truncated_messages(
        tag in any::<u8>(),
        claimed_body_len in 1u32..1024,
        actual_body in proptest::collection::vec(any::<u8>(), 0..128),
    ) {
        let mut buf = BytesMut::new();
        buf.put_u8(tag);
        buf.put_i32(claimed_body_len as i32 + 4); // length includes itself
        buf.extend_from_slice(&actual_body);
        let _ = pg_wired::protocol::backend::parse_message(&mut buf);
    }

    /// DataRow messages with random column counts and data.
    #[test]
    fn fuzz_data_row(
        num_cols in 0u16..20,
        col_data in proptest::collection::vec(
            proptest::collection::vec(any::<u8>(), 0..32),
            0..20usize,
        ),
    ) {
        let mut body = Vec::new();
        body.extend_from_slice(&num_cols.to_be_bytes());
        for (i, data) in col_data.iter().enumerate() {
            if i >= num_cols as usize { break; }
            if data.is_empty() {
                body.extend_from_slice(&(-1i32).to_be_bytes()); // NULL
            } else {
                body.extend_from_slice(&(data.len() as i32).to_be_bytes());
                body.extend_from_slice(data);
            }
        }
        let mut buf = make_message(b'D', &body);
        let _ = pg_wired::protocol::backend::parse_message(&mut buf);
    }

    /// ErrorResponse with random field types and values.
    #[test]
    fn fuzz_error_response(
        fields in proptest::collection::vec(
            (any::<u8>(), "[a-zA-Z0-9 _]{0,32}"),
            0..10,
        ),
    ) {
        let mut body = Vec::new();
        for (field_type, value) in &fields {
            if *field_type == 0 { continue; } // 0 is terminator
            body.push(*field_type);
            body.extend_from_slice(value.as_bytes());
            body.push(0); // null terminator
        }
        body.push(0); // final terminator
        let mut buf = make_message(b'E', &body);
        let _ = pg_wired::protocol::backend::parse_message(&mut buf);
    }

    /// Negative and zero message lengths should be rejected, not panic.
    #[test]
    fn fuzz_bad_lengths(
        tag in any::<u8>(),
        length in -100i32..3,
    ) {
        let mut buf = BytesMut::new();
        buf.put_u8(tag);
        buf.put_i32(length);
        // Add some trailing garbage
        buf.extend_from_slice(&[0u8; 16]);
        let result = pg_wired::protocol::backend::parse_message(&mut buf);
        // Negative/too-small lengths should return Err, not panic
        if length < 4 {
            assert!(result.is_err() || result.unwrap().is_none(),
                "length {length} should be rejected");
        }
    }

    /// Multiple messages concatenated should parse correctly.
    #[test]
    fn fuzz_concatenated_messages(
        tags in proptest::collection::vec(any::<u8>(), 1..5),
        bodies in proptest::collection::vec(
            proptest::collection::vec(any::<u8>(), 0..32),
            1..5,
        ),
    ) {
        let mut buf = BytesMut::new();
        let count = tags.len().min(bodies.len());
        for i in 0..count {
            let tag = tags[i];
            let body = &bodies[i];
            buf.put_u8(tag);
            buf.put_i32((body.len() + 4) as i32);
            buf.extend_from_slice(body);
        }
        // Parse all messages — should not panic.
        for _ in 0..count {
            match pg_wired::protocol::backend::parse_message(&mut buf) {
                Ok(Some(_)) | Ok(None) | Err(_) => {}
            }
        }
    }
}

/// Verify that parse_message is idempotent on incomplete data.
#[test]
fn test_incomplete_data_is_stable() {
    // 4 bytes is not enough for a complete message (need 5: tag + 4-byte length)
    let mut buf = BytesMut::from(&[b'Z', 0, 0, 0][..]);
    assert!(pg_wired::protocol::backend::parse_message(&mut buf).unwrap().is_none());
    // Buffer should not be consumed
    assert_eq!(buf.len(), 4);
}

/// Verify that a message claiming zero body length works.
#[test]
fn test_zero_body_message() {
    let mut buf = make_message(b'1', &[]); // ParseComplete has no body
    let msg = pg_wired::protocol::backend::parse_message(&mut buf).unwrap();
    assert!(msg.is_some());
}
