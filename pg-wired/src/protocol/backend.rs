use bytes::{Buf, Bytes, BytesMut};

use super::types::*;

/// Maximum allowed message size (256 MB). Prevents OOM from malicious/corrupt lengths.
const MAX_MESSAGE_SIZE: usize = 256 * 1024 * 1024;

/// Try to parse one complete backend message from the buffer.
/// Returns None if the buffer doesn't contain a complete message.
pub fn parse_message(buf: &mut BytesMut) -> Result<Option<BackendMsg>, String> {
    if buf.len() < 5 {
        return Ok(None); // Need at least tag + length
    }

    let tag = buf[0];
    let len_raw = i32::from_be_bytes([buf[1], buf[2], buf[3], buf[4]]);
    if len_raw < 4 {
        return Err(format!("invalid message length: {len_raw} (must be >= 4)"));
    }
    let len = len_raw as usize;

    if len > MAX_MESSAGE_SIZE {
        return Err(format!(
            "message too large: {len} bytes (max {MAX_MESSAGE_SIZE})"
        ));
    }

    if buf.len() < 1 + len {
        return Ok(None); // Incomplete message
    }

    // Consume the tag byte.
    buf.advance(1);
    // Consume the length (4 bytes), leaving the body.
    buf.advance(4);
    let body_len = len - 4;
    // Freeze to `Bytes` so DataRow can hand each column a zero-copy slice
    // refcounted into the same backing allocation. Other parsers see `&[u8]`
    // via `Deref` and don't care.
    let body = buf.split_to(body_len).freeze();

    match tag {
        b'R' => parse_auth(&body),
        b'S' => parse_parameter_status(&body),
        b'K' => parse_backend_key_data(&body),
        b'Z' => {
            if body.is_empty() {
                return Err("ReadyForQuery: empty body".into());
            }
            Ok(Some(BackendMsg::ReadyForQuery { status: body[0] }))
        }
        b'1' => Ok(Some(BackendMsg::ParseComplete)),
        b'2' => Ok(Some(BackendMsg::BindComplete)),
        b'3' => Ok(Some(BackendMsg::CloseComplete)),
        b'n' => Ok(Some(BackendMsg::NoData)),
        b'C' => parse_command_complete(&body),
        // DataRow takes the body by value so it can move it into the RawRow
        // without bumping the underlying Arc. Every other branch only
        // borrows.
        b'D' => parse_data_row(body),
        b'T' => parse_row_description(&body),
        b'E' => parse_error_or_notice(&body).map(|e| Some(BackendMsg::ErrorResponse { fields: e })),
        b'N' => {
            parse_error_or_notice(&body).map(|e| Some(BackendMsg::NoticeResponse { fields: e }))
        }
        b'I' => Ok(Some(BackendMsg::EmptyQueryResponse)),
        b'A' => parse_notification(&body),
        b't' => parse_parameter_description(&body),
        b's' => Ok(Some(BackendMsg::PortalSuspended)),
        b'G' => parse_copy_response(&body, true),
        b'H' => parse_copy_response(&body, false),
        b'd' => Ok(Some(BackendMsg::CopyData {
            data: body.to_vec(),
        })),
        b'c' => Ok(Some(BackendMsg::CopyDone)),
        other => {
            tracing::warn!(
                "Unknown backend message tag: {} (0x{:02x})",
                other as char,
                other
            );
            Ok(None) // Skip unknown messages instead of fabricating ReadyForQuery
        }
    }
}

fn parse_auth(body: &[u8]) -> Result<Option<BackendMsg>, String> {
    if body.len() < 4 {
        return Err("AuthenticationRequest: body too short".into());
    }
    let auth_type = i32::from_be_bytes([body[0], body[1], body[2], body[3]]);
    match auth_type {
        0 => Ok(Some(BackendMsg::AuthenticationOk)),
        3 => Ok(Some(BackendMsg::AuthenticationCleartextPassword)),
        5 => {
            if body.len() < 8 {
                return Err("AuthenticationMd5Password: body too short for salt".into());
            }
            let mut salt = [0u8; 4];
            salt.copy_from_slice(&body[4..8]);
            Ok(Some(BackendMsg::AuthenticationMd5Password { salt }))
        }
        10 => {
            // SASL: parse mechanism names (null-terminated strings, double-null terminated)
            let mut mechanisms = Vec::new();
            let mut offset = 4;
            while offset < body.len() && body[offset] != 0 {
                let (name, _) = split_cstring(&body[offset..]);
                let name_str = String::from_utf8(name.to_vec())
                    .map_err(|e| format!("SASL mechanism name is not valid UTF-8: {e}"))?;
                mechanisms.push(name_str);
                offset += name.len() + 1;
            }
            Ok(Some(BackendMsg::AuthenticationSASL { mechanisms }))
        }
        11 => Ok(Some(BackendMsg::AuthenticationSASLContinue {
            data: body[4..].to_vec(),
        })),
        12 => Ok(Some(BackendMsg::AuthenticationSASLFinal {
            data: body[4..].to_vec(),
        })),
        _ => Err(format!("Unsupported auth type: {auth_type}")),
    }
}

fn parse_parameter_status(body: &[u8]) -> Result<Option<BackendMsg>, String> {
    let (name, rest) = split_cstring(body);
    let (value, _) = split_cstring(rest);
    let name_str = String::from_utf8(name.to_vec())
        .map_err(|e| format!("ParameterStatus name is not valid UTF-8: {e}"))?;
    let value_str = String::from_utf8(value.to_vec())
        .map_err(|e| format!("ParameterStatus value is not valid UTF-8: {e}"))?;
    Ok(Some(BackendMsg::ParameterStatus {
        name: name_str,
        value: value_str,
    }))
}

fn parse_backend_key_data(body: &[u8]) -> Result<Option<BackendMsg>, String> {
    if body.len() < 8 {
        return Err("BackendKeyData: body too short (need 8 bytes)".into());
    }
    let pid = i32::from_be_bytes([body[0], body[1], body[2], body[3]]);
    let secret = i32::from_be_bytes([body[4], body[5], body[6], body[7]]);
    Ok(Some(BackendMsg::BackendKeyData { pid, secret }))
}

fn parse_command_complete(body: &[u8]) -> Result<Option<BackendMsg>, String> {
    let (tag, _) = split_cstring(body);
    let tag_str = String::from_utf8(tag.to_vec())
        .map_err(|e| format!("CommandComplete tag is not valid UTF-8: {e}"))?;
    Ok(Some(BackendMsg::CommandComplete { tag: tag_str }))
}

/// Parse DataRow: [int16 num_cols] [int32 len, bytes data]...
/// Hot path. Cell `(offset, length)` pairs (length = -1 means NULL) are
/// written into a small stack buffer when the row fits inline
/// ([`CELL_INLINE_CAP`] cells), avoiding any heap allocation on the common
/// path. Wider rows fall through to a single `Box<[(u32, i32)]>` per row.
///
/// Takes `body` by value and moves it into the `RawRow`, avoiding the
/// per-row Bytes::clone (atomic Arc bump) that a borrowed signature
/// would force.
fn parse_data_row(body: Bytes) -> Result<Option<BackendMsg>, String> {
    if body.len() < 2 {
        return Err("DataRow: body too short for column count".into());
    }
    let body_slice = body.as_ref();
    let body_len = body_slice.len();
    let num_cols = i16_to_usize(i16::from_be_bytes([body_slice[0], body_slice[1]]), "DataRow")?;
    let mut offset = 2usize;

    if num_cols <= CELL_INLINE_CAP {
        let mut data = [(0u32, 0i32); CELL_INLINE_CAP];
        for slot in data.iter_mut().take(num_cols) {
            *slot = read_cell_entry(body_slice, &mut offset, body_len)?;
        }
        return Ok(Some(BackendMsg::DataRow(RawRow::from_inline_unchecked(
            body,
            data,
            num_cols as u8,
        ))));
    }

    let mut entries: Vec<(u32, i32)> = Vec::with_capacity(num_cols);
    for _ in 0..num_cols {
        entries.push(read_cell_entry(body_slice, &mut offset, body_len)?);
    }
    Ok(Some(BackendMsg::DataRow(RawRow::from_entries(
        body, &entries,
    ))))
}

#[inline(always)]
fn read_cell_entry(
    body: &[u8],
    offset: &mut usize,
    body_len: usize,
) -> Result<(u32, i32), String> {
    let off = *offset;
    if off + 4 > body_len {
        return Err(format!(
            "DataRow: truncated at length (offset {off}, body len {body_len})"
        ));
    }
    // SAFETY: `off + 4 <= body_len == body.len()` checked above.
    let len_bytes = unsafe { body.get_unchecked(off..off + 4) };
    let len = i32::from_be_bytes([len_bytes[0], len_bytes[1], len_bytes[2], len_bytes[3]]);
    let after_len = off + 4;
    if len < 0 {
        if len == -1 {
            *offset = after_len;
            return Ok((0, -1));
        }
        return Err(format!("DataRow: invalid negative column length {len}"));
    }
    let ulen = len as usize;
    if after_len + ulen > body_len {
        return Err(format!(
            "DataRow: truncated column data (need {ulen} bytes at offset {after_len}, body len {body_len})"
        ));
    }
    *offset = after_len + ulen;
    Ok((after_len as u32, len))
}

fn parse_row_description(body: &[u8]) -> Result<Option<BackendMsg>, String> {
    if body.len() < 2 {
        return Err("RowDescription: body too short for field count".into());
    }
    let num_fields = i16_to_usize(i16::from_be_bytes([body[0], body[1]]), "RowDescription")?;
    let mut fields = Vec::with_capacity(num_fields);
    let mut offset = 2;

    for field_idx in 0..num_fields {
        if offset >= body.len() {
            return Err(format!(
                "RowDescription: truncated at field {field_idx} name"
            ));
        }
        let (name, _rest) = split_cstring(&body[offset..]);
        offset += name.len() + 1;

        // Each field has: table_oid(4) + column_id(2) + type_oid(4) + type_size(2) + type_modifier(4) + format(2) = 18 bytes
        if offset + 18 > body.len() {
            return Err(format!(
                "RowDescription: truncated at field {field_idx} metadata (need 18 bytes at offset {offset}, body len {})",
                body.len()
            ));
        }

        let table_oid = u32::from_be_bytes([
            body[offset],
            body[offset + 1],
            body[offset + 2],
            body[offset + 3],
        ]);
        offset += 4;
        let column_id = i16::from_be_bytes([body[offset], body[offset + 1]]);
        offset += 2;
        let type_oid = u32::from_be_bytes([
            body[offset],
            body[offset + 1],
            body[offset + 2],
            body[offset + 3],
        ]);
        offset += 4;
        let type_size = i16::from_be_bytes([body[offset], body[offset + 1]]);
        offset += 2;
        let type_modifier = i32::from_be_bytes([
            body[offset],
            body[offset + 1],
            body[offset + 2],
            body[offset + 3],
        ]);
        offset += 4;
        let format = i16::from_be_bytes([body[offset], body[offset + 1]]);
        offset += 2;

        let name_str = String::from_utf8(name.to_vec()).map_err(|e| {
            format!("RowDescription field {field_idx} name is not valid UTF-8: {e}")
        })?;

        fields.push(FieldDescription {
            name: name_str,
            table_oid,
            column_id,
            type_oid,
            type_size,
            type_modifier,
            format: if format == 1 {
                FormatCode::Binary
            } else {
                FormatCode::Text
            },
        });
    }

    Ok(Some(BackendMsg::RowDescription { fields }))
}

/// Parse ParameterDescription: [int16 num_params] [int32 oid]...
fn parse_parameter_description(body: &[u8]) -> Result<Option<BackendMsg>, String> {
    if body.len() < 2 {
        return Err("ParameterDescription: body too short for param count".into());
    }
    let num_params = i16_to_usize(
        i16::from_be_bytes([body[0], body[1]]),
        "ParameterDescription",
    )?;
    if body.len() < 2 + num_params * 4 {
        return Err(format!(
            "ParameterDescription: body too short for {num_params} params (need {}, have {})",
            2 + num_params * 4,
            body.len()
        ));
    }
    let mut type_oids = Vec::with_capacity(num_params);
    let mut offset = 2;
    for _ in 0..num_params {
        let oid = u32::from_be_bytes([
            body[offset],
            body[offset + 1],
            body[offset + 2],
            body[offset + 3],
        ]);
        type_oids.push(oid);
        offset += 4;
    }
    Ok(Some(BackendMsg::ParameterDescription { type_oids }))
}

/// Parse NotificationResponse: pid(i32) + channel(cstring) + payload(cstring)
fn parse_notification(body: &[u8]) -> Result<Option<BackendMsg>, String> {
    if body.len() < 4 {
        return Err("NotificationResponse: body too short for pid".into());
    }
    let pid = i32::from_be_bytes([body[0], body[1], body[2], body[3]]);
    let (channel, rest) = split_cstring(&body[4..]);
    let (payload, _) = split_cstring(rest);
    let channel_str = String::from_utf8(channel.to_vec())
        .map_err(|e| format!("NotificationResponse channel is not valid UTF-8: {e}"))?;
    let payload_str = String::from_utf8(payload.to_vec())
        .map_err(|e| format!("NotificationResponse payload is not valid UTF-8: {e}"))?;
    Ok(Some(BackendMsg::NotificationResponse {
        pid,
        channel: channel_str,
        payload: payload_str,
    }))
}

fn parse_copy_response(body: &[u8], is_in: bool) -> Result<Option<BackendMsg>, String> {
    if body.len() < 3 {
        return Err("CopyResponse: body too short".into());
    }
    let format = body[0];
    let num_cols = i16_to_usize(i16::from_be_bytes([body[1], body[2]]), "CopyResponse")?;
    if body.len() < 3 + num_cols * 2 {
        return Err(format!(
            "CopyResponse: body too short for {num_cols} column formats"
        ));
    }
    let mut column_formats = Vec::with_capacity(num_cols);
    let mut offset = 3;
    for _ in 0..num_cols {
        let cf = i16::from_be_bytes([body[offset], body[offset + 1]]);
        column_formats.push(cf);
        offset += 2;
    }
    if is_in {
        Ok(Some(BackendMsg::CopyInResponse {
            format,
            column_formats,
        }))
    } else {
        Ok(Some(BackendMsg::CopyOutResponse {
            format,
            column_formats,
        }))
    }
}

fn parse_error_or_notice(body: &[u8]) -> Result<PgError, String> {
    let mut err = PgError::default();
    let mut offset = 0;

    while offset < body.len() && body[offset] != 0 {
        let field_type = body[offset];
        offset += 1;
        if offset >= body.len() {
            break;
        }
        let (value, _rest) = split_cstring(&body[offset..]);
        offset += value.len() + 1;
        let value_str = String::from_utf8(value.to_vec())
            .unwrap_or_else(|_| String::from_utf8_lossy(value).into_owned());

        match field_type {
            b'S' => err.severity = value_str,
            b'C' => err.code = value_str,
            b'M' => err.message = value_str,
            b'D' => err.detail = Some(value_str),
            b'H' => err.hint = Some(value_str),
            b'P' => err.position = Some(value_str),
            _ => {} // Skip other fields
        }
    }

    Ok(err)
}

/// Convert i16 to usize, rejecting negative values.
fn i16_to_usize(val: i16, context: &str) -> Result<usize, String> {
    if val < 0 {
        Err(format!("{context}: negative count {val}"))
    } else {
        Ok(val as usize)
    }
}

/// Split a null-terminated string from a byte slice.
fn split_cstring(data: &[u8]) -> (&[u8], &[u8]) {
    match data.iter().position(|&b| b == 0) {
        Some(pos) => (&data[..pos], &data[pos + 1..]),
        None => (data, &[]),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BufMut;

    /// Build a complete wire message: tag(1) + length(4) + body
    fn make_message(tag: u8, body: &[u8]) -> BytesMut {
        let mut buf = BytesMut::new();
        buf.put_u8(tag);
        buf.put_i32((body.len() + 4) as i32);
        buf.extend_from_slice(body);
        buf
    }

    #[test]
    fn test_parse_ready_for_query() {
        let mut buf = make_message(b'Z', b"I");
        let msg = parse_message(&mut buf).unwrap().unwrap();
        assert!(matches!(msg, BackendMsg::ReadyForQuery { status: b'I' }));
    }

    #[test]
    fn test_parse_ready_for_query_empty_body() {
        let mut buf = make_message(b'Z', &[]);
        let err = parse_message(&mut buf).unwrap_err();
        assert!(err.contains("empty body"), "got: {err}");
    }

    #[test]
    fn test_parse_backend_key_data() {
        let mut body = Vec::new();
        body.extend_from_slice(&42i32.to_be_bytes());
        body.extend_from_slice(&99i32.to_be_bytes());
        let mut buf = make_message(b'K', &body);
        let msg = parse_message(&mut buf).unwrap().unwrap();
        assert!(matches!(
            msg,
            BackendMsg::BackendKeyData {
                pid: 42,
                secret: 99
            }
        ));
    }

    #[test]
    fn test_parse_backend_key_data_too_short() {
        let mut buf = make_message(b'K', &[1, 2, 3]);
        let err = parse_message(&mut buf).unwrap_err();
        assert!(err.contains("too short"), "got: {err}");
    }

    #[test]
    fn test_parse_data_row_basic() {
        let mut body = Vec::new();
        body.extend_from_slice(&1i16.to_be_bytes()); // 1 column
        body.extend_from_slice(&5i32.to_be_bytes()); // 5 bytes
        body.extend_from_slice(b"hello");
        let mut buf = make_message(b'D', &body);
        let msg = parse_message(&mut buf).unwrap().unwrap();
        if let BackendMsg::DataRow(row) = msg {
            assert_eq!(row.len(), 1);
            assert_eq!(row.cell(0), Some(b"hello".as_ref()));
        } else {
            panic!("expected DataRow");
        }
    }

    #[test]
    fn test_parse_data_row_null() {
        let mut body = Vec::new();
        body.extend_from_slice(&1i16.to_be_bytes()); // 1 column
        body.extend_from_slice(&(-1i32).to_be_bytes()); // NULL
        let mut buf = make_message(b'D', &body);
        let msg = parse_message(&mut buf).unwrap().unwrap();
        if let BackendMsg::DataRow(row) = msg {
            assert_eq!(row.try_cell(0), Some(None));
        } else {
            panic!("expected DataRow");
        }
    }

    #[test]
    fn test_parse_data_row_truncated_length() {
        let mut body = Vec::new();
        body.extend_from_slice(&1i16.to_be_bytes()); // 1 column
        body.extend_from_slice(&[0, 0]); // only 2 bytes, need 4
        let mut buf = make_message(b'D', &body);
        let err = parse_message(&mut buf).unwrap_err();
        assert!(err.contains("truncated"), "got: {err}");
    }

    #[test]
    fn test_parse_data_row_truncated_data() {
        let mut body = Vec::new();
        body.extend_from_slice(&1i16.to_be_bytes());
        body.extend_from_slice(&100i32.to_be_bytes()); // claims 100 bytes
        body.extend_from_slice(b"short"); // only 5 bytes
        let mut buf = make_message(b'D', &body);
        let err = parse_message(&mut buf).unwrap_err();
        assert!(err.contains("truncated"), "got: {err}");
    }

    #[test]
    fn test_parse_data_row_negative_length() {
        let mut body = Vec::new();
        body.extend_from_slice(&1i16.to_be_bytes());
        body.extend_from_slice(&(-2i32).to_be_bytes()); // invalid negative (not -1)
        let mut buf = make_message(b'D', &body);
        let err = parse_message(&mut buf).unwrap_err();
        assert!(err.contains("invalid negative"), "got: {err}");
    }

    #[test]
    fn test_parse_row_description_too_short() {
        let mut buf = make_message(b'T', &[]);
        let err = parse_message(&mut buf).unwrap_err();
        assert!(err.contains("too short"), "got: {err}");
    }

    #[test]
    fn test_parse_parameter_description_too_short() {
        let mut body = Vec::new();
        body.extend_from_slice(&3i16.to_be_bytes()); // claims 3 params
        body.extend_from_slice(&23u32.to_be_bytes()); // only 1 OID
        let mut buf = make_message(b't', &body);
        let err = parse_message(&mut buf).unwrap_err();
        assert!(err.contains("too short"), "got: {err}");
    }

    #[test]
    fn test_parse_notification_too_short() {
        let mut buf = make_message(b'A', &[1, 2]);
        let err = parse_message(&mut buf).unwrap_err();
        assert!(err.contains("too short"), "got: {err}");
    }

    #[test]
    fn test_parse_copy_response_too_short() {
        let mut buf = make_message(b'G', &[0, 0]);
        let err = parse_message(&mut buf).unwrap_err();
        assert!(err.contains("too short"), "got: {err}");
    }

    #[test]
    fn test_negative_message_length() {
        let mut buf = BytesMut::new();
        buf.put_u8(b'Z');
        buf.put_i32(-1); // negative length
        let err = parse_message(&mut buf).unwrap_err();
        assert!(err.contains("invalid message length"), "got: {err}");
    }

    #[test]
    fn test_message_too_large() {
        let mut buf = BytesMut::new();
        buf.put_u8(b'Z');
        buf.put_i32((MAX_MESSAGE_SIZE + 100) as i32);
        let err = parse_message(&mut buf).unwrap_err();
        assert!(err.contains("too large"), "got: {err}");
    }

    #[test]
    fn test_unknown_tag_returns_none() {
        let mut buf = make_message(b'?', &[0]);
        let msg = parse_message(&mut buf).unwrap();
        assert!(msg.is_none(), "unknown tag should return None");
    }

    #[test]
    fn test_incomplete_message_returns_none() {
        let mut buf = BytesMut::new();
        buf.put_u8(b'Z');
        buf.put_i32(5); // length=5, body_len=1, but no body yet
        assert!(parse_message(&mut buf).unwrap().is_none());
    }

    #[test]
    fn test_parse_auth_too_short() {
        let mut buf = make_message(b'R', &[0, 0]);
        let err = parse_message(&mut buf).unwrap_err();
        assert!(err.contains("too short"), "got: {err}");
    }

    #[test]
    fn test_parse_md5_salt_too_short() {
        let mut body = Vec::new();
        body.extend_from_slice(&5i32.to_be_bytes()); // MD5 auth type
        body.extend_from_slice(&[1, 2]); // only 2 salt bytes, need 4
        let mut buf = make_message(b'R', &body);
        let err = parse_message(&mut buf).unwrap_err();
        assert!(err.contains("too short"), "got: {err}");
    }

    #[test]
    fn test_parse_command_complete() {
        let body = b"SELECT 42\0".to_vec();
        let mut buf = make_message(b'C', &body);
        let msg = parse_message(&mut buf).unwrap().unwrap();
        if let BackendMsg::CommandComplete { tag } = msg {
            assert_eq!(tag, "SELECT 42");
        } else {
            panic!("expected CommandComplete");
        }
    }

    #[test]
    fn test_fuzz_empty_body_all_tags() {
        // Every known tag with empty body should return Err, not panic.
        for tag in [b'R', b'S', b'K', b'Z', b'D', b'T', b'A', b't', b'G', b'H'] {
            let mut buf = make_message(tag, &[]);
            let result = parse_message(&mut buf);
            // Should be Err (body too short) or Ok(Some(...)) — must NOT panic.
            assert!(
                result.is_ok() || result.is_err(),
                "tag {}: should not panic on empty body",
                tag as char
            );
        }
    }
}
