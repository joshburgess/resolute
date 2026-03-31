use bytes::{Buf, BytesMut};

use super::types::*;

/// Try to parse one complete backend message from the buffer.
/// Returns None if the buffer doesn't contain a complete message.
pub fn parse_message(buf: &mut BytesMut) -> Result<Option<BackendMsg>, String> {
    if buf.len() < 5 {
        return Ok(None); // Need at least tag + length
    }

    let tag = buf[0];
    let len = i32::from_be_bytes([buf[1], buf[2], buf[3], buf[4]]) as usize;

    if buf.len() < 1 + len {
        return Ok(None); // Incomplete message
    }

    // Consume the tag byte.
    buf.advance(1);
    // Consume the length (4 bytes), leaving the body.
    buf.advance(4);
    let body_len = len - 4;
    let body = buf.split_to(body_len);

    match tag {
        b'R' => parse_auth(&body),
        b'S' => parse_parameter_status(&body),
        b'K' => parse_backend_key_data(&body),
        b'Z' => Ok(Some(BackendMsg::ReadyForQuery {
            status: body[0],
        })),
        b'1' => Ok(Some(BackendMsg::ParseComplete)),
        b'2' => Ok(Some(BackendMsg::BindComplete)),
        b'3' => Ok(Some(BackendMsg::CloseComplete)),
        b'n' => Ok(Some(BackendMsg::NoData)),
        b'C' => parse_command_complete(&body),
        b'D' => parse_data_row(&body),
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
            tracing::warn!("Unknown backend message tag: {}", other as char);
            Ok(Some(BackendMsg::ReadyForQuery { status: b'I' })) // Skip unknown
        }
    }
}

fn parse_auth(body: &[u8]) -> Result<Option<BackendMsg>, String> {
    let auth_type = i32::from_be_bytes([body[0], body[1], body[2], body[3]]);
    match auth_type {
        0 => Ok(Some(BackendMsg::AuthenticationOk)),
        3 => Ok(Some(BackendMsg::AuthenticationCleartextPassword)),
        5 => {
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
                mechanisms.push(String::from_utf8_lossy(name).into_owned());
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
    Ok(Some(BackendMsg::ParameterStatus {
        name: String::from_utf8_lossy(name).into_owned(),
        value: String::from_utf8_lossy(value).into_owned(),
    }))
}

fn parse_backend_key_data(body: &[u8]) -> Result<Option<BackendMsg>, String> {
    let pid = i32::from_be_bytes([body[0], body[1], body[2], body[3]]);
    let secret = i32::from_be_bytes([body[4], body[5], body[6], body[7]]);
    Ok(Some(BackendMsg::BackendKeyData { pid, secret }))
}

fn parse_command_complete(body: &[u8]) -> Result<Option<BackendMsg>, String> {
    let (tag, _) = split_cstring(body);
    Ok(Some(BackendMsg::CommandComplete {
        tag: String::from_utf8_lossy(tag).into_owned(),
    }))
}

/// Parse DataRow: [int16 num_cols] [int32 len, bytes data]...
/// This is the hot path — keep it fast.
fn parse_data_row(body: &[u8]) -> Result<Option<BackendMsg>, String> {
    let num_cols = i16::from_be_bytes([body[0], body[1]]) as usize;
    let mut columns = Vec::with_capacity(num_cols);
    let mut offset = 2;

    for _ in 0..num_cols {
        let len = i32::from_be_bytes([
            body[offset],
            body[offset + 1],
            body[offset + 2],
            body[offset + 3],
        ]);
        offset += 4;
        if len == -1 {
            columns.push(None); // NULL
        } else {
            let len = len as usize;
            columns.push(Some(body[offset..offset + len].to_vec()));
            offset += len;
        }
    }

    Ok(Some(BackendMsg::DataRow { columns }))
}

fn parse_row_description(body: &[u8]) -> Result<Option<BackendMsg>, String> {
    let num_fields = i16::from_be_bytes([body[0], body[1]]) as usize;
    let mut fields = Vec::with_capacity(num_fields);
    let mut offset = 2;

    for _ in 0..num_fields {
        let (name, _rest) = split_cstring(&body[offset..]);
        offset += name.len() + 1;
        let table_oid = u32::from_be_bytes([body[offset], body[offset+1], body[offset+2], body[offset+3]]);
        offset += 4;
        let column_id = i16::from_be_bytes([body[offset], body[offset+1]]);
        offset += 2;
        let type_oid = u32::from_be_bytes([body[offset], body[offset+1], body[offset+2], body[offset+3]]);
        offset += 4;
        let type_size = i16::from_be_bytes([body[offset], body[offset+1]]);
        offset += 2;
        let type_modifier = i32::from_be_bytes([body[offset], body[offset+1], body[offset+2], body[offset+3]]);
        offset += 4;
        let format = i16::from_be_bytes([body[offset], body[offset+1]]);
        offset += 2;

        fields.push(FieldDescription {
            name: String::from_utf8_lossy(name).into_owned(),
            table_oid,
            column_id,
            type_oid,
            type_size,
            type_modifier,
            format: if format == 1 { FormatCode::Binary } else { FormatCode::Text },
        });
    }

    Ok(Some(BackendMsg::RowDescription { fields }))
}

/// Parse ParameterDescription: [int16 num_params] [int32 oid]...
fn parse_parameter_description(body: &[u8]) -> Result<Option<BackendMsg>, String> {
    let num_params = i16::from_be_bytes([body[0], body[1]]) as usize;
    let mut type_oids = Vec::with_capacity(num_params);
    let mut offset = 2;
    for _ in 0..num_params {
        let oid = u32::from_be_bytes([
            body[offset], body[offset + 1], body[offset + 2], body[offset + 3],
        ]);
        type_oids.push(oid);
        offset += 4;
    }
    Ok(Some(BackendMsg::ParameterDescription { type_oids }))
}

/// Parse NotificationResponse: pid(i32) + channel(cstring) + payload(cstring)
fn parse_notification(body: &[u8]) -> Result<Option<BackendMsg>, String> {
    let pid = i32::from_be_bytes([body[0], body[1], body[2], body[3]]);
    let (channel, rest) = split_cstring(&body[4..]);
    let (payload, _) = split_cstring(rest);
    Ok(Some(BackendMsg::NotificationResponse {
        pid,
        channel: String::from_utf8_lossy(channel).into_owned(),
        payload: String::from_utf8_lossy(payload).into_owned(),
    }))
}

fn parse_copy_response(body: &[u8], is_in: bool) -> Result<Option<BackendMsg>, String> {
    let format = body[0];
    let num_cols = i16::from_be_bytes([body[1], body[2]]) as usize;
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
        let (value, _rest) = split_cstring(&body[offset..]);
        offset += value.len() + 1;
        let value_str = String::from_utf8_lossy(value).into_owned();

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

/// Split a null-terminated string from a byte slice.
fn split_cstring(data: &[u8]) -> (&[u8], &[u8]) {
    match data.iter().position(|&b| b == 0) {
        Some(pos) => (&data[..pos], &data[pos + 1..]),
        None => (data, &[]),
    }
}
