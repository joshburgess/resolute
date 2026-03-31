use bytes::{BufMut, BytesMut};

use super::types::FrontendMsg;

/// Encode multiple frontend messages into a single buffer for one write() syscall.
/// This is the core of message coalescing — Parse+Bind+Execute+Sync all go in one buffer.
pub fn encode_messages(msgs: &[FrontendMsg<'_>], buf: &mut BytesMut) {
    for msg in msgs {
        encode_message(msg, buf);
    }
}

/// Encode a single frontend message into the buffer.
pub fn encode_message(msg: &FrontendMsg<'_>, buf: &mut BytesMut) {
    match msg {
        FrontendMsg::Parse {
            name,
            sql,
            param_oids,
        } => {
            let len = 4 + name.len() + 1 + sql.len() + 1 + 2 + param_oids.len() * 4;
            buf.put_u8(b'P');
            buf.put_i32(len as i32);
            buf.put_slice(name);
            buf.put_u8(0); // null terminator
            buf.put_slice(sql);
            buf.put_u8(0);
            buf.put_i16(param_oids.len() as i16);
            for &oid in *param_oids {
                buf.put_u32(oid);
            }
        }
        FrontendMsg::Bind {
            portal,
            statement,
            param_formats,
            params,
            result_formats,
        } => {
            // Pre-compute size.
            let params_size: usize = params
                .iter()
                .map(|p| match p {
                    None => 4,                    // -1 (null)
                    Some(data) => 4 + data.len(), // len + data
                })
                .sum();
            let len = 4
                + portal.len() + 1
                + statement.len() + 1
                + 2 + param_formats.len() * 2
                + 2 + params_size
                + 2 + result_formats.len() * 2;
            buf.put_u8(b'B');
            buf.put_i32(len as i32);
            buf.put_slice(portal);
            buf.put_u8(0);
            buf.put_slice(statement);
            buf.put_u8(0);
            // Parameter format codes
            buf.put_i16(param_formats.len() as i16);
            for &fmt in *param_formats {
                buf.put_i16(fmt as i16);
            }
            // Parameter values
            buf.put_i16(params.len() as i16);
            for param in *params {
                match param {
                    None => buf.put_i32(-1), // NULL
                    Some(data) => {
                        buf.put_i32(data.len() as i32);
                        buf.put_slice(data);
                    }
                }
            }
            // Result format codes
            buf.put_i16(result_formats.len() as i16);
            for &fmt in *result_formats {
                buf.put_i16(fmt as i16);
            }
        }
        FrontendMsg::Execute { portal, max_rows } => {
            let len = 4 + portal.len() + 1 + 4;
            buf.put_u8(b'E');
            buf.put_i32(len as i32);
            buf.put_slice(portal);
            buf.put_u8(0);
            buf.put_i32(*max_rows);
        }
        FrontendMsg::Sync => {
            buf.put_u8(b'S');
            buf.put_i32(4);
        }
        FrontendMsg::Query(sql) => {
            let len = 4 + sql.len() + 1;
            buf.put_u8(b'Q');
            buf.put_i32(len as i32);
            buf.put_slice(sql);
            buf.put_u8(0);
        }
        FrontendMsg::Describe { kind, name } => {
            let len = 4 + 1 + name.len() + 1;
            buf.put_u8(b'D');
            buf.put_i32(len as i32);
            buf.put_u8(*kind);
            buf.put_slice(name);
            buf.put_u8(0);
        }
        FrontendMsg::Close { kind, name } => {
            let len = 4 + 1 + name.len() + 1;
            buf.put_u8(b'C');
            buf.put_i32(len as i32);
            buf.put_u8(*kind);
            buf.put_slice(name);
            buf.put_u8(0);
        }
        FrontendMsg::Flush => {
            buf.put_u8(b'H');
            buf.put_i32(4);
        }
        FrontendMsg::SASLInitialResponse { mechanism, data } => {
            let len = 4 + mechanism.len() + 1 + 4 + data.len();
            buf.put_u8(b'p');
            buf.put_i32(len as i32);
            buf.put_slice(mechanism);
            buf.put_u8(0);
            buf.put_i32(data.len() as i32);
            buf.put_slice(data);
        }
        FrontendMsg::SASLResponse(data) => {
            let len = 4 + data.len();
            buf.put_u8(b'p');
            buf.put_i32(len as i32);
            buf.put_slice(data);
        }
        FrontendMsg::CopyData(data) => {
            let len = 4 + data.len();
            buf.put_u8(b'd');
            buf.put_i32(len as i32);
            buf.put_slice(data);
        }
        FrontendMsg::CopyDone => {
            buf.put_u8(b'c');
            buf.put_i32(4);
        }
        FrontendMsg::CopyFail(msg) => {
            let len = 4 + msg.len() + 1;
            buf.put_u8(b'f');
            buf.put_i32(len as i32);
            buf.put_slice(msg);
            buf.put_u8(0);
        }
        FrontendMsg::Terminate => {
            buf.put_u8(b'X');
            buf.put_i32(4);
        }
    }
}

/// Encode the startup message (no message type tag, special format).
pub fn encode_startup(user: &str, database: &str, buf: &mut BytesMut) {
    let mut body = BytesMut::new();
    body.put_i32(196608); // Protocol version 3.0
    body.put_slice(b"user\0");
    body.put_slice(user.as_bytes());
    body.put_u8(0);
    body.put_slice(b"database\0");
    body.put_slice(database.as_bytes());
    body.put_u8(0);
    body.put_u8(0); // End of parameters

    buf.put_i32((4 + body.len()) as i32);
    buf.put_slice(&body);
}

/// Encode a password message.
pub fn encode_password(password: &[u8], buf: &mut BytesMut) {
    let len = 4 + password.len() + 1;
    buf.put_u8(b'p');
    buf.put_i32(len as i32);
    buf.put_slice(password);
    buf.put_u8(0);
}

/// Compute MD5 password hash: md5(md5(password + user) + salt).
pub fn md5_password(user: &str, password: &str, salt: &[u8; 4]) -> Vec<u8> {
    let inner = format!(
        "{:x}",
        md5::compute(format!("{}{}", password, user).as_bytes())
    );
    let outer = format!("md5{:x}", md5::compute([inner.as_bytes(), salt].concat()));
    outer.into_bytes()
}
