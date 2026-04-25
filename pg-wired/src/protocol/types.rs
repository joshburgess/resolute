/// PostgreSQL OID type.
pub type Oid = u32;

/// Wire format codes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i16)]
#[non_exhaustive]
pub enum FormatCode {
    Text = 0,
    Binary = 1,
}

/// Frontend (client → server) messages.
#[derive(Debug)]
#[non_exhaustive]
pub enum FrontendMsg<'a> {
    /// Parse: prepare a statement.
    /// name (empty = unnamed), sql, param OIDs
    Parse {
        name: &'a [u8],
        sql: &'a [u8],
        param_oids: &'a [Oid],
    },
    /// Bind: bind parameters to a prepared statement.
    /// portal (empty = unnamed), statement name, param formats, param values, result formats
    Bind {
        portal: &'a [u8],
        statement: &'a [u8],
        param_formats: &'a [FormatCode],
        params: &'a [Option<&'a [u8]>],
        result_formats: &'a [FormatCode],
    },
    /// Execute: execute a bound portal.
    Execute { portal: &'a [u8], max_rows: i32 },
    /// Sync: end of pipeline, triggers response flush.
    Sync,
    /// Query: simple query protocol (text only).
    Query(&'a [u8]),
    /// Describe: request description of a statement or portal.
    Describe {
        kind: u8, // b'S' = statement, b'P' = portal
        name: &'a [u8],
    },
    /// Close: close a prepared statement or portal.
    Close {
        kind: u8, // b'S' = statement, b'P' = portal
        name: &'a [u8],
    },
    /// Flush: request server to flush output.
    Flush,
    /// SASL initial response.
    SASLInitialResponse { mechanism: &'a [u8], data: &'a [u8] },
    /// SASL response (continuation).
    SASLResponse(&'a [u8]),
    /// CopyData: a chunk of COPY data sent to server.
    CopyData(&'a [u8]),
    /// CopyDone: signal that COPY data is complete.
    CopyDone,
    /// CopyFail: abort COPY with an error message.
    CopyFail(&'a [u8]),
    /// Terminate: close connection.
    Terminate,
}

/// Backend (server → client) messages.
#[derive(Debug)]
#[non_exhaustive]
pub enum BackendMsg {
    AuthenticationOk,
    AuthenticationCleartextPassword,
    AuthenticationMd5Password {
        salt: [u8; 4],
    },
    AuthenticationSASL {
        mechanisms: Vec<String>,
    },
    AuthenticationSASLContinue {
        data: Vec<u8>,
    },
    AuthenticationSASLFinal {
        data: Vec<u8>,
    },
    ParameterStatus {
        name: String,
        value: String,
    },
    BackendKeyData {
        pid: i32,
        secret: i32,
    },
    ReadyForQuery {
        status: u8,
    },
    ParseComplete,
    BindComplete,
    CloseComplete,
    NoData,
    CommandComplete {
        tag: String,
    },
    DataRow(RawRow),
    RowDescription {
        fields: Vec<FieldDescription>,
    },
    ErrorResponse {
        fields: PgError,
    },
    NoticeResponse {
        fields: PgError,
    },
    EmptyQueryResponse,
    /// ParameterDescription: param type OIDs from a Describe Statement.
    ParameterDescription {
        type_oids: Vec<Oid>,
    },
    /// NotificationResponse: async notification from LISTEN/NOTIFY.
    NotificationResponse {
        pid: i32,
        channel: String,
        payload: String,
    },
    /// PortalSuspended: Execute completed with row limit, portal still open.
    PortalSuspended,
    /// CopyInResponse: server is ready to receive COPY data.
    CopyInResponse {
        format: u8, // 0=text, 1=binary
        column_formats: Vec<i16>,
    },
    /// CopyOutResponse: server is about to send COPY data.
    CopyOutResponse {
        format: u8,
        column_formats: Vec<i16>,
    },
    /// CopyData: a chunk of COPY data (in either direction).
    CopyData {
        data: Vec<u8>,
    },
    /// CopyDone: COPY stream completed.
    CopyDone,
}

/// A parsed wire-protocol DataRow. Cell `(offset, length)` pairs (length
/// `-1` = NULL) are stored inline in the row up to [`CELL_INLINE_CAP`]
/// columns, falling back to a `Box<[(u32, i32)]>` for wider rows. Inline
/// storage avoids the per-row Arc bump/decrement that a shared `Bytes`
/// would impose. Cell values point into `body`.
pub const CELL_INLINE_CAP: usize = 12;

#[derive(Debug, Clone)]
pub struct RawRow {
    pub(crate) body: bytes::Bytes,
    cells: Cells,
}

impl RawRow {
    /// Raw wire-protocol body the cell offsets index into. Exposed for
    /// downstream typed-row decoders (e.g., `resolute::Row`); offset/length
    /// semantics are an internal contract, treat as opaque.
    #[doc(hidden)]
    pub fn body(&self) -> &bytes::Bytes {
        &self.body
    }
}

#[derive(Debug, Clone)]
enum Cells {
    Inline {
        data: [(u32, i32); CELL_INLINE_CAP],
        len: u8,
    },
    Heap(Box<[(u32, i32)]>),
}

impl RawRow {
    /// Empty row (zero columns).
    pub fn empty() -> Self {
        Self {
            body: bytes::Bytes::new(),
            cells: Cells::Inline {
                data: [(0, 0); CELL_INLINE_CAP],
                len: 0,
            },
        }
    }

    /// Construct directly from an inline cell buffer. Caller must ensure
    /// `len <= CELL_INLINE_CAP` and that the first `len` entries in `data`
    /// are valid (offset+length lies within `body` for non-NULL cells).
    #[inline]
    pub fn from_inline_unchecked(
        body: bytes::Bytes,
        data: [(u32, i32); CELL_INLINE_CAP],
        len: u8,
    ) -> Self {
        debug_assert!(len as usize <= CELL_INLINE_CAP);
        Self {
            body,
            cells: Cells::Inline { data, len },
        }
    }

    /// Construct from a body and a slice of `(offset, length)` pairs.
    /// Caller must guarantee every non-NULL entry `(offset, length)` lies
    /// within `body`.
    pub fn from_entries(body: bytes::Bytes, entries: &[(u32, i32)]) -> Self {
        let cells = if entries.len() <= CELL_INLINE_CAP {
            let mut data = [(0u32, 0i32); CELL_INLINE_CAP];
            data[..entries.len()].copy_from_slice(entries);
            Cells::Inline {
                data,
                len: entries.len() as u8,
            }
        } else {
            Cells::Heap(entries.into())
        };
        Self { body, cells }
    }

    /// Construct a single-cell row pointing at the entirety of `body`.
    /// Used for COPY data chunks.
    pub fn from_full_body(body: bytes::Bytes) -> Self {
        let len = body.len() as i32;
        let mut data = [(0u32, 0i32); CELL_INLINE_CAP];
        data[0] = (0, len);
        Self {
            body,
            cells: Cells::Inline { data, len: 1 },
        }
    }

    fn entries(&self) -> &[(u32, i32)] {
        match &self.cells {
            Cells::Inline { data, len } => &data[..*len as usize],
            Cells::Heap(b) => b,
        }
    }

    pub fn len(&self) -> usize {
        self.entries().len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries().is_empty()
    }

    /// Get cell `idx` as a `&[u8]`. Returns `None` for NULL **or** out-of-range.
    /// Use [`Self::try_cell`] when you need to distinguish the two.
    pub fn cell(&self, idx: usize) -> Option<&[u8]> {
        let (off, len) = *self.entries().get(idx)?;
        if len < 0 {
            return None;
        }
        let start = off as usize;
        let end = start + len as usize;
        Some(&self.body[start..end])
    }

    /// Outer `None` = out-of-range; inner `None` = SQL NULL; `Some(Some(_))` = bytes.
    pub fn try_cell(&self, idx: usize) -> Option<Option<&[u8]>> {
        let (off, len) = *self.entries().get(idx)?;
        if len < 0 {
            return Some(None);
        }
        let start = off as usize;
        let end = start + len as usize;
        Some(Some(&self.body[start..end]))
    }

    /// Iterate over cells; `None` items are SQL NULLs.
    pub fn iter(&self) -> impl Iterator<Item = Option<&[u8]>> + '_ {
        let body = self.body.as_ref();
        self.entries().iter().map(move |&(off, len)| {
            if len < 0 {
                None
            } else {
                let start = off as usize;
                let end = start + len as usize;
                Some(&body[start..end])
            }
        })
    }
}

#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct FieldDescription {
    pub name: String,
    pub table_oid: Oid,
    pub column_id: i16,
    pub type_oid: Oid,
    pub type_size: i16,
    pub type_modifier: i32,
    pub format: FormatCode,
}

impl Default for FieldDescription {
    fn default() -> Self {
        Self {
            name: String::new(),
            table_oid: 0,
            column_id: 0,
            type_oid: 0,
            type_size: -1,
            type_modifier: -1,
            format: FormatCode::Binary,
        }
    }
}

#[derive(Debug, Clone, Default)]
#[non_exhaustive]
pub struct PgError {
    pub severity: String,
    pub code: String,
    pub message: String,
    pub detail: Option<String>,
    pub hint: Option<String>,
    pub position: Option<String>,
}
