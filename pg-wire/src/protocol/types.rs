/// PostgreSQL OID type.
pub type Oid = u32;

/// Wire format codes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i16)]
pub enum FormatCode {
    Text = 0,
    Binary = 1,
}

/// Frontend (client → server) messages.
#[derive(Debug)]
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
    Execute {
        portal: &'a [u8],
        max_rows: i32,
    },
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
    SASLInitialResponse {
        mechanism: &'a [u8],
        data: &'a [u8],
    },
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
pub enum BackendMsg {
    AuthenticationOk,
    AuthenticationCleartextPassword,
    AuthenticationMd5Password { salt: [u8; 4] },
    AuthenticationSASL { mechanisms: Vec<String> },
    AuthenticationSASLContinue { data: Vec<u8> },
    AuthenticationSASLFinal { data: Vec<u8> },
    ParameterStatus { name: String, value: String },
    BackendKeyData { pid: i32, secret: i32 },
    ReadyForQuery { status: u8 },
    ParseComplete,
    BindComplete,
    CloseComplete,
    NoData,
    CommandComplete { tag: String },
    DataRow { columns: Vec<Option<Vec<u8>>> },
    RowDescription { fields: Vec<FieldDescription> },
    ErrorResponse { fields: PgError },
    NoticeResponse { fields: PgError },
    EmptyQueryResponse,
    /// ParameterDescription: param type OIDs from a Describe Statement.
    ParameterDescription { type_oids: Vec<Oid> },
    /// NotificationResponse: async notification from LISTEN/NOTIFY.
    NotificationResponse { pid: i32, channel: String, payload: String },
    /// PortalSuspended: Execute completed with row limit, portal still open.
    PortalSuspended,
    /// CopyInResponse: server is ready to receive COPY data.
    CopyInResponse {
        format: u8,           // 0=text, 1=binary
        column_formats: Vec<i16>,
    },
    /// CopyOutResponse: server is about to send COPY data.
    CopyOutResponse {
        format: u8,
        column_formats: Vec<i16>,
    },
    /// CopyData: a chunk of COPY data (in either direction).
    CopyData { data: Vec<u8> },
    /// CopyDone: COPY stream completed.
    CopyDone,
}

#[derive(Debug, Clone)]
pub struct FieldDescription {
    pub name: String,
    pub table_oid: Oid,
    pub column_id: i16,
    pub type_oid: Oid,
    pub type_size: i16,
    pub type_modifier: i32,
    pub format: FormatCode,
}

#[derive(Debug, Clone, Default)]
pub struct PgError {
    pub severity: String,
    pub code: String,
    pub message: String,
    pub detail: Option<String>,
    pub hint: Option<String>,
    pub position: Option<String>,
}
