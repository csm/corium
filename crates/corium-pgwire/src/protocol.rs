//! `PostgreSQL` v3 frontend/backend message framing.
//!
//! This module is deliberately small: it reads the frontend messages the
//! read-only server understands and writes the backend messages it needs to
//! answer them. Wire integers are big-endian; strings are NUL-terminated
//! UTF-8. See the `PostgreSQL` "Frontend/Backend Protocol" chapter for the
//! canonical layout.

use std::io;

use tokio::io::{AsyncRead, AsyncReadExt as _, AsyncWrite, AsyncWriteExt as _};

/// Protocol version 3.0, as sent in a startup message (`0x0003_0000`).
pub(crate) const PROTOCOL_VERSION_3: i32 = 196_608;
/// Magic version selecting TLS negotiation instead of a normal startup.
const SSL_REQUEST_CODE: i32 = 80_877_103;
/// Magic version selecting GSSAPI encryption negotiation.
const GSS_ENC_REQUEST_CODE: i32 = 80_877_104;
/// Largest message body we will buffer, guarding against hostile length
/// prefixes. Read-only query text and parameters stay well under this.
const MAX_MESSAGE_LEN: usize = 64 * 1024 * 1024;

/// A parsed startup handshake, after any TLS/GSS negotiation was declined.
pub(crate) struct Startup {
    /// Connection parameters (`user`, `database`, `application_name`, ...).
    pub(crate) parameters: Vec<(String, String)>,
}

impl Startup {
    /// Looks up a startup parameter by key.
    pub(crate) fn get(&self, key: &str) -> Option<&str> {
        self.parameters
            .iter()
            .find(|(name, _)| name == key)
            .map(|(_, value)| value.as_str())
    }
}

/// A frontend (client) message the server acts on.
#[derive(Debug)]
pub(crate) enum Frontend {
    /// Simple query: one SQL string that may contain several statements.
    Query(String),
    /// A cleartext password sent in response to an authentication request.
    Password(String),
    /// Extended protocol: prepare `query` under `name`.
    Parse {
        /// Prepared-statement name (empty is the unnamed statement).
        name: String,
        /// SQL text.
        query: String,
        /// Number of parameter type OIDs the client declared.
        parameter_count: usize,
    },
    /// Extended protocol: bind a portal to a prepared statement.
    Bind {
        /// Destination portal name (empty is the unnamed portal).
        portal: String,
        /// Source prepared-statement name.
        statement: String,
        /// Number of bound parameter values.
        parameter_count: usize,
        /// Requested result column format codes (0 = text, 1 = binary).
        result_formats: Vec<i16>,
    },
    /// Extended protocol: describe a statement (`S`) or portal (`P`).
    Describe {
        /// `b'S'` for a statement, `b'P'` for a portal.
        kind: u8,
        /// Object name.
        name: String,
    },
    /// Extended protocol: execute a bound portal.
    Execute {
        /// Portal name.
        portal: String,
    },
    /// Extended protocol: close a statement (`S`) or portal (`P`).
    Close {
        /// `b'S'` or `b'P'`.
        kind: u8,
        /// Object name.
        name: String,
    },
    /// Extended protocol synchronization point.
    Sync,
    /// Flush the output buffer without a synchronization point.
    Flush,
    /// Client asked to end the session.
    Terminate,
}

/// Reads frontend messages from a client stream.
pub(crate) struct FrontendReader<R> {
    inner: R,
}

impl<R: AsyncRead + Unpin> FrontendReader<R> {
    /// Wraps a readable client stream.
    pub(crate) const fn new(inner: R) -> Self {
        Self { inner }
    }

    /// Reads the startup handshake, declining TLS and GSSAPI negotiation.
    ///
    /// The first message may be an `SSLRequest`/`GSSENCRequest`; the caller's
    /// writer sends the single-byte refusal, so we loop until a real startup
    /// message (protocol 3.0) arrives.
    pub(crate) async fn read_startup<W: AsyncWrite + Unpin>(
        &mut self,
        writer: &mut BackendWriter<W>,
    ) -> io::Result<Startup> {
        loop {
            let length = self.inner.read_i32().await?;
            let body_len = message_body_len(length, 4)?;
            let mut body = vec![0u8; body_len];
            self.inner.read_exact(&mut body).await?;
            let version = i32::from_be_bytes([body[0], body[1], body[2], body[3]]);
            match version {
                SSL_REQUEST_CODE | GSS_ENC_REQUEST_CODE => {
                    writer.write_negotiation_refusal().await?;
                }
                PROTOCOL_VERSION_3 => {
                    return Ok(Startup {
                        parameters: parse_startup_parameters(&body[4..])?,
                    });
                }
                other => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("unsupported protocol version {other}"),
                    ));
                }
            }
        }
    }

    /// Reads the next regular message, or `None` at end of stream.
    pub(crate) async fn read_message(&mut self) -> io::Result<Option<Frontend>> {
        let tag = match self.inner.read_u8().await {
            Ok(tag) => tag,
            Err(error) if error.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
            Err(error) => return Err(error),
        };
        let length = self.inner.read_i32().await?;
        // The length prefix counts itself plus the body, but not the tag byte
        // (already consumed above), so the body is `length - 4`.
        let body_len = message_body_len(length, 4)?;
        let mut body = vec![0u8; body_len];
        self.inner.read_exact(&mut body).await?;
        let mut cursor = Cursor::new(&body);
        let message = match tag {
            b'Q' => Frontend::Query(cursor.read_cstr()?),
            b'p' => Frontend::Password(cursor.read_cstr()?),
            b'P' => {
                let name = cursor.read_cstr()?;
                let query = cursor.read_cstr()?;
                let parameter_count = usize::from(cursor.read_i16()?.max(0).unsigned_abs());
                Frontend::Parse {
                    name,
                    query,
                    parameter_count,
                }
            }
            b'B' => {
                let portal = cursor.read_cstr()?;
                let statement = cursor.read_cstr()?;
                let format_count = cursor.read_i16()?;
                for _ in 0..format_count {
                    let _ = cursor.read_i16()?;
                }
                let parameter_count = usize::from(cursor.read_i16()?.max(0).unsigned_abs());
                for _ in 0..parameter_count {
                    let len = cursor.read_i32()?;
                    if len > 0 {
                        cursor.skip(usize::try_from(len).unwrap_or(0))?;
                    }
                }
                let result_format_count = cursor.read_i16()?.max(0);
                let mut result_formats =
                    Vec::with_capacity(usize::from(result_format_count.unsigned_abs()));
                for _ in 0..result_format_count {
                    result_formats.push(cursor.read_i16()?);
                }
                Frontend::Bind {
                    portal,
                    statement,
                    parameter_count,
                    result_formats,
                }
            }
            b'D' => {
                let kind = cursor.read_u8()?;
                let name = cursor.read_cstr()?;
                Frontend::Describe { kind, name }
            }
            b'E' => {
                let portal = cursor.read_cstr()?;
                let _max_rows = cursor.read_i32()?;
                Frontend::Execute { portal }
            }
            b'C' => {
                let kind = cursor.read_u8()?;
                let name = cursor.read_cstr()?;
                Frontend::Close { kind, name }
            }
            b'S' => Frontend::Sync,
            b'H' => Frontend::Flush,
            b'X' => Frontend::Terminate,
            other => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("unsupported frontend message '{}'", char::from(other)),
                ));
            }
        };
        Ok(Some(message))
    }
}

/// A backend field description used to build a `RowDescription`.
pub(crate) struct FieldDescription {
    /// Column name.
    pub(crate) name: String,
    /// `PostgreSQL` type OID.
    pub(crate) type_oid: i32,
    /// Type length in bytes, or -1 for variable-length types.
    pub(crate) type_len: i16,
}

/// Severity/code/message triple for an `ErrorResponse`.
pub(crate) struct ErrorFields<'a> {
    /// `SQLSTATE` code, e.g. `"42601"`.
    pub(crate) code: &'a str,
    /// Human-readable message.
    pub(crate) message: &'a str,
}

/// Buffers and writes backend messages to a client stream.
pub(crate) struct BackendWriter<W> {
    inner: W,
    buffer: Vec<u8>,
}

impl<W: AsyncWrite + Unpin> BackendWriter<W> {
    /// Wraps a writable client stream.
    pub(crate) const fn new(inner: W) -> Self {
        Self {
            inner,
            buffer: Vec::new(),
        }
    }

    /// Sends the single-byte `N` that declines TLS/GSS negotiation.
    async fn write_negotiation_refusal(&mut self) -> io::Result<()> {
        self.inner.write_all(b"N").await?;
        self.inner.flush().await
    }

    /// `AuthenticationOk`.
    pub(crate) fn authentication_ok(&mut self) {
        self.frame(b'R', |body| body.extend_from_slice(&0i32.to_be_bytes()));
    }

    /// `AuthenticationCleartextPassword`.
    pub(crate) fn authentication_cleartext_password(&mut self) {
        self.frame(b'R', |body| body.extend_from_slice(&3i32.to_be_bytes()));
    }

    /// `ParameterStatus` reporting one server setting.
    pub(crate) fn parameter_status(&mut self, name: &str, value: &str) {
        self.frame(b'S', |body| {
            put_cstr(body, name);
            put_cstr(body, value);
        });
    }

    /// `BackendKeyData` (a placeholder process id and secret key).
    pub(crate) fn backend_key_data(&mut self, process_id: i32, secret: i32) {
        self.frame(b'K', |body| {
            body.extend_from_slice(&process_id.to_be_bytes());
            body.extend_from_slice(&secret.to_be_bytes());
        });
    }

    /// `ReadyForQuery` with the given transaction status byte (`I`/`T`/`E`).
    pub(crate) fn ready_for_query(&mut self, status: u8) {
        self.frame(b'Z', |body| body.push(status));
    }

    /// `RowDescription` for a result's columns (always text format).
    pub(crate) fn row_description(&mut self, fields: &[FieldDescription]) {
        self.frame(b'T', |body| {
            put_i16(body, i16::try_from(fields.len()).unwrap_or(i16::MAX));
            for field in fields {
                put_cstr(body, &field.name);
                body.extend_from_slice(&0i32.to_be_bytes()); // table OID
                put_i16(body, 0); // column attribute number
                body.extend_from_slice(&field.type_oid.to_be_bytes());
                put_i16(body, field.type_len);
                body.extend_from_slice(&(-1i32).to_be_bytes()); // type modifier
                put_i16(body, 0); // text format
            }
        });
    }

    /// `NoData`, for a describe of a statement that returns no columns.
    pub(crate) fn no_data(&mut self) {
        self.frame(b'n', |_| {});
    }

    /// `ParameterDescription` (this server only supports zero parameters).
    pub(crate) fn parameter_description_empty(&mut self) {
        self.frame(b't', |body| put_i16(body, 0));
    }

    /// One `DataRow`. Each value is already encoded as its text form, or
    /// `None` for SQL NULL.
    pub(crate) fn data_row(&mut self, values: &[Option<Vec<u8>>]) {
        self.frame(b'D', |body| {
            put_i16(body, i16::try_from(values.len()).unwrap_or(i16::MAX));
            for value in values {
                match value {
                    Some(bytes) => {
                        body.extend_from_slice(
                            &i32::try_from(bytes.len()).unwrap_or(i32::MAX).to_be_bytes(),
                        );
                        body.extend_from_slice(bytes);
                    }
                    None => body.extend_from_slice(&(-1i32).to_be_bytes()),
                }
            }
        });
    }

    /// `CommandComplete` carrying a command tag such as `SELECT 3`.
    pub(crate) fn command_complete(&mut self, tag: &str) {
        self.frame(b'C', |body| put_cstr(body, tag));
    }

    /// `EmptyQueryResponse`, for a blank query string.
    pub(crate) fn empty_query_response(&mut self) {
        self.frame(b'I', |_| {});
    }

    /// `ParseComplete`.
    pub(crate) fn parse_complete(&mut self) {
        self.frame(b'1', |_| {});
    }

    /// `BindComplete`.
    pub(crate) fn bind_complete(&mut self) {
        self.frame(b'2', |_| {});
    }

    /// `CloseComplete`.
    pub(crate) fn close_complete(&mut self) {
        self.frame(b'3', |_| {});
    }

    /// `ErrorResponse` with severity `ERROR`.
    pub(crate) fn error_response(&mut self, fields: &ErrorFields<'_>) {
        self.frame(b'E', |body| {
            body.push(b'S');
            put_cstr(body, "ERROR");
            body.push(b'V');
            put_cstr(body, "ERROR");
            body.push(b'C');
            put_cstr(body, fields.code);
            body.push(b'M');
            put_cstr(body, fields.message);
            body.push(0);
        });
    }

    /// Frames a message with `tag`, filling its body via `fill`.
    fn frame(&mut self, tag: u8, fill: impl FnOnce(&mut Vec<u8>)) {
        self.buffer.push(tag);
        let length_at = self.buffer.len();
        self.buffer.extend_from_slice(&0i32.to_be_bytes());
        fill(&mut self.buffer);
        let length = i32::try_from(self.buffer.len() - length_at).unwrap_or(i32::MAX);
        self.buffer[length_at..length_at + 4].copy_from_slice(&length.to_be_bytes());
    }

    /// Flushes all buffered messages to the underlying stream.
    pub(crate) async fn flush(&mut self) -> io::Result<()> {
        if !self.buffer.is_empty() {
            self.inner.write_all(&self.buffer).await?;
            self.buffer.clear();
        }
        self.inner.flush().await
    }
}

/// Validates a wire length prefix and returns the remaining body length.
fn message_body_len(length: i32, header: usize) -> io::Result<usize> {
    let length = usize::try_from(length)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "negative message length"))?;
    if length < header || length > MAX_MESSAGE_LEN {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("invalid message length {length}"),
        ));
    }
    Ok(length - header)
}

/// Parses the `key\0value\0...\0` parameter block of a startup message.
fn parse_startup_parameters(body: &[u8]) -> io::Result<Vec<(String, String)>> {
    let mut cursor = Cursor::new(body);
    let mut parameters = Vec::new();
    loop {
        let key = cursor.read_cstr()?;
        if key.is_empty() {
            break;
        }
        let value = cursor.read_cstr()?;
        parameters.push((key, value));
    }
    Ok(parameters)
}

/// A minimal cursor over a message body.
struct Cursor<'a> {
    bytes: &'a [u8],
    position: usize,
}

impl<'a> Cursor<'a> {
    const fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, position: 0 }
    }

    fn read_u8(&mut self) -> io::Result<u8> {
        let byte = *self.bytes.get(self.position).ok_or_else(truncated)?;
        self.position += 1;
        Ok(byte)
    }

    fn read_i16(&mut self) -> io::Result<i16> {
        let end = self.position + 2;
        let slice = self.bytes.get(self.position..end).ok_or_else(truncated)?;
        self.position = end;
        Ok(i16::from_be_bytes([slice[0], slice[1]]))
    }

    fn read_i32(&mut self) -> io::Result<i32> {
        let end = self.position + 4;
        let slice = self.bytes.get(self.position..end).ok_or_else(truncated)?;
        self.position = end;
        Ok(i32::from_be_bytes([slice[0], slice[1], slice[2], slice[3]]))
    }

    fn skip(&mut self, count: usize) -> io::Result<()> {
        let end = self.position.checked_add(count).ok_or_else(truncated)?;
        if end > self.bytes.len() {
            return Err(truncated());
        }
        self.position = end;
        Ok(())
    }

    fn read_cstr(&mut self) -> io::Result<String> {
        let start = self.position;
        while self.position < self.bytes.len() {
            if self.bytes[self.position] == 0 {
                let text = std::str::from_utf8(&self.bytes[start..self.position])
                    .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "invalid UTF-8"))?
                    .to_owned();
                self.position += 1;
                return Ok(text);
            }
            self.position += 1;
        }
        Err(truncated())
    }
}

fn truncated() -> io::Error {
    io::Error::new(io::ErrorKind::UnexpectedEof, "truncated message")
}

fn put_i16(buffer: &mut Vec<u8>, value: i16) {
    buffer.extend_from_slice(&value.to_be_bytes());
}

fn put_cstr(buffer: &mut Vec<u8>, value: &str) {
    buffer.extend_from_slice(value.as_bytes());
    buffer.push(0);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cursor_reads_strings_and_integers() {
        let bytes = [b'h', b'i', 0, 0x00, 0x2a, 0xff, 0xff, 0xff, 0xff];
        let mut cursor = Cursor::new(&bytes);
        assert_eq!(cursor.read_cstr().unwrap(), "hi");
        assert_eq!(cursor.read_i16().unwrap(), 42);
        assert_eq!(cursor.read_i32().unwrap(), -1);
        assert!(cursor.read_u8().is_err());
    }

    #[test]
    fn frame_writes_length_prefix() {
        let mut writer = BackendWriter::new(Vec::new());
        writer.command_complete("SELECT 1");
        // 'C' + 4-byte length + "SELECT 1\0".
        assert_eq!(writer.buffer[0], b'C');
        let length = i32::from_be_bytes([
            writer.buffer[1],
            writer.buffer[2],
            writer.buffer[3],
            writer.buffer[4],
        ]);
        assert_eq!(usize::try_from(length).unwrap(), writer.buffer.len() - 1);
    }
}
