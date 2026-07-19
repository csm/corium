//! Shared helpers for the `MusicBrainz` example: a streaming reader for
//! whitespace/newline-separated top-level EDN forms (so multi-gigabyte
//! datasets load without being held in memory at once) and endpoint parsing.

use std::io::{self, BufRead};

/// Turns a transactor endpoint (`http://host:port`, `host:port`, or a
/// `corium://host:port/db` URL) plus a database name into the pair the
/// example needs: a gRPC endpoint (`http://host:port`) for the peer API and a
/// `corium://host:port/db` URL for `corium.api/connect`.
///
/// # Errors
/// Returns a message when the endpoint has no host:port authority.
pub fn endpoints(transactor: &str, db: &str) -> Result<(String, String), String> {
    let authority = transactor
        .strip_prefix("corium://")
        .map(|rest| rest.split('/').next().unwrap_or(rest))
        .or_else(|| transactor.strip_prefix("http://"))
        .or_else(|| transactor.strip_prefix("https://"))
        .unwrap_or(transactor)
        .trim_end_matches('/');
    if authority.is_empty() || !authority.contains(':') {
        return Err(format!(
            "expected a host:port transactor endpoint, got {transactor:?}"
        ));
    }
    Ok((
        format!("http://{authority}"),
        format!("corium://{authority}/{db}"),
    ))
}

/// A streaming iterator over complete top-level EDN forms read from any
/// buffered source. Each item is the source text of one form (a transaction
/// vector or an entity map), ready to hand to the EDN reader.
///
/// Top-level forms must be collections — a list `(…)`, vector `[…]`, or map
/// `{…}` — which is exactly the shape of transaction data. Interspersed
/// whitespace, commas, and `;` line comments between forms are skipped.
/// Strings, `\c` character literals, and comments inside a form are handled
/// so their brackets and quotes never confuse the delimiter scan.
pub struct FormReader<R: BufRead> {
    bytes: io::Bytes<R>,
}

impl<R: BufRead> FormReader<R> {
    /// Wraps a buffered reader.
    pub fn new(reader: R) -> Self {
        Self {
            bytes: reader.bytes(),
        }
    }

    fn next_byte(&mut self) -> Option<io::Result<u8>> {
        self.bytes.next()
    }

    fn eof() -> io::Error {
        io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "unbalanced EDN form at end of input",
        )
    }
}

impl<R: BufRead> Iterator for FormReader<R> {
    type Item = io::Result<String>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut buf: Vec<u8> = Vec::new();
        let mut depth = 0_i32;
        let mut in_string = false;
        let mut started = false;

        loop {
            let byte = match self.next_byte() {
                None if started => return Some(Err(Self::eof())),
                None => return None,
                Some(Err(error)) => return Some(Err(error)),
                Some(Ok(byte)) => byte,
            };

            if !started {
                match byte {
                    b';' => {
                        // Line comment between forms: skip to end of line.
                        loop {
                            match self.next_byte() {
                                Some(Ok(b'\n')) | None => break,
                                Some(Ok(_)) => {}
                                Some(Err(error)) => return Some(Err(error)),
                            }
                        }
                    }
                    b if b.is_ascii_whitespace() || b == b',' => {}
                    b'[' | b'(' | b'{' => {
                        started = true;
                        depth = 1;
                        buf.push(byte);
                    }
                    other => {
                        return Some(Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            format!(
                                "top-level EDN form must be a list, vector, or map; found byte {other:#x}"
                            ),
                        )));
                    }
                }
                continue;
            }

            buf.push(byte);

            if in_string {
                if byte == b'\\' {
                    match self.next_byte() {
                        Some(Ok(next)) => buf.push(next),
                        Some(Err(error)) => return Some(Err(error)),
                        None => return Some(Err(Self::eof())),
                    }
                } else if byte == b'"' {
                    in_string = false;
                }
                continue;
            }

            match byte {
                b'"' => in_string = true,
                b'\\' => {
                    // Character literal (`\a`, `\newline`, `\{` …): the byte
                    // right after the backslash is data, never a delimiter.
                    match self.next_byte() {
                        Some(Ok(next)) => buf.push(next),
                        Some(Err(error)) => return Some(Err(error)),
                        None => return Some(Err(Self::eof())),
                    }
                }
                b';' => loop {
                    match self.next_byte() {
                        Some(Ok(newline @ b'\n')) => {
                            buf.push(newline);
                            break;
                        }
                        Some(Ok(other)) => buf.push(other),
                        Some(Err(error)) => return Some(Err(error)),
                        None => break,
                    }
                },
                b'[' | b'(' | b'{' => depth += 1,
                b']' | b')' | b'}' => {
                    depth -= 1;
                    if depth == 0 {
                        return Some(Ok(String::from_utf8_lossy(&buf).into_owned()));
                    }
                }
                _ => {}
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    fn forms(input: &str) -> Vec<String> {
        FormReader::new(Cursor::new(input.as_bytes().to_vec()))
            .map(|form| form.expect("form"))
            .collect()
    }

    #[test]
    fn splits_top_level_forms_across_lines_and_comments() {
        let input = "; leading comment\n\
             [{:db/id \"a\" :artist/name \"Portishead\"}]\n\n\
             [{:db/id \"b\"\n  :artist/name \"Radiohead\"}] ; trailing\n";
        let got = forms(input);
        assert_eq!(got.len(), 2);
        assert!(got[0].contains("Portishead"));
        assert!(got[1].contains("Radiohead"));
    }

    #[test]
    fn brackets_inside_strings_and_char_literals_do_not_split() {
        let input = r#"[{:db/id "x" :note "a ] } ) tricky \" string"} \] \}]
                       [{:db/id "y"}]"#;
        let got = forms(input);
        assert_eq!(got.len(), 2, "got {got:?}");
        assert!(got[0].contains("tricky"));
        assert!(got[1].contains(":db/id \"y\""));
    }

    #[test]
    fn nested_collections_and_sets_stay_together() {
        let input = "[{:db/id \"m\" :release/media [{:medium/position 1}]\n  :tags #{:a :b}}]";
        let got = forms(input);
        assert_eq!(got.len(), 1);
        assert!(got[0].contains(":medium/position 1"));
        assert!(got[0].contains("#{:a :b}"));
    }

    #[test]
    fn unbalanced_tail_is_an_error() {
        let mut reader = FormReader::new(Cursor::new(b"[{:db/id \"z\"".to_vec()));
        assert!(reader.next().expect("item").is_err());
    }

    #[test]
    fn endpoints_normalizes_forms() {
        assert_eq!(
            endpoints("http://127.0.0.1:4334", "mbrainz").expect("ok"),
            (
                "http://127.0.0.1:4334".to_owned(),
                "corium://127.0.0.1:4334/mbrainz".to_owned()
            )
        );
        assert_eq!(
            endpoints("localhost:4334", "mbrainz").expect("ok").1,
            "corium://localhost:4334/mbrainz"
        );
        assert!(endpoints("no-port", "mbrainz").is_err());
    }
}
