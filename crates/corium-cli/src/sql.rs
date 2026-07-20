//! Read-only interactive SQL shell.

use std::path::Path;
use std::time::Instant;

use corium_db::Db;
use corium_peer::Connection;
use corium_sql::{SqlColumn, SqlRow, SqlSession};
use rustyline::DefaultEditor;
use rustyline::error::ReadlineError;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
enum View {
    #[default]
    Current,
    AsOf(u64),
    Since(u64),
    History,
}

impl View {
    fn apply(self, db: &Db) -> Db {
        match self {
            Self::Current => db.clone(),
            Self::AsOf(t) => db.as_of(t),
            Self::Since(t) => db.since(t),
            Self::History => db.history(),
        }
    }
}

#[derive(Default)]
struct Shell {
    view: View,
    timing: bool,
}

enum MetaAction {
    Continue,
    Quit,
}

/// Runs an interactive shell, a command, or a SQL file.
pub async fn run(
    connection: &Connection,
    command: Option<&str>,
    file: Option<&Path>,
) -> Result<(), String> {
    let mut shell = Shell::default();
    if let Some(command) = command {
        return execute_script(&shell, &connection.db(), command).await;
    }
    if let Some(path) = file {
        let sql = std::fs::read_to_string(path)
            .map_err(|error| format!("cannot read {}: {error}", path.display()))?;
        return execute_script(&shell, &connection.db(), &sql).await;
    }

    let mut editor = DefaultEditor::new().map_err(|error| error.to_string())?;
    let mut buffer = String::new();
    println!(
        "Corium SQL for {:?}. End statements with ';'; type \\help for commands.",
        connection.db_name()
    );
    loop {
        let prompt = if buffer.is_empty() {
            "corium-sql> "
        } else {
            "        -> "
        };
        match editor.readline(prompt) {
            Ok(line) => {
                if buffer.is_empty() && line.trim_start().starts_with('\\') {
                    match shell.meta(&connection.db(), line.trim()).await {
                        Ok(MetaAction::Continue) => {}
                        Ok(MetaAction::Quit) => return Ok(()),
                        Err(error) => eprintln!("error: {error}"),
                    }
                    continue;
                }
                if !line.trim().is_empty() {
                    if !buffer.is_empty() {
                        buffer.push('\n');
                    }
                    buffer.push_str(&line);
                }
                let (statements, remainder) = split_statements(&buffer);
                buffer = remainder;
                for statement in statements {
                    let _ = editor.add_history_entry(statement.as_str());
                    let base = connection.db();
                    let execution = shell.execute(&base, &statement);
                    tokio::select! {
                        result = execution => {
                            if let Err(error) = result {
                                eprintln!("error: {error}");
                            }
                        }
                        _ = tokio::signal::ctrl_c() => eprintln!("query cancelled"),
                    }
                }
            }
            Err(ReadlineError::Interrupted) => {
                buffer.clear();
                println!("^C");
            }
            Err(ReadlineError::Eof) if buffer.trim().is_empty() => return Ok(()),
            Err(ReadlineError::Eof) => {
                let statement = std::mem::take(&mut buffer);
                shell.execute(&connection.db(), &statement).await?;
                return Ok(());
            }
            Err(error) => return Err(error.to_string()),
        }
    }
}

impl Shell {
    async fn execute(&self, base: &Db, sql: &str) -> Result<(), String> {
        let started = Instant::now();
        let db = self.view.apply(base);
        let session = SqlSession::new(&db).map_err(|error| error.to_string())?;
        let query = session
            .query(sql)
            .await
            .map_err(|error| error.to_string())?;
        let columns = query.columns().to_vec();
        let rows = query.collect().await.map_err(|error| error.to_string())?;
        print_table(&columns, &rows);
        if self.timing {
            println!("Time: {:.3} ms", started.elapsed().as_secs_f64() * 1_000.0);
        }
        Ok(())
    }

    async fn meta(&mut self, base: &Db, line: &str) -> Result<MetaAction, String> {
        let mut words = line.split_whitespace();
        let command = words.next().unwrap_or_default();
        let argument = words.next();
        if words.next().is_some() {
            return Err("too many command arguments".into());
        }
        match command {
            "\\q" | "\\quit" => Ok(MetaAction::Quit),
            "\\help" | "\\?" => {
                println!(
                    "\\as-of t | \\since t | \\history on|off | \\current | \\basis | \\dt | \\d table | \\timing on|off | \\q"
                );
                Ok(MetaAction::Continue)
            }
            "\\as-of" => {
                self.view = View::AsOf(parse_t(argument, "\\as-of")?);
                println!("View: {}", view_name(self.view));
                Ok(MetaAction::Continue)
            }
            "\\since" => {
                self.view = View::Since(parse_t(argument, "\\since")?);
                println!("View: {}", view_name(self.view));
                Ok(MetaAction::Continue)
            }
            "\\history" => {
                self.view = match argument {
                    Some("on") => View::History,
                    Some("off") => View::Current,
                    _ => return Err("usage: \\history on|off".into()),
                };
                println!("View: {}", view_name(self.view));
                Ok(MetaAction::Continue)
            }
            "\\current" if argument.is_none() => {
                self.view = View::Current;
                println!("View: current");
                Ok(MetaAction::Continue)
            }
            "\\current" => Err("usage: \\current".into()),
            "\\basis" if argument.is_none() => {
                let db = self.view.apply(base);
                println!("basis_t={} view={}", db.basis_t(), view_name(self.view));
                Ok(MetaAction::Continue)
            }
            "\\basis" => Err("usage: \\basis".into()),
            "\\timing" => {
                self.timing = match argument {
                    Some("on") => true,
                    Some("off") => false,
                    _ => return Err("usage: \\timing on|off".into()),
                };
                println!("Timing is {}", if self.timing { "on" } else { "off" });
                Ok(MetaAction::Continue)
            }
            "\\dt" if argument.is_none() => {
                let db = self.view.apply(base);
                let session = SqlSession::new(&db).map_err(|error| error.to_string())?;
                for table in session.tables() {
                    println!("{table}");
                }
                Ok(MetaAction::Continue)
            }
            "\\dt" => Err("usage: \\dt".into()),
            "\\d" => {
                let table = argument.ok_or_else(|| "usage: \\d table".to_owned())?;
                self.execute(base, &format!("SELECT * FROM {table} LIMIT 0"))
                    .await?;
                Ok(MetaAction::Continue)
            }
            _ => Err(format!("unknown SQL shell command {command}; try \\help")),
        }
    }
}

async fn execute_script(shell: &Shell, db: &Db, script: &str) -> Result<(), String> {
    let (mut statements, remainder) = split_statements(script);
    if !remainder.trim().is_empty() {
        statements.push(remainder);
    }
    for statement in statements {
        shell.execute(db, &statement).await?;
    }
    Ok(())
}

fn print_table(columns: &[SqlColumn], rows: &[SqlRow]) {
    if columns.is_empty() {
        println!("({} rows)", rows.len());
        return;
    }
    let mut widths = columns
        .iter()
        .map(|column| column.name.chars().count())
        .collect::<Vec<_>>();
    let rendered = rows
        .iter()
        .map(|row| {
            row.iter()
                .enumerate()
                .map(|(index, value)| {
                    let text = value.to_string().replace('\n', "\\n");
                    widths[index] = widths[index].max(text.chars().count());
                    text
                })
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    print_row(
        &columns
            .iter()
            .map(|column| column.name.clone())
            .collect::<Vec<_>>(),
        &widths,
    );
    println!(
        "{}",
        widths
            .iter()
            .map(|width| "-".repeat(*width))
            .collect::<Vec<_>>()
            .join("-+-")
    );
    for row in &rendered {
        print_row(row, &widths);
    }
    println!("({} rows)", rows.len());
}

fn print_row(row: &[String], widths: &[usize]) {
    println!(
        "{}",
        row.iter()
            .zip(widths)
            .map(|(value, width)| format!("{value:width$}"))
            .collect::<Vec<_>>()
            .join(" | ")
    );
}

/// Splits complete semicolon-terminated statements outside quoted strings and
/// SQL comments. The returned remainder is an incomplete trailing statement.
fn split_statements(input: &str) -> (Vec<String>, String) {
    #[derive(Clone, Copy, Eq, PartialEq)]
    enum State {
        Normal,
        SingleQuote,
        DoubleQuote,
        LineComment,
        BlockComment,
    }
    let bytes = input.as_bytes();
    let mut state = State::Normal;
    let mut statements = Vec::new();
    let mut start = 0;
    let mut index = 0;
    while index < bytes.len() {
        let byte = bytes[index];
        let next = bytes.get(index + 1).copied();
        match state {
            State::Normal => match (byte, next) {
                (b'\'', _) => state = State::SingleQuote,
                (b'"', _) => state = State::DoubleQuote,
                (b'-', Some(b'-')) => {
                    state = State::LineComment;
                    index += 1;
                }
                (b'/', Some(b'*')) => {
                    state = State::BlockComment;
                    index += 1;
                }
                (b';', _) => {
                    let statement = input[start..index].trim();
                    if !statement.is_empty() {
                        statements.push(statement.to_owned());
                    }
                    start = index + 1;
                }
                _ => {}
            },
            State::SingleQuote => {
                if byte == b'\'' {
                    if next == Some(b'\'') {
                        index += 1;
                    } else {
                        state = State::Normal;
                    }
                }
            }
            State::DoubleQuote => {
                if byte == b'"' {
                    if next == Some(b'"') {
                        index += 1;
                    } else {
                        state = State::Normal;
                    }
                }
            }
            State::LineComment if byte == b'\n' => state = State::Normal,
            State::BlockComment if byte == b'*' && next == Some(b'/') => {
                state = State::Normal;
                index += 1;
            }
            State::LineComment | State::BlockComment => {}
        }
        index += 1;
    }
    (statements, input[start..].trim().to_owned())
}

fn parse_t(argument: Option<&str>, command: &str) -> Result<u64, String> {
    argument
        .ok_or_else(|| format!("usage: {command} t"))?
        .parse()
        .map_err(|_| format!("{command} requires a non-negative transaction number"))
}

fn view_name(view: View) -> String {
    match view {
        View::Current => "current".into(),
        View::AsOf(t) => format!("as-of {t}"),
        View::Since(t) => format!("since {t}"),
        View::History => "history".into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn statement_splitter_respects_quotes_and_comments() {
        let sql = "SELECT ';' AS x; -- ; ignored\nSELECT \"a;b\"; SELECT 3";
        let (statements, remainder) = split_statements(sql);
        assert_eq!(
            statements,
            vec!["SELECT ';' AS x", "-- ; ignored\nSELECT \"a;b\""]
        );
        assert_eq!(remainder, "SELECT 3");
    }
}
