//! Interactive peer-local query console.

use std::time::Instant;

use corium_core::{EntityId, Value};
use corium_db::Db;
use corium_peer::Connection;
use corium_query::ast::{InSpec, parse_query};
use corium_query::edn::read_one;
use corium_query::{ExecOptions, QInput};
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

/// What the caller should do after a console line executes.
pub(crate) enum Action {
    /// Print (or display) this text.
    Output(String),
    /// Enter the live tx-report watch.
    Watch,
    /// Leave the console.
    Quit,
}

/// Line-oriented console state: the active time view and output options,
/// shared by the readline console and the TUI query pane.
#[derive(Default)]
pub(crate) struct Session {
    view: View,
    timing: bool,
}

impl Session {
    /// Applies the session's time view (`:as-of`, `:since`, `:history`) to a
    /// base database value.
    pub(crate) fn apply_view(&self, base: &Db) -> Db {
        self.view.apply(base)
    }

    /// Human-readable name of the active time view.
    pub(crate) fn view_label(&self) -> String {
        view_name(self.view)
    }

    /// Executes one console line (a `:command`, query, or pull form).
    pub(crate) fn execute(&mut self, base: &Db, line: &str) -> Result<Action, String> {
        let line = line.trim();
        if line.is_empty() {
            return Ok(Action::Output(String::new()));
        }
        if line.starts_with(':') {
            return self.command(base, line);
        }
        let view = self.view.apply(base);
        let form = read_one(line).map_err(|error| error.to_string())?;
        if let Some(result) = execute_pull(&view, &form)? {
            return Ok(Action::Output(result));
        }
        let query = parse_query(&form).map_err(|error| error.to_string())?;
        let mut inputs = Vec::with_capacity(query.inputs.len());
        for input in &query.inputs {
            match input {
                InSpec::Db(_) => inputs.push(QInput::Db(&view)),
                _ => {
                    return Err(
                        "console queries currently accept database inputs only; use corium.api for parameterized queries"
                            .into(),
                    );
                }
            }
        }
        let started = Instant::now();
        let (result, report) = corium_query::run(&query, &inputs, ExecOptions::default())
            .map_err(|error| error.to_string())?;
        if self.timing {
            Ok(Action::Output(format!(
                "{result}\n;; {:.3} ms, {} datoms scanned",
                started.elapsed().as_secs_f64() * 1_000.0,
                report.datoms_scanned
            )))
        } else {
            Ok(Action::Output(result.to_string()))
        }
    }

    #[allow(clippy::too_many_lines)]
    fn command(&mut self, base: &Db, line: &str) -> Result<Action, String> {
        let mut words = line.split_whitespace();
        let command = words.next().unwrap_or_default();
        let argument = words.next();
        let mut no_more = || {
            words
                .next()
                .is_none()
                .then_some(())
                .ok_or_else(|| "too many command arguments".to_owned())
        };
        match command {
            ":as-of" => {
                let t = parse_t(argument, ":as-of")?;
                no_more()?;
                self.view = View::AsOf(t);
                Ok(Action::Output(format!(";; view is now as-of {t}")))
            }
            ":since" => {
                let t = parse_t(argument, ":since")?;
                no_more()?;
                self.view = View::Since(t);
                Ok(Action::Output(format!(";; view is now since {t}")))
            }
            ":history" => {
                no_more()?;
                match argument {
                    Some("on") => self.view = View::History,
                    Some("off") => self.view = View::Current,
                    _ => return Err("usage: :history on|off".into()),
                }
                Ok(Action::Output(format!(";; view is now {}", view_name(self.view))))
            }
            ":current" => {
                if argument.is_some() {
                    return Err("usage: :current".into());
                }
                self.view = View::Current;
                Ok(Action::Output(";; view is now current".into()))
            }
            ":basis" => {
                if argument.is_some() {
                    return Err("usage: :basis".into());
                }
                let view = self.view.apply(base);
                Ok(Action::Output(format!(
                    "{{:basis-t {} :view {}}}",
                    view.basis_t(),
                    view_name(self.view)
                )))
            }
            ":stats" => {
                if argument.is_some() {
                    return Err("usage: :stats".into());
                }
                let view = self.view.apply(base);
                let stats = view.stats();
                Ok(Action::Output(format!(
                    "{{:basis-t {} :datoms {} :entities {} :attributes {}}}",
                    view.basis_t(), stats.datoms, stats.entities, stats.attributes
                )))
            }
            ":schema" => {
                no_more()?;
                let view = self.view.apply(base);
                let rows = view
                    .schema()
                    .iter()
                    .filter(|(id, _)| {
                        argument.is_none_or(|name| {
                            view.idents().ident(**id).is_some_and(|ident| {
                                ident.to_string().trim_start_matches(':')
                                    == name.trim_start_matches(':')
                            })
                        })
                    })
                    .map(|(id, attr)| {
                        let name = view
                            .idents()
                            .ident(*id)
                            .map_or_else(|| id.raw().to_string(), ToString::to_string);
                        format!(
                            "{{:ident {name} :value-type :{:?} :cardinality :{:?} :unique {:?} :index {} :component {} :no-history {}}}",
                            attr.value_type,
                            attr.cardinality,
                            attr.unique,
                            attr.indexed,
                            attr.is_component,
                            attr.no_history
                        )
                    })
                    .collect::<Vec<_>>();
                if argument.is_some() && rows.is_empty() {
                    return Err(format!("unknown attribute {}", argument.unwrap_or_default()));
                }
                Ok(Action::Output(format!("[{}]", rows.join("\n "))))
            }
            ":timing" => {
                no_more()?;
                self.timing = match argument {
                    Some("on") => true,
                    Some("off") => false,
                    _ => return Err("usage: :timing on|off".into()),
                };
                Ok(Action::Output(format!(";; timing {}", argument.unwrap_or_default())))
            }
            ":watch" => {
                if argument.is_some() {
                    return Err("usage: :watch".into());
                }
                Ok(Action::Watch)
            }
            ":quit" | ":exit" => Ok(Action::Quit),
            ":help" => Ok(Action::Output(
                ":as-of t | :since t | :history on|off | :current | :basis | :schema [attr] | :stats | :watch | :timing on|off | :quit".into(),
            )),
            _ => Err(format!("unknown console command {command}; try :help")),
        }
    }
}

/// Executes a top-level `(pull pattern entity)` form, returning `Ok(None)`
/// when the form is not a pull expression.
pub(crate) fn execute_pull(
    db: &Db,
    form: &corium_query::edn::Edn,
) -> Result<Option<String>, String> {
    let corium_query::edn::Edn::List(items) = form else {
        return Ok(None);
    };
    let [operation, pattern, entity] = items.as_slice() else {
        return Ok(None);
    };
    if operation.as_symbol() != Some("pull") {
        return Ok(None);
    }
    let eid = match entity {
        corium_query::edn::Edn::Long(raw) => u64::try_from(*raw)
            .map(EntityId::from_raw)
            .map_err(|_| "pull entity id cannot be negative".to_owned())?,
        corium_query::edn::Edn::Keyword(keyword) => db
            .idents()
            .entid(keyword)
            .ok_or_else(|| format!("unknown entity ident {keyword}"))?,
        corium_query::edn::Edn::Vector(parts) if parts.len() == 2 => {
            let attr = parts[0]
                .as_keyword()
                .and_then(|keyword| db.idents().entid(keyword))
                .ok_or_else(|| "unknown lookup-ref attribute".to_owned())?;
            let value = corium_query::boundary::edn_to_value(Some(db), &parts[1])
                .ok_or_else(|| "invalid lookup-ref value".to_owned())?;
            db.lookup(attr, &value)
                .ok_or_else(|| "lookup ref did not resolve".to_owned())?
        }
        other => match corium_query::boundary::edn_to_value(Some(db), other) {
            Some(Value::Ref(eid)) => eid,
            _ => return Err("pull entity must be an id, ident, or lookup ref".into()),
        },
    };
    corium_query::pull(db, pattern, eid)
        .map(|result| Some(result.to_string()))
        .map_err(|error| error.to_string())
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
        View::AsOf(t) => format!("[:as-of {t}]"),
        View::Since(t) => format!("[:since {t}]"),
        View::History => "history".into(),
    }
}

/// Runs the interactive console until EOF or `:quit`.
pub async fn run(connection: &Connection) -> Result<(), String> {
    let mut editor = DefaultEditor::new().map_err(|error| error.to_string())?;
    let mut session = Session::default();
    println!(
        "Corium console for {:?}. Type :help for commands.",
        connection.db_name()
    );
    loop {
        match editor.readline("corium> ") {
            Ok(line) => {
                if !line.trim().is_empty() {
                    let _ = editor.add_history_entry(line.as_str());
                }
                match session.execute(&connection.db(), &line) {
                    Ok(Action::Output(output)) if !output.is_empty() => println!("{output}"),
                    Ok(Action::Output(_)) => {}
                    Ok(Action::Quit) => return Ok(()),
                    Ok(Action::Watch) => watch(connection).await?,
                    Err(error) => eprintln!("error: {error}"),
                }
            }
            Err(ReadlineError::Interrupted) => println!("^C"),
            Err(ReadlineError::Eof) => return Ok(()),
            Err(error) => return Err(error.to_string()),
        }
    }
}

async fn watch(connection: &Connection) -> Result<(), String> {
    let mut reports = connection.tx_reports();
    println!(";; watching tx reports; Ctrl-C returns to the console");
    loop {
        tokio::select! {
            result = reports.recv() => match result {
                Ok(report) => println!("{{:t {} :tx-instant {} :datoms {}}}", report.t, report.tx_instant, report.datoms.len()),
                Err(tokio::sync::broadcast::error::RecvError::Lagged(count)) => eprintln!(";; lagged by {count} reports"),
                Err(tokio::sync::broadcast::error::RecvError::Closed) => return Err("tx-report stream closed".into()),
            },
            _ = tokio::signal::ctrl_c() => return Ok(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use corium_core::{Attribute, Cardinality, Datom, EntityId, Keyword, Schema, Value, ValueType};
    use corium_db::Idents;

    use super::*;

    fn fixture() -> Db {
        let attr = EntityId::from_raw(10);
        let mut schema = Schema::default();
        schema.insert(Attribute {
            id: attr,
            value_type: ValueType::Long,
            cardinality: Cardinality::One,
            unique: None,
            is_component: false,
            indexed: true,
            no_history: false,
        });
        let mut idents = Idents::default();
        idents.insert(Keyword::parse("item/value"), attr);
        let db = Db::new(schema).with_naming(idents, corium_core::KeywordInterner::default());
        let one = Datom {
            e: EntityId::from_raw(1_000),
            a: attr,
            v: Value::Long(1),
            tx: EntityId::from_raw(1),
            added: true,
        };
        let retract = Datom {
            added: false,
            tx: EntityId::from_raw(2),
            ..one.clone()
        };
        let two = Datom {
            v: Value::Long(2),
            tx: EntityId::from_raw(2),
            ..one.clone()
        };
        db.with_transaction(1, &[one])
            .with_transaction(2, &[retract, two])
    }

    #[test]
    fn demo_script_exercises_full_time_model() {
        let db = fixture();
        let mut session = Session::default();
        let query = "[:find ?v :where [?e :item/value ?v]]";
        assert!(output(session.execute(&db, "(pull [:item/value] 1000)")).contains('2'));
        assert!(matches!(
            session.execute(&db, ":basis"),
            Ok(Action::Output(_))
        ));
        assert!(session.execute(&db, ":as-of 1").is_ok());
        assert!(output(session.execute(&db, query)).contains('1'));
        assert!(session.execute(&db, ":since 1").is_ok());
        assert!(output(session.execute(&db, query)).contains('2'));
        assert!(session.execute(&db, ":history on").is_ok());
        let history = output(session.execute(&db, query));
        assert!(history.contains('1') && history.contains('2'));
        assert!(session.execute(&db, ":history off").is_ok());
        assert!(matches!(
            session.execute(&db, ":stats"),
            Ok(Action::Output(_))
        ));
    }

    fn output(action: Result<Action, String>) -> String {
        match action.expect("console action") {
            Action::Output(output) => output,
            Action::Watch | Action::Quit => panic!("expected output"),
        }
    }
}
