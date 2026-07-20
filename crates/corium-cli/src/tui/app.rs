//! Application state and event handling for the TUI dashboard.

use std::collections::VecDeque;
use std::time::{Duration, Instant};

use corium_core::Datom;
use corium_db::Db;
use corium_peer::PeerReport;
use corium_protocol::pb;
use corium_query::ExecOptions;
use corium_query::QInput;
use corium_query::ast::{FindElem, FindSpec, InSpec, parse_query};
use corium_query::edn::{Edn, read_one};
use ratatui::crossterm::event::{
    Event as TermEvent, KeyCode, KeyEvent, KeyEventKind, KeyModifiers,
};
use ratatui::widgets::TableState;

use crate::console;

/// Points kept per metrics sparkline.
const HISTORY_CAP: usize = 240;
/// Transactions kept in the live feed.
const TX_FEED_CAP: usize = 500;
/// Datoms retained per feed entry for the detail pane.
const TX_DETAIL_CAP: usize = 512;
/// Query latencies kept for the dashboard summary.
const LATENCY_CAP: usize = 64;

/// Events consumed by the main loop.
pub enum Event {
    /// Terminal input (keys, resize).
    Term(TermEvent),
    /// A successful Status RPC sample.
    Status(Box<StatusSample>),
    /// A failed Status RPC.
    StatusError(String),
    /// A live transaction report.
    Tx(Box<TxEntry>),
    /// The tx-report subscription skipped this many reports.
    TxLagged(u64),
}

/// One Status RPC observation with the peer-measured round-trip time.
pub struct StatusSample {
    /// When the response arrived.
    pub at: Instant,
    /// Peer-observed round-trip time of the RPC.
    pub rtt: Duration,
    /// The transactor's answer.
    pub status: pb::StatusResponse,
}

/// One transaction in the live feed.
pub struct TxEntry {
    /// Transaction number.
    pub t: u64,
    /// Commit timestamp (Unix milliseconds).
    pub tx_instant: i64,
    /// Total datoms in the transaction.
    pub datom_count: usize,
    /// Leading datoms retained for the detail pane (capped).
    pub datoms: Vec<Datom>,
}

impl TxEntry {
    /// Builds a feed entry from a peer report, capping retained datoms.
    pub fn from_report(report: &PeerReport) -> Self {
        Self {
            t: report.t,
            tx_instant: report.tx_instant,
            datom_count: report.datoms.len(),
            datoms: report.datoms.iter().take(TX_DETAIL_CAP).cloned().collect(),
        }
    }
}

/// Top-level panels.
#[derive(Clone, Copy, Eq, PartialEq)]
pub enum Tab {
    /// Datalog/pull workbench.
    Query,
    /// Store metrics dashboard.
    Metrics,
    /// Live transaction feed.
    Transactions,
    /// Attribute schema browser.
    Schema,
}

impl Tab {
    /// All tabs in display order.
    pub const ALL: [Self; 4] = [Self::Query, Self::Metrics, Self::Transactions, Self::Schema];

    /// Tab title shown in the header bar.
    pub fn title(self) -> &'static str {
        match self {
            Self::Query => "1 Query",
            Self::Metrics => "2 Metrics",
            Self::Transactions => "3 Transactions",
            Self::Schema => "4 Schema",
        }
    }

    fn index(self) -> usize {
        Self::ALL.iter().position(|tab| *tab == self).unwrap_or(0)
    }

    fn next(self) -> Self {
        Self::ALL[(self.index() + 1) % Self::ALL.len()]
    }

    fn previous(self) -> Self {
        Self::ALL[(self.index() + Self::ALL.len() - 1) % Self::ALL.len()]
    }
}

/// Rolling metrics derived from Status RPC samples.
#[derive(Default)]
pub struct MetricsState {
    /// Most recent sample.
    pub last: Option<StatusSample>,
    /// Most recent Status RPC error, cleared by the next success.
    pub error: Option<String>,
    /// Transactions per second between the last two samples.
    pub tx_rate: f64,
    /// Indexed datom growth per second between the last two samples.
    pub datom_rate: f64,
    /// Transactions-per-minute history (sparkline).
    pub tx_rate_history: VecDeque<u64>,
    /// Status round-trip-time history in microseconds (sparkline).
    pub rtt_history: VecDeque<u64>,
    /// Transaction queue-depth history (sparkline).
    pub queue_history: VecDeque<u64>,
    /// Index-lag history (sparkline).
    pub lag_history: VecDeque<u64>,
}

impl MetricsState {
    #[allow(
        clippy::cast_precision_loss,
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss
    )]
    fn record(&mut self, sample: StatusSample) {
        if let Some(previous) = &self.last {
            let elapsed = sample.at.duration_since(previous.at).as_secs_f64();
            if elapsed > 0.0 {
                let tx_delta = sample
                    .status
                    .transaction_count
                    .saturating_sub(previous.status.transaction_count);
                let datom_delta = sample
                    .status
                    .datom_count
                    .saturating_sub(previous.status.datom_count);
                self.tx_rate = tx_delta as f64 / elapsed;
                self.datom_rate = datom_delta as f64 / elapsed;
                push_capped(
                    &mut self.tx_rate_history,
                    (self.tx_rate * 60.0).round() as u64,
                );
            }
        }
        push_capped(
            &mut self.rtt_history,
            u64::try_from(sample.rtt.as_micros()).unwrap_or(u64::MAX),
        );
        push_capped(
            &mut self.queue_history,
            sample.status.transaction_queue_depth,
        );
        push_capped(&mut self.lag_history, sample.status.index_lag);
        self.error = None;
        self.last = Some(sample);
    }
}

fn push_capped(history: &mut VecDeque<u64>, value: u64) {
    if history.len() == HISTORY_CAP {
        history.pop_front();
    }
    history.push_back(value);
}

/// Rendered form of the last query result.
pub enum QueryOutput {
    /// Nothing run yet.
    Empty,
    /// Raw text (pull results, console command output, errors).
    Text(String),
    /// Tabular relation with `:find` headers.
    Table {
        /// Column labels from the `:find` clause.
        headers: Vec<String>,
        /// Stringified result rows.
        rows: Vec<Vec<String>>,
    },
}

/// Query workbench state: the editor, history, and last result.
pub struct QueryState {
    /// Editor contents (may span multiple lines).
    pub input: String,
    /// Cursor byte offset into `input`.
    pub cursor: usize,
    /// Last result.
    pub output: QueryOutput,
    /// One-line result summary or error.
    pub status: String,
    /// Result scroll offset (rows or lines).
    pub scroll: usize,
    /// Recent query wall-clock latencies in milliseconds.
    pub latencies: VecDeque<f64>,
    /// Console session carrying the active time view (`:as-of`, …).
    pub session: console::Session,
    history: Vec<String>,
    history_index: Option<usize>,
    stash: String,
}

impl Default for QueryState {
    fn default() -> Self {
        Self {
            input: String::new(),
            cursor: 0,
            output: QueryOutput::Empty,
            status: "Enter runs a complete form; Alt-Enter inserts a newline. Try :help.".into(),
            scroll: 0,
            latencies: VecDeque::new(),
            session: console::Session::default(),
            history: Vec::new(),
            history_index: None,
            stash: String::new(),
        }
    }
}

/// Live transaction feed state.
#[derive(Default)]
pub struct TxFeed {
    /// Newest-last feed entries (capped).
    pub entries: VecDeque<TxEntry>,
    /// Selected entry index into `entries`.
    pub selected: usize,
    /// Whether selection tracks the newest transaction.
    pub follow: bool,
    /// Reports dropped by the broadcast subscription.
    pub lagged: u64,
    /// Detail pane scroll offset.
    pub detail_scroll: usize,
    /// Feed table scroll state (kept across frames so scrolling is stable).
    pub table_state: TableState,
}

impl TxFeed {
    fn push(&mut self, entry: TxEntry) {
        if self.entries.len() == TX_FEED_CAP {
            self.entries.pop_front();
            self.selected = self.selected.saturating_sub(1);
        }
        self.entries.push_back(entry);
        if self.follow {
            self.selected = self.entries.len() - 1;
            self.detail_scroll = 0;
        }
    }
}

/// Schema browser state.
#[derive(Default)]
pub struct SchemaState {
    /// Case-insensitive substring filter over attribute idents.
    pub filter: String,
    /// Whether keystrokes edit the filter.
    pub editing: bool,
    /// Selected row index into the filtered listing.
    pub selected: usize,
    /// Listing scroll state (kept across frames so scrolling is stable).
    pub table_state: TableState,
}

/// Whole-dashboard state.
pub struct App {
    /// Hosted database name.
    pub db_name: String,
    /// Active panel.
    pub tab: Tab,
    /// Set when the user asked to leave.
    pub should_quit: bool,
    /// Query workbench.
    pub query: QueryState,
    /// Metrics dashboard.
    pub metrics: MetricsState,
    /// Transaction feed.
    pub txs: TxFeed,
    /// Schema browser.
    pub schema: SchemaState,
}

impl App {
    /// Fresh state for a database.
    pub fn new(db_name: String) -> Self {
        Self {
            db_name,
            tab: Tab::Query,
            should_quit: false,
            query: QueryState::default(),
            metrics: MetricsState::default(),
            txs: TxFeed {
                follow: true,
                ..TxFeed::default()
            },
            schema: SchemaState::default(),
        }
    }

    /// Applies one event against the current database value.
    pub fn handle(&mut self, event: Event, db: &Db) {
        match event {
            Event::Term(TermEvent::Key(key)) if key.kind != KeyEventKind::Release => {
                self.handle_key(key, db);
            }
            Event::Term(_) => {}
            Event::Status(sample) => self.metrics.record(*sample),
            Event::StatusError(error) => self.metrics.error = Some(error),
            Event::Tx(entry) => self.txs.push(*entry),
            Event::TxLagged(count) => self.txs.lagged += count,
        }
    }

    fn handle_key(&mut self, key: KeyEvent, db: &Db) {
        if key.modifiers.contains(KeyModifiers::CONTROL) && key.code == KeyCode::Char('c') {
            self.should_quit = true;
            return;
        }
        match key.code {
            KeyCode::Tab if !(self.tab == Tab::Schema && self.schema.editing) => {
                self.tab = self.tab.next();
                return;
            }
            KeyCode::BackTab => {
                self.tab = self.tab.previous();
                return;
            }
            _ => {}
        }
        match self.tab {
            Tab::Query => self.handle_query_key(key, db),
            Tab::Metrics => self.handle_common_key(key),
            Tab::Transactions => self.handle_tx_key(key),
            Tab::Schema => self.handle_schema_key(key),
        }
    }

    /// Keys shared by the non-editor tabs: quit and direct tab selection.
    fn handle_common_key(&mut self, key: KeyEvent) {
        match key.code {
            KeyCode::Char('q') | KeyCode::Esc => self.should_quit = true,
            KeyCode::Char('1') => self.tab = Tab::Query,
            KeyCode::Char('2') => self.tab = Tab::Metrics,
            KeyCode::Char('3') => self.tab = Tab::Transactions,
            KeyCode::Char('4') => self.tab = Tab::Schema,
            _ => {}
        }
    }

    fn handle_tx_key(&mut self, key: KeyEvent) {
        match key.code {
            KeyCode::Up => {
                self.txs.follow = false;
                self.txs.selected = self.txs.selected.saturating_sub(1);
                self.txs.detail_scroll = 0;
            }
            KeyCode::Down => {
                if self.txs.selected + 1 < self.txs.entries.len() {
                    self.txs.selected += 1;
                    self.txs.detail_scroll = 0;
                }
                if self.txs.selected + 1 == self.txs.entries.len() {
                    self.txs.follow = true;
                }
            }
            KeyCode::Char('f') => {
                self.txs.follow = !self.txs.follow;
                if self.txs.follow && !self.txs.entries.is_empty() {
                    self.txs.selected = self.txs.entries.len() - 1;
                }
            }
            KeyCode::PageUp => {
                self.txs.detail_scroll = self.txs.detail_scroll.saturating_sub(10);
            }
            KeyCode::PageDown => self.txs.detail_scroll += 10,
            _ => self.handle_common_key(key),
        }
    }

    fn handle_schema_key(&mut self, key: KeyEvent) {
        if self.schema.editing {
            match key.code {
                KeyCode::Char(character) => self.schema.filter.push(character),
                KeyCode::Backspace => {
                    self.schema.filter.pop();
                }
                KeyCode::Enter => self.schema.editing = false,
                KeyCode::Esc => {
                    self.schema.editing = false;
                    self.schema.filter.clear();
                }
                _ => {}
            }
            self.schema.selected = 0;
            return;
        }
        match key.code {
            KeyCode::Char('/') => self.schema.editing = true,
            KeyCode::Up => self.schema.selected = self.schema.selected.saturating_sub(1),
            KeyCode::Down => self.schema.selected += 1,
            KeyCode::PageUp => self.schema.selected = self.schema.selected.saturating_sub(10),
            KeyCode::PageDown => self.schema.selected += 10,
            KeyCode::Home => self.schema.selected = 0,
            _ => self.handle_common_key(key),
        }
    }

    fn handle_query_key(&mut self, key: KeyEvent, db: &Db) {
        let query = &mut self.query;
        match key.code {
            KeyCode::Enter if key.modifiers.contains(KeyModifiers::ALT) => query.insert('\n'),
            KeyCode::Enter => {
                if edn_complete(&query.input) {
                    if query.run(db) {
                        self.should_quit = true;
                    }
                } else {
                    query.insert('\n');
                }
            }
            KeyCode::Char('u') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                query.input.clear();
                query.cursor = 0;
            }
            KeyCode::Char('l') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                query.output = QueryOutput::Empty;
                query.status.clear();
                query.scroll = 0;
            }
            KeyCode::Char(character) if !key.modifiers.contains(KeyModifiers::CONTROL) => {
                query.insert(character);
            }
            KeyCode::Backspace => query.backspace(),
            KeyCode::Delete => query.delete(),
            KeyCode::Left => query.move_left(),
            KeyCode::Right => query.move_right(),
            KeyCode::Home => query.move_home(),
            KeyCode::End => query.move_end(),
            KeyCode::Up => query.up(),
            KeyCode::Down => query.down(),
            KeyCode::PageUp => query.scroll = query.scroll.saturating_sub(10),
            KeyCode::PageDown => query.scroll += 10,
            _ => {}
        }
    }
}

impl QueryState {
    fn insert(&mut self, character: char) {
        self.input.insert(self.cursor, character);
        self.cursor += character.len_utf8();
    }

    fn backspace(&mut self) {
        if let Some((offset, _)) = self.input[..self.cursor].char_indices().next_back() {
            self.input.remove(offset);
            self.cursor = offset;
        }
    }

    fn delete(&mut self) {
        if self.cursor < self.input.len() {
            self.input.remove(self.cursor);
        }
    }

    fn move_left(&mut self) {
        if let Some((offset, _)) = self.input[..self.cursor].char_indices().next_back() {
            self.cursor = offset;
        }
    }

    fn move_right(&mut self) {
        if let Some(character) = self.input[self.cursor..].chars().next() {
            self.cursor += character.len_utf8();
        }
    }

    fn move_home(&mut self) {
        self.cursor = self.line_start(self.cursor);
    }

    fn move_end(&mut self) {
        self.cursor = self.line_end(self.cursor);
    }

    fn line_start(&self, from: usize) -> usize {
        self.input[..from]
            .rfind('\n')
            .map_or(0, |offset| offset + 1)
    }

    fn line_end(&self, from: usize) -> usize {
        self.input[from..]
            .find('\n')
            .map_or(self.input.len(), |offset| from + offset)
    }

    /// Cursor position as (line, column) in characters, for terminal cursor
    /// placement.
    pub fn cursor_line_col(&self) -> (usize, usize) {
        let line = self.input[..self.cursor].matches('\n').count();
        let column = self.input[self.line_start(self.cursor)..self.cursor]
            .chars()
            .count();
        (line, column)
    }

    fn up(&mut self) {
        let start = self.line_start(self.cursor);
        if start == 0 {
            self.history_previous();
            return;
        }
        let column = self.input[start..self.cursor].chars().count();
        let previous_start = self.line_start(start - 1);
        self.cursor = offset_at_column(&self.input, previous_start, start - 1, column);
    }

    fn down(&mut self) {
        let end = self.line_end(self.cursor);
        if end == self.input.len() {
            self.history_next();
            return;
        }
        let start = self.line_start(self.cursor);
        let column = self.input[start..self.cursor].chars().count();
        let next_start = end + 1;
        let next_end = self.line_end(next_start);
        self.cursor = offset_at_column(&self.input, next_start, next_end, column);
    }

    fn history_previous(&mut self) {
        let index = match self.history_index {
            Some(0) => return,
            Some(index) => index - 1,
            None if self.history.is_empty() => return,
            None => {
                self.stash = self.input.clone();
                self.history.len() - 1
            }
        };
        self.history_index = Some(index);
        self.input = self.history[index].clone();
        self.cursor = self.input.len();
    }

    fn history_next(&mut self) {
        let Some(index) = self.history_index else {
            return;
        };
        if index + 1 < self.history.len() {
            self.history_index = Some(index + 1);
            self.input = self.history[index + 1].clone();
        } else {
            self.history_index = None;
            self.input = self.stash.clone();
        }
        self.cursor = self.input.len();
    }

    /// Runs the editor contents; returns true when the user asked to quit.
    fn run(&mut self, db: &Db) -> bool {
        let line = self.input.trim().to_owned();
        if line.is_empty() {
            return false;
        }
        if self.history.last() != Some(&line) {
            self.history.push(line.clone());
        }
        self.history_index = None;
        self.input.clear();
        self.cursor = 0;
        self.scroll = 0;
        if line.starts_with(':') {
            match self.session.execute(db, &line) {
                Ok(console::Action::Output(text)) => {
                    self.output = QueryOutput::Text(text);
                    self.status = format!("view: {}", self.session.view_label());
                }
                Ok(console::Action::Watch) => {
                    self.output = QueryOutput::Text(
                        ";; live transactions stream on the Transactions tab".into(),
                    );
                    self.status.clear();
                }
                Ok(console::Action::Quit) => return true,
                Err(error) => self.status = format!("error: {error}"),
            }
            return false;
        }
        let view = self.session.apply_view(db);
        let started = Instant::now();
        match run_form(&view, &line) {
            Ok((output, scanned)) => {
                let elapsed = started.elapsed().as_secs_f64() * 1_000.0;
                if self.latencies.len() == LATENCY_CAP {
                    self.latencies.pop_front();
                }
                self.latencies.push_back(elapsed);
                let rows = match &output {
                    QueryOutput::Table { rows, .. } => Some(rows.len()),
                    QueryOutput::Text(_) | QueryOutput::Empty => None,
                };
                self.status = summary_line(rows, elapsed, scanned, view.basis_t());
                self.output = output;
            }
            Err(error) => self.status = format!("error: {error}"),
        }
        false
    }
}

/// Character offset `column` into the line spanning `[start, end)`, clamped
/// to the line end.
fn offset_at_column(text: &str, start: usize, end: usize, column: usize) -> usize {
    text[start..end]
        .char_indices()
        .nth(column)
        .map_or(end, |(offset, _)| start + offset)
}

fn summary_line(
    rows: Option<usize>,
    elapsed_ms: f64,
    scanned: Option<usize>,
    basis_t: u64,
) -> String {
    let mut parts = Vec::new();
    if let Some(rows) = rows {
        parts.push(format!("{rows} rows"));
    }
    parts.push(format!("{elapsed_ms:.3} ms"));
    if let Some(scanned) = scanned {
        parts.push(format!("{scanned} datoms scanned"));
    }
    parts.push(format!("basis-t {basis_t}"));
    parts.join(" · ")
}

/// Executes a query or pull form against a database view. Returns the
/// rendered output and, for Datalog queries, the datoms-scanned count.
fn run_form(view: &Db, line: &str) -> Result<(QueryOutput, Option<usize>), String> {
    let form = read_one(line).map_err(|error| error.to_string())?;
    if let Some(text) = console::execute_pull(view, &form)? {
        return Ok((QueryOutput::Text(text), None));
    }
    let query = parse_query(&form).map_err(|error| error.to_string())?;
    let mut inputs = Vec::with_capacity(query.inputs.len());
    for input in &query.inputs {
        match input {
            InSpec::Db(_) => inputs.push(QInput::Db(view)),
            _ => {
                return Err(
                    "the TUI accepts database inputs only; use corium.api for parameterized queries"
                        .into(),
                );
            }
        }
    }
    let (result, report) = corium_query::run(&query, &inputs, ExecOptions::default())
        .map_err(|error| error.to_string())?;
    Ok((tabulate(&query.find, result), Some(report.datoms_scanned)))
}

/// Renders a query result as a table when its shape allows, falling back to
/// raw EDN text.
fn tabulate(find: &FindSpec, result: Edn) -> QueryOutput {
    let headers: Vec<String> = find.elems().iter().map(|elem| elem_label(elem)).collect();
    match (find, result) {
        (FindSpec::Rel(_), Edn::Set(items) | Edn::Vector(items)) => {
            let rows = items
                .into_iter()
                .map(|row| match row {
                    Edn::Vector(cells) | Edn::List(cells) => {
                        cells.iter().map(ToString::to_string).collect()
                    }
                    other => vec![other.to_string()],
                })
                .collect();
            QueryOutput::Table { headers, rows }
        }
        (FindSpec::Coll(_), Edn::Set(items) | Edn::Vector(items)) => QueryOutput::Table {
            headers,
            rows: items
                .into_iter()
                .map(|item| vec![item.to_string()])
                .collect(),
        },
        (FindSpec::Tuple(_), Edn::Vector(cells)) => QueryOutput::Table {
            headers,
            rows: vec![cells.iter().map(ToString::to_string).collect()],
        },
        (_, other) => QueryOutput::Text(other.to_string()),
    }
}

fn elem_label(elem: &FindElem) -> String {
    match elem {
        FindElem::Var(var) => var.clone(),
        FindElem::Aggregate(aggregate) => match aggregate.n {
            Some(n) => format!("({} {} {})", aggregate.op, n, aggregate.var),
            None => format!("({} {})", aggregate.op, aggregate.var),
        },
        FindElem::Pull(var, _) => format!("(pull {var} …)"),
    }
}

/// True when `text` holds at least one form and all `()[]{}` and strings are
/// balanced, i.e. pressing Enter should run it rather than continue editing.
pub fn edn_complete(text: &str) -> bool {
    let mut depth = 0_i64;
    let mut in_string = false;
    let mut chars = text.chars();
    let mut seen = false;
    while let Some(character) = chars.next() {
        if in_string {
            match character {
                '\\' => {
                    chars.next();
                }
                '"' => in_string = false,
                _ => {}
            }
            continue;
        }
        match character {
            ';' => {
                for skipped in chars.by_ref() {
                    if skipped == '\n' {
                        break;
                    }
                }
            }
            '"' => {
                in_string = true;
                seen = true;
            }
            '\\' => {
                // Character literal: the next char is data.
                chars.next();
                seen = true;
            }
            '(' | '[' | '{' => {
                depth += 1;
                seen = true;
            }
            ')' | ']' | '}' => depth -= 1,
            other => {
                if !other.is_whitespace() {
                    seen = true;
                }
            }
        }
    }
    seen && depth <= 0 && !in_string
}

/// Formats a Unix-millisecond timestamp as UTC `YYYY-MM-DD HH:MM:SS.mmm`.
pub fn format_instant(unix_ms: i64) -> String {
    let seconds = unix_ms.div_euclid(1_000);
    let millis = unix_ms.rem_euclid(1_000);
    let days = seconds.div_euclid(86_400);
    let tod = seconds.rem_euclid(86_400);
    let (year, month, day) = civil_from_days(days);
    format!(
        "{year:04}-{month:02}-{day:02} {:02}:{:02}:{:02}.{millis:03}",
        tod / 3_600,
        (tod % 3_600) / 60,
        tod % 60
    )
}

/// Gregorian calendar date for a day count since 1970-01-01 (Howard Hinnant's
/// `civil_from_days`).
fn civil_from_days(days: i64) -> (i64, i64, i64) {
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = yoe + era * 400 + i64::from(month <= 2);
    (year, month, day)
}

/// Groups digits with commas: `1234567` → `"1,234,567"`.
pub fn group_digits(value: u64) -> String {
    let digits = value.to_string();
    let mut grouped = String::with_capacity(digits.len() + digits.len() / 3);
    for (index, digit) in digits.chars().enumerate() {
        if index > 0 && (digits.len() - index).is_multiple_of(3) {
            grouped.push(',');
        }
        grouped.push(digit);
    }
    grouped
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn balanced_forms_are_complete() {
        assert!(edn_complete("[:find ?e :where [?e :db/ident ?v]]"));
        assert!(edn_complete("(pull [:db/ident] 10)"));
        assert!(edn_complete(":stats"));
        assert!(!edn_complete("[:find ?e"));
        assert!(!edn_complete("[:find \"unclosed]"));
        assert!(!edn_complete("   "));
        assert!(edn_complete("[\"bracket in \\\" string ]\"]"));
        assert!(!edn_complete("[:find ?e ; comment ]\n"));
    }

    #[test]
    fn instants_format_as_utc() {
        assert_eq!(format_instant(0), "1970-01-01 00:00:00.000");
        assert_eq!(format_instant(1_700_000_000_123), "2023-11-14 22:13:20.123");
        assert_eq!(format_instant(-1), "1969-12-31 23:59:59.999");
    }

    #[test]
    fn digits_group_in_threes() {
        assert_eq!(group_digits(0), "0");
        assert_eq!(group_digits(999), "999");
        assert_eq!(group_digits(1_000), "1,000");
        assert_eq!(group_digits(1_234_567), "1,234,567");
    }

    #[test]
    fn editor_navigates_lines_and_history() {
        let mut query = QueryState::default();
        for character in "ab\ncd".chars() {
            query.insert(character);
        }
        assert_eq!(query.cursor_line_col(), (1, 2));
        query.up();
        assert_eq!(query.cursor_line_col(), (0, 2));
        query.down();
        assert_eq!(query.cursor_line_col(), (1, 2));
        query.history.push("[:find ?e]".into());
        query.move_home();
        query.up(); // to the first line
        query.up(); // then into history
        assert_eq!(query.input, "[:find ?e]");
        query.history_next();
        assert_eq!(query.input, "ab\ncd");
    }

    #[test]
    fn rel_results_tabulate_with_find_headers() {
        let find = FindSpec::Rel(vec![FindElem::Var("?e".into()), FindElem::Var("?v".into())]);
        let result = Edn::Set(vec![Edn::Vector(vec![Edn::Long(1), Edn::Long(2)])]);
        match tabulate(&find, result) {
            QueryOutput::Table { headers, rows } => {
                assert_eq!(headers, vec!["?e", "?v"]);
                assert_eq!(rows, vec![vec!["1".to_owned(), "2".to_owned()]]);
            }
            QueryOutput::Text(_) | QueryOutput::Empty => panic!("expected a table"),
        }
    }
}
