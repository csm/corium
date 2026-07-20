//! Rendering for the TUI dashboard.

use std::time::{SystemTime, UNIX_EPOCH};

use corium_db::Db;
use ratatui::Frame;
use ratatui::layout::{Constraint, Layout, Position, Rect};
use ratatui::style::{Color, Modifier, Style, Stylize};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Cell, Paragraph, Row, Sparkline, Table, Tabs, Wrap};

use super::app::{App, QueryOutput, Tab, format_instant, group_digits};

/// Maximum rendered width of one result column.
const MAX_COLUMN_WIDTH: u16 = 40;

/// Draws one frame.
pub fn draw(frame: &mut Frame, app: &mut App, db: &Db) {
    let [header, body, footer] = Layout::vertical([
        Constraint::Length(1),
        Constraint::Min(3),
        Constraint::Length(1),
    ])
    .areas(frame.area());
    draw_header(frame, app, db, header);
    match app.tab {
        Tab::Query => draw_query(frame, app, body),
        Tab::Metrics => draw_metrics(frame, app, body),
        Tab::Transactions => draw_transactions(frame, app, db, body),
        Tab::Schema => draw_schema(frame, app, db, body),
    }
    draw_footer(frame, app, footer);
}

fn draw_header(frame: &mut Frame, app: &App, db: &Db, area: Rect) {
    let summary = format!(" {} · basis-t {} ", app.db_name, group_digits(db.basis_t()));
    let summary_width = u16::try_from(summary.chars().count()).unwrap_or(u16::MAX);
    let [tabs_area, summary_area] =
        Layout::horizontal([Constraint::Min(10), Constraint::Length(summary_width)]).areas(area);
    let tabs = Tabs::new(Tab::ALL.iter().map(|tab| tab.title()))
        .select(Tab::ALL.iter().position(|tab| *tab == app.tab))
        .style(Style::default().fg(Color::DarkGray))
        .highlight_style(
            Style::default()
                .fg(Color::LightCyan)
                .add_modifier(Modifier::BOLD),
        );
    frame.render_widget(tabs, tabs_area);
    frame.render_widget(
        Paragraph::new(summary).style(Style::default().fg(Color::Gray)),
        summary_area,
    );
}

fn draw_footer(frame: &mut Frame, app: &App, area: Rect) {
    let hints = match app.tab {
        Tab::Query => {
            "Enter run · Alt-Enter newline · ↑/↓ lines+history · PgUp/PgDn results · Ctrl-U clear · Ctrl-L clear output · Tab panels · Ctrl-C quit"
        }
        Tab::Metrics => "Tab/1-4 panels · q quit",
        Tab::Transactions => "↑/↓ select · f follow · PgUp/PgDn datoms · Tab/1-4 panels · q quit",
        Tab::Schema => "/ filter · Esc clear filter · ↑/↓ select · Tab/1-4 panels · q quit",
    };
    frame.render_widget(
        Paragraph::new(hints).style(Style::default().fg(Color::DarkGray)),
        area,
    );
}

// ---------------------------------------------------------------------------
// Query tab
// ---------------------------------------------------------------------------

fn draw_query(frame: &mut Frame, app: &mut App, area: Rect) {
    let input_lines = app.query.input.matches('\n').count() + 1;
    let input_height = u16::try_from(input_lines.clamp(1, 8)).unwrap_or(8) + 2;
    let [input_area, status_area, result_area] = Layout::vertical([
        Constraint::Length(input_height),
        Constraint::Length(1),
        Constraint::Min(1),
    ])
    .areas(area);

    let (cursor_line, cursor_column) = app.query.cursor_line_col();
    let visible = usize::from(input_height.saturating_sub(2));
    let input_scroll = cursor_line.saturating_sub(visible.saturating_sub(1));
    let input = Paragraph::new(app.query.input.as_str())
        .scroll((u16::try_from(input_scroll).unwrap_or(0), 0))
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title(format!(
                    " query · view: {} ",
                    app.query.session.view_label()
                ))
                .border_style(Style::default().fg(Color::Cyan)),
        );
    frame.render_widget(input, input_area);
    frame.set_cursor_position(Position::new(
        input_area.x + 1 + u16::try_from(cursor_column).unwrap_or(u16::MAX),
        input_area.y + 1 + u16::try_from(cursor_line - input_scroll).unwrap_or(0),
    ));

    let status_style = if app.query.status.starts_with("error") {
        Style::default().fg(Color::Red)
    } else {
        Style::default().fg(Color::Gray)
    };
    frame.render_widget(
        Paragraph::new(app.query.status.as_str()).style(status_style),
        status_area,
    );

    match &app.query.output {
        QueryOutput::Empty => frame.render_widget(
            Paragraph::new("run a Datalog query, a (pull …) form, or a :command")
                .style(Style::default().fg(Color::DarkGray))
                .block(Block::default().borders(Borders::ALL).title(" results ")),
            result_area,
        ),
        QueryOutput::Text(text) => {
            let lines = text.lines().count();
            app.query.scroll = app.query.scroll.min(lines.saturating_sub(1));
            let paragraph = Paragraph::new(text.as_str())
                .wrap(Wrap { trim: false })
                .scroll((u16::try_from(app.query.scroll).unwrap_or(u16::MAX), 0))
                .block(
                    Block::default()
                        .borders(Borders::ALL)
                        .title(format!(" results · {lines} lines ")),
                );
            frame.render_widget(paragraph, result_area);
        }
        QueryOutput::Table { headers, rows } => {
            app.query.scroll = app.query.scroll.min(rows.len().saturating_sub(1));
            let table = result_table(headers, rows, app.query.scroll, result_area);
            frame.render_widget(table, result_area);
        }
    }
}

/// Builds the visible slice of a result table, windowed by `scroll`.
fn result_table<'a>(
    headers: &'a [String],
    rows: &'a [Vec<String>],
    scroll: usize,
    area: Rect,
) -> Table<'a> {
    let visible = usize::from(area.height.saturating_sub(3));
    let window = &rows[scroll.min(rows.len())..(scroll + visible).min(rows.len())];
    let widths: Vec<Constraint> = headers
        .iter()
        .enumerate()
        .map(|(column, header)| {
            let content = window
                .iter()
                .filter_map(|row| row.get(column))
                .map(|cell| cell.chars().count())
                .max()
                .unwrap_or(0);
            let width = content.max(header.chars().count());
            Constraint::Length(
                u16::try_from(width)
                    .unwrap_or(u16::MAX)
                    .min(MAX_COLUMN_WIDTH),
            )
        })
        .collect();
    let header_row = Row::new(headers.iter().map(|header| {
        Cell::from(header.as_str()).style(
            Style::default()
                .fg(Color::LightCyan)
                .add_modifier(Modifier::BOLD),
        )
    }));
    let body = window
        .iter()
        .map(|row| Row::new(row.iter().map(|cell| Cell::from(cell.as_str()))));
    Table::new(body, widths)
        .header(header_row)
        .block(Block::default().borders(Borders::ALL).title(format!(
            " results · rows {}-{} of {} ",
            scroll + usize::from(!rows.is_empty()),
            (scroll + visible).min(rows.len()),
            rows.len()
        )))
}

// ---------------------------------------------------------------------------
// Metrics tab
// ---------------------------------------------------------------------------

fn draw_metrics(frame: &mut Frame, app: &mut App, area: Rect) {
    let [tiles_area, charts_area, detail_area] = Layout::vertical([
        Constraint::Length(3),
        Constraint::Length(8),
        Constraint::Min(8),
    ])
    .areas(area);

    let Some(sample) = &app.metrics.last else {
        let message = app.metrics.error.as_ref().map_or_else(
            || "waiting for the first status sample…".to_owned(),
            |error| format!("status unavailable: {error}"),
        );
        frame.render_widget(
            Paragraph::new(message)
                .style(Style::default().fg(Color::Yellow))
                .block(Block::default().borders(Borders::ALL).title(" metrics ")),
            area,
        );
        return;
    };
    let status = &sample.status;

    let tiles: [(&str, String); 6] = [
        ("basis-t", group_digits(status.basis_t)),
        (
            "index-t (lag)",
            format!(
                "{} ({})",
                group_digits(status.index_basis_t),
                status.index_lag
            ),
        ),
        ("datoms", group_digits(status.datom_count)),
        ("entities", group_digits(status.entity_count)),
        ("attributes", group_digits(status.attribute_count)),
        ("tx queue", group_digits(status.transaction_queue_depth)),
    ];
    let tile_areas = Layout::horizontal([Constraint::Ratio(1, 6); 6]).split(tiles_area);
    for ((title, value), tile_area) in tiles.iter().zip(tile_areas.iter()) {
        frame.render_widget(
            Paragraph::new(value.as_str())
                .style(Style::default().add_modifier(Modifier::BOLD))
                .block(
                    Block::default()
                        .borders(Borders::ALL)
                        .title(format!(" {title} "))
                        .border_style(Style::default().fg(Color::DarkGray)),
                ),
            *tile_area,
        );
    }

    let chart_areas = Layout::horizontal([Constraint::Ratio(1, 3); 3]).split(charts_area);
    sparkline(
        frame,
        chart_areas[0],
        &format!(" transactions/min · now {:.2}/s ", app.metrics.tx_rate),
        &app.metrics.tx_rate_history,
        Color::LightGreen,
    );
    sparkline(
        frame,
        chart_areas[1],
        &format!(
            " status rtt · now {:.2} ms ",
            sample.rtt.as_secs_f64() * 1_000.0
        ),
        &app.metrics.rtt_history,
        Color::LightBlue,
    );
    sparkline(
        frame,
        chart_areas[2],
        &format!(" index lag · now {} ", status.index_lag),
        &app.metrics.lag_history,
        Color::LightMagenta,
    );

    let [transactor_area, peer_area] =
        Layout::horizontal([Constraint::Ratio(1, 2); 2]).areas(detail_area);
    frame.render_widget(
        info_list(" transactor ", &transactor_lines(status)),
        transactor_area,
    );
    frame.render_widget(info_list(" peer ", &peer_lines(app)), peer_area);
}

fn sparkline(
    frame: &mut Frame,
    area: Rect,
    title: &str,
    history: &std::collections::VecDeque<u64>,
    color: Color,
) {
    let width = usize::from(area.width.saturating_sub(2));
    let values: Vec<u64> = history.iter().rev().take(width).rev().copied().collect();
    let widget = Sparkline::default()
        .data(&values)
        .style(Style::default().fg(color))
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title(title.to_owned())
                .border_style(Style::default().fg(Color::DarkGray)),
        );
    frame.render_widget(widget, area);
}

fn info_list<'a>(title: &'a str, lines: &[(String, String)]) -> Paragraph<'a> {
    let label_width = lines
        .iter()
        .map(|(label, _)| label.chars().count())
        .max()
        .unwrap_or(0);
    let text: Vec<Line<'a>> = lines
        .iter()
        .map(|(label, value)| {
            Line::from(vec![
                Span::styled(
                    format!("{label:<label_width$}  "),
                    Style::default().fg(Color::Gray),
                ),
                Span::raw(value.clone()),
            ])
        })
        .collect();
    Paragraph::new(text).block(
        Block::default()
            .borders(Borders::ALL)
            .title(title)
            .border_style(Style::default().fg(Color::DarkGray)),
    )
}

#[allow(clippy::cast_precision_loss)]
fn transactor_lines(status: &corium_protocol::pb::StatusResponse) -> Vec<(String, String)> {
    let failure_rate = if status.transaction_count == 0 {
        0.0
    } else {
        status.transaction_failure_count as f64 * 100.0 / status.transaction_count as f64
    };
    let lease = if status.lease_owner.is_empty() {
        "-".to_owned()
    } else {
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|elapsed| i64::try_from(elapsed.as_millis()).unwrap_or(i64::MAX))
            .unwrap_or(0);
        let remaining = status.lease_expires_unix_ms.saturating_sub(now_ms);
        format!(
            "{} (v{}, expires in {:.1}s)",
            status.lease_owner,
            status.lease_version,
            remaining as f64 / 1_000.0
        )
    };
    vec![
        (
            "transactions".into(),
            group_digits(status.transaction_count),
        ),
        (
            "failures".into(),
            format!(
                "{} ({failure_rate:.2}%)",
                group_digits(status.transaction_failure_count)
            ),
        ),
        (
            "queue depth".into(),
            group_digits(status.transaction_queue_depth),
        ),
        ("index runs".into(), group_digits(status.indexing_runs)),
        ("gc runs".into(), group_digits(status.gc_runs)),
        ("gc swept blobs".into(), group_digits(status.gc_swept_blobs)),
        ("lease".into(), lease),
    ]
}

fn peer_lines(app: &App) -> Vec<(String, String)> {
    let latencies = &app.query.latencies;
    let query_summary = if latencies.is_empty() {
        "none yet".to_owned()
    } else {
        let last = latencies.back().copied().unwrap_or(0.0);
        let sum: f64 = latencies.iter().sum();
        #[allow(clippy::cast_precision_loss)]
        let average = sum / latencies.len() as f64;
        let max = latencies.iter().copied().fold(0.0_f64, f64::max);
        format!(
            "{} run · last {last:.2} ms · avg {average:.2} ms · max {max:.2} ms",
            latencies.len()
        )
    };
    let mut lines = vec![
        ("database".into(), app.db_name.clone()),
        (
            "datom growth".into(),
            format!("{:.1}/s (indexed)", app.metrics.datom_rate),
        ),
        ("queries".into(), query_summary),
        (
            "tx feed".into(),
            format!(
                "{} received · {} lagged",
                app.txs.entries.len(),
                app.txs.lagged
            ),
        ),
    ];
    if let Some(error) = &app.metrics.error {
        lines.push(("status error".into(), error.clone()));
    }
    lines
}

// ---------------------------------------------------------------------------
// Transactions tab
// ---------------------------------------------------------------------------

fn draw_transactions(frame: &mut Frame, app: &mut App, db: &Db, area: Rect) {
    let [feed_area, detail_area] =
        Layout::horizontal([Constraint::Percentage(45), Constraint::Percentage(55)]).areas(area);

    if !app.txs.entries.is_empty() {
        app.txs.selected = app.txs.selected.min(app.txs.entries.len() - 1);
    }
    let rows = app.txs.entries.iter().map(|entry| {
        Row::new(vec![
            Cell::from(entry.t.to_string()),
            Cell::from(format_instant(entry.tx_instant)),
            Cell::from(group_digits(
                u64::try_from(entry.datom_count).unwrap_or(u64::MAX),
            )),
        ])
    });
    let title = format!(
        " transactions · {} · {:.2}/s{} ",
        app.txs.entries.len(),
        app.metrics.tx_rate,
        if app.txs.follow { " · following" } else { "" }
    );
    let table = Table::new(
        rows,
        [
            Constraint::Length(10),
            Constraint::Length(23),
            Constraint::Length(8),
        ],
    )
    .header(
        Row::new(vec!["t", "committed (UTC)", "datoms"]).style(
            Style::default()
                .fg(Color::LightCyan)
                .add_modifier(Modifier::BOLD),
        ),
    )
    .row_highlight_style(Style::default().add_modifier(Modifier::REVERSED))
    .block(Block::default().borders(Borders::ALL).title(title));
    app.txs.table_state.select(if app.txs.entries.is_empty() {
        None
    } else {
        Some(app.txs.selected)
    });
    frame.render_stateful_widget(table, feed_area, &mut app.txs.table_state);

    let detail = app.txs.entries.get(app.txs.selected).map_or_else(
        || vec![Line::from("waiting for transactions…").fg(Color::DarkGray)],
        |entry| {
            let mut lines = vec![
                Line::from(format!(
                    "t {} · {} · {} datoms",
                    entry.t,
                    format_instant(entry.tx_instant),
                    entry.datom_count
                ))
                .add_modifier(Modifier::BOLD),
                Line::from(""),
            ];
            for datom in &entry.datoms {
                lines.push(datom_line(db, datom));
            }
            if entry.datoms.len() < entry.datom_count {
                lines.push(
                    Line::from(format!(
                        "… {} more datoms not retained",
                        entry.datom_count - entry.datoms.len()
                    ))
                    .fg(Color::DarkGray),
                );
            }
            lines
        },
    );
    app.txs.detail_scroll = app.txs.detail_scroll.min(detail.len().saturating_sub(1));
    frame.render_widget(
        Paragraph::new(detail)
            .scroll((u16::try_from(app.txs.detail_scroll).unwrap_or(u16::MAX), 0))
            .block(Block::default().borders(Borders::ALL).title(" datoms ")),
        detail_area,
    );
}

fn datom_line<'a>(db: &Db, datom: &corium_core::Datom) -> Line<'a> {
    let (sign, color) = if datom.added {
        ("+", Color::LightGreen)
    } else {
        ("-", Color::LightRed)
    };
    let attribute = db.idents().ident(datom.a).map_or_else(
        || datom.a.raw().to_string(),
        std::string::ToString::to_string,
    );
    let value = corium_query::boundary::value_to_edn(db, &datom.v);
    Line::from(vec![
        Span::styled(sign.to_owned(), Style::default().fg(color)),
        Span::raw(format!(" {} ", datom.e.raw())),
        Span::styled(attribute, Style::default().fg(Color::LightCyan)),
        Span::raw(format!(" {value}")),
    ])
}

// ---------------------------------------------------------------------------
// Schema tab
// ---------------------------------------------------------------------------

fn draw_schema(frame: &mut Frame, app: &mut App, db: &Db, area: Rect) {
    let filter = app.schema.filter.to_lowercase();
    let mut rows: Vec<[String; 7]> = db
        .schema()
        .iter()
        .map(|(id, attribute)| {
            let ident = db
                .idents()
                .ident(*id)
                .map_or_else(|| id.raw().to_string(), std::string::ToString::to_string);
            [
                ident,
                format!(":{:?}", attribute.value_type).to_lowercase(),
                format!(":{:?}", attribute.cardinality).to_lowercase(),
                attribute.unique.map_or_else(
                    || "-".to_owned(),
                    |unique| format!("{unique:?}").to_lowercase(),
                ),
                yes_no(attribute.indexed),
                yes_no(attribute.is_component),
                yes_no(attribute.no_history),
            ]
        })
        .filter(|row| filter.is_empty() || row[0].to_lowercase().contains(&filter))
        .collect();
    rows.sort();
    if !rows.is_empty() {
        app.schema.selected = app.schema.selected.min(rows.len() - 1);
    }

    let filter_line = if app.schema.editing {
        format!(" attributes · filter: {}▏ ", app.schema.filter)
    } else if app.schema.filter.is_empty() {
        format!(" attributes · {} ", rows.len())
    } else {
        format!(
            " attributes · {} · filter: {} ",
            rows.len(),
            app.schema.filter
        )
    };
    let header = Row::new(vec![
        "ident",
        "value-type",
        "cardinality",
        "unique",
        "indexed",
        "component",
        "no-history",
    ])
    .style(
        Style::default()
            .fg(Color::LightCyan)
            .add_modifier(Modifier::BOLD),
    );
    let ident_width = rows
        .iter()
        .map(|row| row[0].chars().count())
        .max()
        .unwrap_or(5)
        .max(5);
    let table = Table::new(
        rows.iter()
            .map(|row| Row::new(row.iter().map(|cell| Cell::from(cell.as_str())))),
        [
            Constraint::Length(u16::try_from(ident_width).unwrap_or(u16::MAX).min(48)),
            Constraint::Length(10),
            Constraint::Length(11),
            Constraint::Length(10),
            Constraint::Length(7),
            Constraint::Length(9),
            Constraint::Length(10),
        ],
    )
    .header(header)
    .row_highlight_style(Style::default().add_modifier(Modifier::REVERSED))
    .block(Block::default().borders(Borders::ALL).title(filter_line));
    app.schema.table_state.select(if rows.is_empty() {
        None
    } else {
        Some(app.schema.selected)
    });
    frame.render_stateful_widget(table, area, &mut app.schema.table_state);
}

fn yes_no(flag: bool) -> String {
    if flag {
        "yes".to_owned()
    } else {
        "-".to_owned()
    }
}
