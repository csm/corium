//! Full-screen terminal dashboard (`corium tui`) in the spirit of Datomic's
//! web console: an interactive query workbench, live data-store metrics
//! (transaction rates, latencies, index lag), a streaming transaction feed,
//! and a schema browser — all over a single peer connection.

mod app;
mod ui;

use std::sync::Arc;
use std::time::{Duration, Instant};

use corium_peer::Connection;
use tokio::sync::mpsc;

use app::{App, Event, StatusSample, TxEntry};

/// Runs the dashboard until the user quits (`Ctrl-C`, `q`, or `:quit`).
pub async fn run(connection: Arc<Connection>, refresh: Duration) -> Result<(), String> {
    let (sender, mut events) = mpsc::channel::<Event>(256);
    spawn_input_reader(sender.clone());
    spawn_status_poller(Arc::clone(&connection), refresh, sender.clone());
    spawn_tx_listener(&connection, sender);
    let mut terminal = ratatui::init();
    let result = event_loop(&mut terminal, &connection, &mut events).await;
    ratatui::restore();
    result
}

async fn event_loop(
    terminal: &mut ratatui::DefaultTerminal,
    connection: &Connection,
    events: &mut mpsc::Receiver<Event>,
) -> Result<(), String> {
    let mut app = App::new(connection.db_name().to_owned());
    loop {
        let db = connection.db();
        terminal
            .draw(|frame| ui::draw(frame, &mut app, &db))
            .map_err(|error| format!("cannot draw terminal: {error}"))?;
        let Some(event) = events.recv().await else {
            return Ok(());
        };
        app.handle(event, &db);
        // Coalesce whatever else is already queued before redrawing.
        while let Ok(event) = events.try_recv() {
            app.handle(event, &connection.db());
        }
        if app.should_quit {
            return Ok(());
        }
    }
}

/// Reads crossterm events on a dedicated thread (the read is blocking) and
/// forwards them to the async event loop.
fn spawn_input_reader(sender: mpsc::Sender<Event>) {
    std::thread::spawn(move || {
        loop {
            match ratatui::crossterm::event::read() {
                Ok(event) => {
                    if sender.blocking_send(Event::Term(event)).is_err() {
                        return;
                    }
                }
                Err(_) => return,
            }
        }
    });
}

/// Polls the transactor Status RPC on a fixed interval, measuring round-trip
/// time as a peer-observed latency signal.
fn spawn_status_poller(
    connection: Arc<Connection>,
    refresh: Duration,
    sender: mpsc::Sender<Event>,
) {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(refresh);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            ticker.tick().await;
            let started = Instant::now();
            let event = match connection.status().await {
                Ok(status) => Event::Status(Box::new(StatusSample {
                    at: Instant::now(),
                    rtt: started.elapsed(),
                    status,
                })),
                Err(error) => Event::StatusError(error.to_string()),
            };
            if sender.send(event).await.is_err() {
                return;
            }
        }
    });
}

/// Forwards live transaction reports from the peer subscription.
fn spawn_tx_listener(connection: &Connection, sender: mpsc::Sender<Event>) {
    let mut reports = connection.tx_reports();
    tokio::spawn(async move {
        loop {
            let event = match reports.recv().await {
                Ok(report) => Event::Tx(Box::new(TxEntry::from_report(&report))),
                Err(tokio::sync::broadcast::error::RecvError::Lagged(count)) => {
                    Event::TxLagged(count)
                }
                Err(tokio::sync::broadcast::error::RecvError::Closed) => return,
            };
            if sender.send(event).await.is_err() {
                return;
            }
        }
    });
}
