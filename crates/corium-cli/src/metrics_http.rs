//! Minimal Prometheus HTTP exposition used by process commands.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

/// Binds a metrics listener and serves snapshots in a background task.
pub async fn spawn(
    address: SocketAddr,
    render: Arc<dyn Fn() -> String + Send + Sync>,
) -> Result<tokio::task::JoinHandle<()>, String> {
    let listener = TcpListener::bind(address)
        .await
        .map_err(|error| format!("cannot bind metrics endpoint {address}: {error}"))?;
    Ok(tokio::spawn(async move {
        loop {
            let Ok((mut socket, _)) = listener.accept().await else {
                continue;
            };
            let render = Arc::clone(&render);
            tokio::spawn(async move {
                let mut request = [0_u8; 2048];
                let Ok(Ok(read)) =
                    tokio::time::timeout(Duration::from_secs(5), socket.read(&mut request)).await
                else {
                    return;
                };
                let request = String::from_utf8_lossy(&request[..read]);
                let metrics = request.starts_with("GET /metrics ");
                let (status, content_type, body) = if metrics {
                    ("200 OK", "text/plain; version=0.0.4", render())
                } else {
                    ("404 Not Found", "text/plain", "not found\n".to_owned())
                };
                let response = format!(
                    "HTTP/1.1 {status}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                let _ = socket.write_all(response.as_bytes()).await;
            });
        }
    }))
}
