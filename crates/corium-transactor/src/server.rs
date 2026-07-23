//! Tonic services exposing a [`TransactorNode`] over the network.

use std::pin::Pin;
use std::sync::Arc;

use corium_protocol::auth::{AuthInterceptor, Authenticator};
use corium_protocol::codec;
use corium_protocol::pb;
use corium_protocol::pb::catalog_server::{Catalog, CatalogServer};
use corium_protocol::pb::transactor_server::{Transactor, TransactorServer};
use tokio_stream::Stream;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

use crate::node::{DbState, IndexPolicyUpdate, NodeError, TransactorNode};

/// Maps node errors onto gRPC statuses.
#[must_use]
pub fn to_status(error: &NodeError) -> Status {
    match error {
        NodeError::UnknownDb(name) => Status::not_found(format!("unknown database {name:?}")),
        NodeError::InvalidName(_)
        | NodeError::BadRequest(_)
        | NodeError::Codec(_)
        | NodeError::TxForm(_)
        | NodeError::SchemaForm(_) => Status::invalid_argument(error.to_string()),
        NodeError::Deposed(_) | NodeError::Standby { .. } | NodeError::UnsupportedFormat { .. } => {
            Status::failed_precondition(error.to_string())
        }
        NodeError::Transact(inner) => match inner {
            crate::TransactError::Tx(_) => Status::invalid_argument(inner.to_string()),
            crate::TransactError::Deposed { .. } => Status::failed_precondition(inner.to_string()),
            _ => Status::internal(inner.to_string()),
        },
        NodeError::Store(_)
        | NodeError::Log(_)
        | NodeError::Lease(_)
        | NodeError::GroupCommit(_) => Status::internal(error.to_string()),
    }
}

type ItemStream = Pin<Box<dyn Stream<Item = Result<pb::SubscribeItem, Status>> + Send>>;

/// Streams a subscription: handshake, gapless log backfill, then live items.
///
/// The broadcast receiver is registered before the basis snapshot is taken,
/// and live reports at or below the last backfilled `t` are dropped, so no
/// transaction can fall between backfill and the live stream.
pub(crate) fn subscription_stream(
    state: &Arc<DbState>,
    from_basis_t: u64,
    heartbeat_interval_ms: u64,
) -> ItemStream {
    let mut live = state.stream_items();
    let (schema, interner) = state.handshake_snapshot();
    let basis = state.db().basis_t();
    let index_basis = state.index_basis();
    let (tx, rx) = tokio::sync::mpsc::channel::<Result<pb::SubscribeItem, Status>>(64);
    let state = Arc::clone(state);
    tokio::spawn(async move {
        let send = |item: pb::subscribe_item::Item| {
            let tx = tx.clone();
            async move {
                tx.send(Ok(pb::SubscribeItem { item: Some(item) }))
                    .await
                    .is_ok()
            }
        };
        if !send(pb::subscribe_item::Item::Handshake(pb::Handshake {
            basis_t: basis,
            index_basis_t: index_basis,
            schema,
            heartbeat_interval_ms,
        }))
        .await
        {
            return;
        }
        let mut last_sent = from_basis_t;
        if from_basis_t < basis {
            let records = match state.tx_range(from_basis_t + 1, Some(basis + 1)).await {
                Ok(records) => records,
                Err(error) => {
                    let _ = tx.send(Err(to_status(&error))).await;
                    return;
                }
            };
            for record in records {
                let datoms = match codec::encode_datoms(&record.datoms, &interner) {
                    Ok(datoms) => datoms,
                    Err(error) => {
                        let _ = tx.send(Err(Status::internal(error.to_string()))).await;
                        return;
                    }
                };
                if !send(pb::subscribe_item::Item::Report(pb::TxReport {
                    t: record.t,
                    tx_instant: record.tx_instant,
                    datoms,
                }))
                .await
                {
                    return;
                }
                last_sent = record.t;
            }
        }
        loop {
            match live.recv().await {
                Ok(pb::subscribe_item::Item::Report(report)) => {
                    if report.t <= last_sent {
                        continue;
                    }
                    last_sent = report.t;
                    if !send(pb::subscribe_item::Item::Report(report)).await {
                        return;
                    }
                }
                Ok(item) => {
                    if !send(item).await {
                        return;
                    }
                }
                Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                    // The subscriber fell behind the broadcast buffer; end the
                    // stream so it reconnects and backfills from its basis.
                    let _ = tx
                        .send(Err(Status::data_loss("subscription lagged; resubscribe")))
                        .await;
                    return;
                }
                Err(tokio::sync::broadcast::error::RecvError::Closed) => return,
            }
        }
    });
    Box::pin(ReceiverStream::new(rx))
}

/// Transactor gRPC service over a node.
pub struct TransactorSvc(pub Arc<TransactorNode>);

#[tonic::async_trait]
impl Transactor for TransactorSvc {
    async fn transact(
        &self,
        request: Request<pb::TransactRequest>,
    ) -> Result<Response<pb::TransactResponse>, Status> {
        let request = request.into_inner();
        check_version(request.protocol_version)?;
        self.0
            .transact(&request.db, &request.tx_data)
            .await
            .map(Response::new)
            .map_err(|error| to_status(&error))
    }

    type SubscribeStream = ItemStream;

    async fn subscribe(
        &self,
        request: Request<pb::SubscribeRequest>,
    ) -> Result<Response<Self::SubscribeStream>, Status> {
        let request = request.into_inner();
        check_version(request.protocol_version)?;
        let state = self
            .0
            .db_state(&request.db)
            .await
            .map_err(|error| to_status(&error))?;
        let heartbeat_interval_ms =
            u64::try_from(self.0.config().heartbeat_interval.as_millis()).unwrap_or(0);
        Ok(Response::new(subscription_stream(
            &state,
            request.from_basis_t,
            heartbeat_interval_ms,
        )))
    }

    async fn sync(
        &self,
        request: Request<pb::SyncRequest>,
    ) -> Result<Response<pb::SyncResponse>, Status> {
        let request = request.into_inner();
        let basis_t = self
            .0
            .sync(&request.db, request.t)
            .await
            .map_err(|error| to_status(&error))?;
        Ok(Response::new(pb::SyncResponse { basis_t }))
    }

    async fn status(
        &self,
        request: Request<pb::StatusRequest>,
    ) -> Result<Response<pb::StatusResponse>, Status> {
        let request = request.into_inner();
        self.0
            .status(&request.db)
            .await
            .map(Response::new)
            .map_err(|error| to_status(&error))
    }
}

fn check_version(version: u32) -> Result<(), Status> {
    if version == corium_protocol::PROTOCOL_VERSION {
        Ok(())
    } else {
        Err(Status::failed_precondition(format!(
            "protocol version {version} is not supported; upgrade to {}",
            corium_protocol::PROTOCOL_VERSION
        )))
    }
}

/// Catalog gRPC service over a node.
pub struct CatalogSvc(pub Arc<TransactorNode>);

#[tonic::async_trait]
impl Catalog for CatalogSvc {
    async fn create_database(
        &self,
        request: Request<pb::CreateDatabaseRequest>,
    ) -> Result<Response<pb::CreateDatabaseResponse>, Status> {
        let request = request.into_inner();
        let created = self
            .0
            .create_db(&request.db, &request.schema)
            .await
            .map_err(|error| to_status(&error))?;
        Ok(Response::new(pb::CreateDatabaseResponse { created }))
    }

    async fn delete_database(
        &self,
        request: Request<pb::DeleteDatabaseRequest>,
    ) -> Result<Response<pb::DeleteDatabaseResponse>, Status> {
        let request = request.into_inner();
        let deleted = self
            .0
            .delete_db(&request.db)
            .await
            .map_err(|error| to_status(&error))?;
        Ok(Response::new(pb::DeleteDatabaseResponse { deleted }))
    }

    async fn fork_database(
        &self,
        request: Request<pb::ForkDatabaseRequest>,
    ) -> Result<Response<pb::ForkDatabaseResponse>, Status> {
        let request = request.into_inner();
        let forked = self
            .0
            .fork_db(&request.db, &request.target, request.as_of_t)
            .await
            .map_err(|error| to_status(&error))?;
        Ok(Response::new(pb::ForkDatabaseResponse {
            created: forked.is_some(),
            basis_t: forked.unwrap_or(0),
        }))
    }

    async fn list_databases(
        &self,
        _request: Request<pb::ListDatabasesRequest>,
    ) -> Result<Response<pb::ListDatabasesResponse>, Status> {
        Ok(Response::new(pb::ListDatabasesResponse {
            dbs: self.0.list_dbs(),
        }))
    }

    async fn gc_deleted_databases(
        &self,
        request: Request<pb::GcDeletedDatabasesRequest>,
    ) -> Result<Response<pb::GcDeletedDatabasesResponse>, Status> {
        let swept = match requested_gc_retention(request.into_inner()) {
            None => self.0.gc_deleted().await,
            Some(retention) => self.0.gc_deleted_with_retention(retention).await,
        };
        let swept_blobs = swept.map_err(|error| to_status(&error))?;
        Ok(Response::new(pb::GcDeletedDatabasesResponse {
            swept_blobs,
        }))
    }

    async fn request_index(
        &self,
        request: Request<pb::RequestIndexRequest>,
    ) -> Result<Response<pb::RequestIndexResponse>, Status> {
        let request = request.into_inner();
        let index_basis_t = self
            .0
            .request_index(&request.db)
            .await
            .map_err(|error| to_status(&error))?;
        Ok(Response::new(pb::RequestIndexResponse { index_basis_t }))
    }

    async fn set_index_policy(
        &self,
        request: Request<pb::SetIndexPolicyRequest>,
    ) -> Result<Response<pb::SetIndexPolicyResponse>, Status> {
        let request = request.into_inner();
        let update = IndexPolicyUpdate {
            interval: request.interval_ms.map(std::time::Duration::from_millis),
            backoff: request.backoff,
            tail_threshold: request.tail_threshold,
            tail_deadline: request
                .tail_deadline_ms
                .map(std::time::Duration::from_millis),
        };
        let policy = self
            .0
            .set_index_policy(&request.db, update)
            .await
            .map_err(|error| to_status(&error))?;
        let millis =
            |duration: std::time::Duration| u64::try_from(duration.as_millis()).unwrap_or(u64::MAX);
        Ok(Response::new(pb::SetIndexPolicyResponse {
            interval_ms: millis(policy.interval),
            backoff: policy.backoff,
            tail_threshold: policy.tail_threshold,
            tail_deadline_ms: millis(policy.tail_deadline),
        }))
    }

    async fn get_backup_info(
        &self,
        request: Request<pb::GetBackupInfoRequest>,
    ) -> Result<Response<pb::GetBackupInfoResponse>, Status> {
        self.0
            .backup_info(&request.into_inner().db)
            .await
            .map(Response::new)
            .map_err(|error| to_status(&error))
    }
}

fn requested_gc_retention(request: pb::GcDeletedDatabasesRequest) -> Option<std::time::Duration> {
    request
        .retention_millis
        .map(std::time::Duration::from_millis)
}

/// Serves the transactor and catalog services until `shutdown` resolves.
///
/// # Errors
/// Returns an error when the listener cannot be bound or TLS is invalid.
pub async fn serve(
    node: Arc<TransactorNode>,
    addr: std::net::SocketAddr,
    authenticator: Arc<dyn Authenticator>,
    tls: Option<tonic::transport::ServerTlsConfig>,
    shutdown: impl std::future::Future<Output = ()> + Send,
) -> Result<(), tonic::transport::Error> {
    let interceptor = AuthInterceptor::new(authenticator);
    let mut builder = tonic::transport::Server::builder();
    if let Some(tls) = tls {
        builder = builder.tls_config(tls)?;
    }
    builder
        .add_service(TransactorServer::with_interceptor(
            TransactorSvc(Arc::clone(&node)),
            interceptor.clone(),
        ))
        .add_service(CatalogServer::with_interceptor(
            CatalogSvc(node),
            interceptor,
        ))
        .serve_with_shutdown(addr, shutdown)
        .await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn gc_retention_distinguishes_default_zero_and_subsecond() {
        let default = pb::GcDeletedDatabasesRequest {
            retention_millis: None,
        };
        let immediate = pb::GcDeletedDatabasesRequest {
            retention_millis: Some(0),
        };
        let subsecond = pb::GcDeletedDatabasesRequest {
            retention_millis: Some(500),
        };

        assert_eq!(requested_gc_retention(default), None);
        assert_eq!(
            requested_gc_retention(immediate),
            Some(std::time::Duration::ZERO)
        );
        assert_eq!(
            requested_gc_retention(subsecond),
            Some(std::time::Duration::from_millis(500))
        );
    }
}
