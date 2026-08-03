//! Tonic services exposing a [`TransactorNode`] over the network.

use std::pin::Pin;
use std::sync::Arc;

use corium_protocol::authz::{self, Access, Action, Guard, IdentityInterceptor, Principal};
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
        // A desired schema that cannot be planned at all is a bad request in
        // the same sense a malformed schema form is.
        | NodeError::SchemaUpdate(_)
        | NodeError::SchemaForm(_) => Status::invalid_argument(error.to_string()),
        // A plan the database has moved past is retryable after a replan, so
        // it aborts rather than failing outright — as a stale basis does.
        NodeError::BasisMismatch { .. } | NodeError::StalePlan { .. } => {
            Status::aborted(error.to_string())
        }
        NodeError::Deposed(_)
        | NodeError::Standby { .. }
        | NodeError::UnsupportedFormat { .. }
        // A plan whose precondition or authority is missing describes the
        // database's state, not a malformed request: the caller resolves the
        // violation or supplies the flag it was told to.
        | NodeError::BlockedPlan(_)
        // A missing or unresolvable key is an operator misconfiguration, not
        // a transient fault: the caller must fix the deployment, not retry.
        | NodeError::Keys(_)
        | NodeError::KeysFenced { .. } => Status::failed_precondition(error.to_string()),
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
                    tx_instant: corium_db::bootstrap::asserted_instant(record.t, &record.datoms)
                        .unwrap_or(record.tx_instant),
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
pub struct TransactorSvc(pub Arc<TransactorNode>, pub Guard);

/// Authorizes `access` for the request's [`Principal`], mapping the guard's
/// decision onto a gRPC status.
///
/// A decision carrying an attribute [`ViewFilter`](authz::ViewFilter) is
/// rejected: no transactor or catalog surface applies one, so honouring it by
/// returning full data would be an authorization bypass.
///
/// A decision that only restricts *keys* is honoured by doing nothing, which
/// is correct rather than lenient: the transactor holds no class key, so every
/// protected value it accepts, logs, indexes, and hands back is already sealed.
/// There is nothing here for a key policy to withhold.
async fn authorize(guard: &Guard, principal: &Principal, access: Access) -> Result<(), Status> {
    match guard.authorize(principal, &access).await {
        Ok(None) => Ok(()),
        Ok(Some(grant)) if grant.view.is_none() => Ok(()),
        Ok(Some(_)) => Err(Status::unimplemented(
            "view filtering is not enforced on this surface yet",
        )),
        Err(error) => Err(error.to_status()),
    }
}

/// Authorizes a write against `database`, refusing a principal whose view
/// actually hides one of its attributes.
///
/// Holding a view is not the test — one that hides nothing restricts nothing —
/// and asking the same question the peer server and SQL ask is what keeps one
/// principal under one policy from getting three different answers. The schema
/// is resolved only for a principal the policy already allowed *and* handed a
/// view, so a denied caller never causes a database to be opened.
async fn authorize_write(
    node: &TransactorNode,
    guard: &Guard,
    principal: &Principal,
    database: &str,
) -> Result<(), Status> {
    let access = Access::on(Action::Transact, database);
    let grant = match guard.authorize(principal, &access).await {
        Ok(None) => return Ok(()),
        Ok(Some(grant)) => grant,
        Err(error) => return Err(error.to_status()),
    };
    if grant.view.is_none() {
        return Ok(());
    }
    let state = node
        .db_state(database)
        .await
        .map_err(|error| to_status(&error))?;
    let db = state.db();
    if grant.hides_attributes(db.schema(), db.idents()) {
        return Err(Status::unimplemented(
            "view filtering is not enforced on this surface yet",
        ));
    }
    Ok(())
}

#[tonic::async_trait]
impl Transactor for TransactorSvc {
    async fn transact(
        &self,
        request: Request<pb::TransactRequest>,
    ) -> Result<Response<pb::TransactResponse>, Status> {
        let principal = authz::principal(&request);
        let request = request.into_inner();
        check_version(request.protocol_version)?;
        authorize_write(&self.0, &self.1, &principal, &request.db).await?;
        self.0
            .transact_at(&request.db, &request.tx_data, request.expected_basis_t)
            .await
            .map(Response::new)
            .map_err(|error| to_status(&error))
    }

    type SubscribeStream = ItemStream;

    async fn subscribe(
        &self,
        request: Request<pb::SubscribeRequest>,
    ) -> Result<Response<Self::SubscribeStream>, Status> {
        let principal = authz::principal(&request);
        let request = request.into_inner();
        check_version(request.protocol_version)?;
        authorize(
            &self.1,
            &principal,
            Access::on(Action::Subscribe, &request.db),
        )
        .await?;
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
        let principal = authz::principal(&request);
        let request = request.into_inner();
        authorize(
            &self.1,
            &principal,
            Access::on(Action::Inspect, &request.db),
        )
        .await?;
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
        let principal = authz::principal(&request);
        let request = request.into_inner();
        authorize(
            &self.1,
            &principal,
            Access::on(Action::Inspect, &request.db),
        )
        .await?;
        self.0
            .status(&request.db)
            .await
            .map(Response::new)
            .map_err(|error| to_status(&error))
    }
}

fn check_version(version: u32) -> Result<(), Status> {
    if (corium_protocol::MIN_SUPPORTED_PROTOCOL_VERSION..=corium_protocol::PROTOCOL_VERSION)
        .contains(&version)
    {
        Ok(())
    } else {
        Err(Status::failed_precondition(format!(
            "protocol version {version} is not supported; supported range is {}..={}",
            corium_protocol::MIN_SUPPORTED_PROTOCOL_VERSION,
            corium_protocol::PROTOCOL_VERSION
        )))
    }
}

/// Catalog gRPC service over a node.
pub struct CatalogSvc(pub Arc<TransactorNode>, pub Guard);

#[tonic::async_trait]
impl Catalog for CatalogSvc {
    async fn create_database(
        &self,
        request: Request<pb::CreateDatabaseRequest>,
    ) -> Result<Response<pb::CreateDatabaseResponse>, Status> {
        let principal = authz::principal(&request);
        let request = request.into_inner();
        authorize(
            &self.1,
            &principal,
            Access::on(Action::CreateDatabase, &request.db),
        )
        .await?;
        let storage_key = parse_key_id(&request.storage_key)?;
        let created = self
            .0
            .create_db(&request.db, &request.schema, storage_key)
            .await
            .map_err(|error| to_status(&error))?;
        Ok(Response::new(pb::CreateDatabaseResponse { created }))
    }

    async fn delete_database(
        &self,
        request: Request<pb::DeleteDatabaseRequest>,
    ) -> Result<Response<pb::DeleteDatabaseResponse>, Status> {
        let principal = authz::principal(&request);
        let request = request.into_inner();
        authorize(
            &self.1,
            &principal,
            Access::on(Action::DeleteDatabase, &request.db),
        )
        .await?;
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
        let principal = authz::principal(&request);
        let request = request.into_inner();
        authorize(
            &self.1,
            &principal,
            Access::on(Action::ForkDatabase, &request.target),
        )
        .await?;
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
        request: Request<pb::ListDatabasesRequest>,
    ) -> Result<Response<pb::ListDatabasesResponse>, Status> {
        let principal = authz::principal(&request);
        authorize(&self.1, &principal, Access::catalog(Action::ListDatabases)).await?;
        Ok(Response::new(pb::ListDatabasesResponse {
            dbs: self.0.list_dbs(),
        }))
    }

    async fn gc_deleted_databases(
        &self,
        request: Request<pb::GcDeletedDatabasesRequest>,
    ) -> Result<Response<pb::GcDeletedDatabasesResponse>, Status> {
        let principal = authz::principal(&request);
        authorize(&self.1, &principal, Access::catalog(Action::GarbageCollect)).await?;
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
        let principal = authz::principal(&request);
        let request = request.into_inner();
        authorize(
            &self.1,
            &principal,
            Access::on(Action::ManageIndex, &request.db),
        )
        .await?;
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
        let principal = authz::principal(&request);
        let request = request.into_inner();
        authorize(
            &self.1,
            &principal,
            Access::on(Action::ManageIndex, &request.db),
        )
        .await?;
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

    async fn get_storage_info(
        &self,
        request: Request<pb::GetStorageInfoRequest>,
    ) -> Result<Response<pb::GetStorageInfoResponse>, Status> {
        let principal = authz::principal(&request);
        let request = request.into_inner();
        authorize(
            &self.1,
            &principal,
            Access::on(Action::Inspect, &request.db),
        )
        .await?;
        self.0
            .backup_info(&request.db)
            .await
            .map(Response::new)
            .map_err(|error| to_status(&error))
    }

    async fn key_status(
        &self,
        request: Request<pb::KeyStatusRequest>,
    ) -> Result<Response<pb::KeyStatusResponse>, Status> {
        let principal = authz::principal(&request);
        let request = request.into_inner();
        authorize(
            &self.1,
            &principal,
            Access::on(Action::ManageKeys, &request.db),
        )
        .await?;
        let status = self
            .0
            .key_status(&request.db)
            .await
            .map_err(|error| to_status(&error))?;
        let basis_t = status.basis_t;
        let Some(manifest) = status
            .manifest
            .filter(|manifest| !manifest.storage_keys.is_empty())
        else {
            return Ok(Response::new(pb::KeyStatusResponse {
                encrypted: false,
                basis_t,
                ..pb::KeyStatusResponse::default()
            }));
        };
        let storage_keys = manifest
            .storage_keys
            .iter()
            .map(|key| pb::StorageKeyEpoch {
                epoch: key.epoch,
                kek_epoch: key.kek_epoch,
                algorithm: key.algorithm.to_string(),
                state: key.state.to_string(),
                created_at_unix_ms: key.created_at_unix_ms,
                opened_at_t: key.opened_at_t,
                live_objects: key.live_objects,
                records_sealed: manifest
                    .log_records_sealed(key.epoch, basis_t)
                    .unwrap_or_default(),
            })
            .collect();
        Ok(Response::new(pb::KeyStatusResponse {
            encrypted: true,
            kek: manifest.kek.to_string(),
            storage_keys,
            basis_t,
            records_per_epoch_limit: corium_store::LOG_RECORDS_PER_EPOCH_LIMIT,
            rotation_due: manifest.storage_rotation_due(basis_t),
            keys_unavailable: status.keys_unavailable,
            keys_fenced: status.keys_fenced,
        }))
    }

    async fn rotate_storage_key(
        &self,
        request: Request<pb::RotateStorageKeyRequest>,
    ) -> Result<Response<pb::RotateStorageKeyResponse>, Status> {
        let principal = authz::principal(&request);
        let request = request.into_inner();
        authorize(
            &self.1,
            &principal,
            Access::on(Action::ManageKeys, &request.db),
        )
        .await?;
        let epoch = self
            .0
            .rotate_storage_key(&request.db)
            .await
            .map_err(|error| to_status(&error))?;
        Ok(Response::new(pb::RotateStorageKeyResponse { epoch }))
    }

    async fn rewrap_keys(
        &self,
        request: Request<pb::RewrapKeysRequest>,
    ) -> Result<Response<pb::RewrapKeysResponse>, Status> {
        let principal = authz::principal(&request);
        let request = request.into_inner();
        authorize(
            &self.1,
            &principal,
            Access::on(Action::ManageKeys, &request.db),
        )
        .await?;
        let kek = parse_key_id(&request.kek)?
            .ok_or_else(|| Status::invalid_argument("a key-encryption key is required"))?;
        self.0
            .rewrap_keys(&request.db, kek)
            .await
            .map_err(|error| to_status(&error))?;
        Ok(Response::new(pb::RewrapKeysResponse {}))
    }

    async fn alter_schema(
        &self,
        request: Request<pb::AlterSchemaRequest>,
    ) -> Result<Response<pb::AlterSchemaResponse>, Status> {
        let principal = authz::principal(&request);
        let request = request.into_inner();
        // `AlterSchema`, not `Transact`: a writer that may add facts must not
        // be able to broaden the vocabulary it writes them under.
        authorize(
            &self.1,
            &principal,
            Access::on(Action::AlterSchema, &request.db),
        )
        .await?;
        // The requester recorded in the audit trail is the authenticated
        // identity, never a field the caller supplies.
        let requester = format!("{}:{}", principal.provider, principal.subject);
        let response = self
            .0
            .alter_schema(&request, &requester)
            .await
            .map_err(|error| to_status(&error))?;
        Ok(Response::new(response))
    }
}

/// Parses an optional key identity, where empty means "unset".
fn parse_key_id(value: &str) -> Result<Option<corium_crypt::KeyId>, Status> {
    if value.is_empty() {
        return Ok(None);
    }
    corium_crypt::KeyId::new(value)
        .map(Some)
        .map_err(|error| Status::invalid_argument(error.to_string()))
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
    guard: Guard,
    tls: Option<tonic::transport::ServerTlsConfig>,
    shutdown: impl std::future::Future<Output = ()> + Send,
) -> Result<(), tonic::transport::Error> {
    // The interceptor authenticates each request and attaches its `Principal`;
    // the services carry the same guard to authorize the concrete access.
    let interceptor = IdentityInterceptor::new(guard.clone());
    let mut builder = tonic::transport::Server::builder();
    if let Some(tls) = tls {
        builder = builder.tls_config(tls)?;
    }
    builder
        .add_service(TransactorServer::with_interceptor(
            TransactorSvc(Arc::clone(&node), guard.clone()),
            interceptor.clone(),
        ))
        .add_service(CatalogServer::with_interceptor(
            CatalogSvc(node, guard),
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

    #[test]
    fn protocol_versions_allow_server_first_rolling_upgrades() {
        assert!(check_version(corium_protocol::MIN_SUPPORTED_PROTOCOL_VERSION).is_ok());
        assert!(check_version(corium_protocol::PROTOCOL_VERSION).is_ok());
        assert_eq!(
            check_version(corium_protocol::PROTOCOL_VERSION + 1)
                .unwrap_err()
                .code(),
            tonic::Code::FailedPrecondition
        );
    }
}
