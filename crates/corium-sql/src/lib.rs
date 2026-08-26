//! SQL execution and autocommit mutation planning over Corium database values.
//!
//! A [`SqlSession`] captures one [`corium_db::Db`] time view. Current,
//! as-of, and since views expose one wide table per attribute namespace;
//! history views expose normalized event relations only.

mod catalog;
mod mutation;
mod value;

use arrow::record_batch::RecordBatch;
use corium_db::read::ReadContext;
use corium_db::{Db, DbView};
use datafusion::execution::context::SQLOptions;
use datafusion::physical_plan::SendableRecordBatchStream;
use datafusion::prelude::SessionContext;
use futures::StreamExt as _;
use thiserror::Error;

pub use mutation::{MutationKind, SqlMutation, SqlMutationResult};
pub use value::{SqlColumn, SqlRow, SqlType, SqlValue};

/// SQL planning, catalog, or execution failure.
#[derive(Debug, Error)]
pub enum SqlError {
    /// `DataFusion` rejected or failed the query.
    #[error(transparent)]
    DataFusion(#[from] datafusion::error::DataFusionError),
    /// Arrow rejected a generated batch.
    #[error(transparent)]
    Arrow(#[from] arrow::error::ArrowError),
    /// A Corium schema cannot be represented by the SQL projection.
    #[error("SQL schema error: {0}")]
    Schema(String),
    /// SQL is valid but cannot be represented as a Corium mutation.
    #[error("SQL mutation error: {0}")]
    Mutation(String),
    /// SQL parsing failed before `DataFusion` planning.
    #[error(transparent)]
    Parser(#[from] sqlparser::parser::ParserError),
}

/// One SQL environment over a fixed immutable database view.
pub struct SqlSession {
    context: SessionContext,
    db: Db,
    read: ReadContext,
    basis_t: u64,
    view: DbView,
}

impl SqlSession {
    /// Builds the SQL catalog for `db`, read without restriction.
    ///
    /// Current, as-of, and since views get namespace-derived wide tables in
    /// `corium`; every view gets normalized relations in `corium_sys`.
    ///
    /// # Errors
    /// Returns [`SqlError`] when the database schema cannot be projected.
    pub fn new(db: &Db) -> Result<Self, SqlError> {
        Self::with_read(db, &ReadContext::open())
    }

    /// Builds the SQL catalog for `db` as one principal sees it.
    ///
    /// A column `read` hides keeps its declared type and reports NULL, and
    /// never takes a pushed predicate — so a restricted session runs the same
    /// SQL as an unrestricted one and simply learns less from it.
    ///
    /// # Errors
    /// Returns [`SqlError`] when the database schema cannot be projected.
    pub fn with_read(db: &Db, read: &ReadContext) -> Result<Self, SqlError> {
        let context = SessionContext::new();
        catalog::register(&context, db, read)?;
        Ok(Self {
            context,
            db: db.clone(),
            read: read.clone(),
            basis_t: db.basis_t(),
            view: db.view(),
        })
    }

    /// Transaction basis captured by this session.
    #[must_use]
    pub const fn basis_t(&self) -> u64 {
        self.basis_t
    }

    /// Corium time view captured by this session.
    #[must_use]
    pub const fn view(&self) -> DbView {
        self.view
    }

    /// Registered Corium relations as `schema.table` names.
    #[must_use]
    pub fn tables(&self) -> Vec<String> {
        let Some(catalog) = self.context.catalog("datafusion") else {
            return Vec::new();
        };
        let mut tables = Vec::new();
        for schema_name in ["corium", "corium_sys"] {
            if let Some(schema) = catalog.schema(schema_name) {
                tables.extend(
                    schema
                        .table_names()
                        .into_iter()
                        .map(|table| format!("{schema_name}.{table}")),
                );
            }
        }
        tables.sort();
        tables
    }

    /// Plans and starts a read-only SQL query.
    ///
    /// DDL, DML, and session-mutating statements are rejected by this method;
    /// use [`Self::mutation`] to plan supported DML. Dropping the
    /// returned stream cancels unfinished execution.
    ///
    /// # Errors
    /// Returns [`SqlError`] for SQL parsing, planning, or execution failure.
    pub async fn query(&self, sql: &str) -> Result<SqlQuery, SqlError> {
        self.query_params(sql, &[]).await
    }

    /// Plans and starts a read-only query with PostgreSQL-style `$1`
    /// parameters bound as typed values.
    ///
    /// # Errors
    /// Returns [`SqlError`] for parameter binding, planning, or execution
    /// failure.
    pub async fn query_params(&self, sql: &str, params: &[SqlValue]) -> Result<SqlQuery, SqlError> {
        let options = SQLOptions::new()
            .with_allow_ddl(false)
            .with_allow_dml(false)
            .with_allow_statements(false);
        let mut frame = self.context.sql_with_options(sql, options).await?;
        if !params.is_empty() {
            frame = frame.with_param_values(
                params
                    .iter()
                    .map(SqlValue::to_scalar)
                    .collect::<Result<Vec<_>, _>>()?,
            )?;
        }
        let stream = frame.execute_stream().await?;
        let columns = stream
            .schema()
            .fields()
            .iter()
            .map(|field| SqlColumn::from_arrow(field))
            .collect();
        Ok(SqlQuery {
            columns,
            stream,
            batch: None,
            row: 0,
        })
    }

    /// Plans one supported `INSERT`, `UPDATE`, or `DELETE` into Corium
    /// transaction forms. Returns `None` for read-only statements.
    ///
    /// The returned mutation is fenced to this session's basis. Its forms
    /// must be submitted through the normal transactor path and then
    /// [`SqlMutation::finish`] called with the committed database value and
    /// tempid map to produce any `RETURNING` rows.
    ///
    /// # Errors
    /// Returns [`SqlError`] for malformed or unsupported mutation shapes.
    pub async fn mutation(&self, sql: &str) -> Result<Option<SqlMutation>, SqlError> {
        self.mutation_params(sql, &[]).await
    }

    /// Plans one supported mutation with PostgreSQL-style `$1` parameters.
    ///
    /// # Errors
    /// Returns [`SqlError`] for malformed, unbound, or unsupported mutation
    /// shapes.
    pub async fn mutation_params(
        &self,
        sql: &str,
        params: &[SqlValue],
    ) -> Result<Option<SqlMutation>, SqlError> {
        mutation::plan(&self.db, &self.read, sql, params).await
    }

    /// Describes the columns produced by a mutation's `RETURNING` clause
    /// without evaluating its rows or planning transaction forms.
    ///
    /// Returns `None` for a non-mutation and an empty vector for a mutation
    /// without `RETURNING`.
    ///
    /// # Errors
    /// Returns [`SqlError`] for malformed mutation syntax or an invalid
    /// returning projection.
    pub async fn mutation_columns(
        &self,
        sql: &str,
        params: &[SqlValue],
    ) -> Result<Option<Vec<SqlColumn>>, SqlError> {
        mutation::describe(&self.db, &self.read, sql, params).await
    }
}

/// Streaming result of one SQL statement.
pub struct SqlQuery {
    columns: Vec<SqlColumn>,
    stream: SendableRecordBatchStream,
    batch: Option<RecordBatch>,
    row: usize,
}

impl SqlQuery {
    /// Result columns in projection order.
    #[must_use]
    pub fn columns(&self) -> &[SqlColumn] {
        &self.columns
    }

    /// Reads the next result row, or `None` at end of stream.
    ///
    /// # Errors
    /// Returns [`SqlError`] when execution or value conversion fails.
    pub async fn next_row(&mut self) -> Result<Option<SqlRow>, SqlError> {
        loop {
            if let Some(batch) = &self.batch
                && self.row < batch.num_rows()
            {
                let row = batch
                    .columns()
                    .iter()
                    .map(|array| {
                        datafusion::common::ScalarValue::try_from_array(array.as_ref(), self.row)
                            .map_err(SqlError::from)
                            .and_then(SqlValue::from_scalar)
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                self.row += 1;
                return Ok(Some(row));
            }
            match self.stream.next().await {
                Some(batch) => {
                    self.batch = Some(batch?);
                    self.row = 0;
                }
                None => return Ok(None),
            }
        }
    }

    /// Collects all remaining rows.
    ///
    /// # Errors
    /// Returns [`SqlError`] when execution or value conversion fails.
    pub async fn collect(mut self) -> Result<Vec<SqlRow>, SqlError> {
        let mut rows = Vec::new();
        while let Some(row) = self.next_row().await? {
            rows.push(row);
        }
        Ok(rows)
    }
}

#[cfg(test)]
mod tests {
    use corium_core::{
        Attribute, Cardinality, Datom, EntityId, Keyword, KeywordInterner, Partition, Schema,
        Value, ValueType,
    };
    use corium_db::{Db, Idents};
    use corium_forms::txforms::tx_items_from_edn;
    use corium_tx::prepare;

    use super::*;

    /// A fixture whose `person/secret` is protected by a class that refuses a
    /// keyless read outright (`:db.protect.missing/error`).
    fn refusing_fixture() -> Db {
        use corium_core::{MissingKeyPolicy, ProtectionClass, ProtectionTimeline, Sealed};

        let name = EntityId::from_raw(100);
        let secret = EntityId::from_raw(101);
        let class = EntityId::from_raw(90);
        let mut schema = Schema::default();
        for (id, value_type) in [(name, ValueType::Str), (secret, ValueType::Str)] {
            schema.insert(Attribute {
                id,
                value_type,
                cardinality: Cardinality::One,
                unique: None,
                is_component: false,
                indexed: false,
                no_history: false,
            });
        }
        schema.insert_class(ProtectionClass {
            id: class,
            key_id: "file:/nonexistent".to_owned(),
            algorithm: corium_core::SealAlgorithm::Aes256GcmSiv,
            scope: corium_core::ProtectionScope::Attribute,
            padding: None,
            on_missing_key: MissingKeyPolicy::Error,
            legacy_plaintext: corium_core::LegacyPlaintextPolicy::Redact,
            current_epoch: 1,
        });
        schema.set_protection(secret, ProtectionTimeline::protected_from(0, class));
        let mut idents = Idents::default();
        idents.insert(Keyword::parse("person/name"), name);
        idents.insert(Keyword::parse("person/secret"), secret);
        let alice = EntityId::from_raw(1_000);
        let tx = EntityId::new(Partition::Tx as u32, 1);
        Db::new(schema)
            .with_naming(idents, KeywordInterner::default())
            .with_transaction(
                1,
                &[
                    Datom {
                        e: alice,
                        a: name,
                        v: Value::Str("Alice".into()),
                        tx,
                        added: true,
                    },
                    Datom {
                        e: alice,
                        a: secret,
                        v: Value::Sealed(Sealed {
                            class,
                            epoch: 1,
                            vtype: ValueType::Str,
                            body: std::sync::Arc::from(&b"ciphertext"[..]),
                        }),
                        tx,
                        added: true,
                    },
                ],
            )
    }

    /// The read a strict-key-policy surface gives a principal holding no
    /// class key: a hydrator with an empty key set, which is what makes the
    /// class's own missing-key policy apply.
    fn keyless_read() -> ReadContext {
        ReadContext::open()
            .with_hydrator(std::sync::Arc::new(corium_db::protect::Hydrator::default()))
    }

    #[tokio::test]
    async fn a_refusing_class_does_not_break_unrelated_statements() {
        // A keyless session on a database holding one `error`-policy class
        // must still run every statement that does not read that column.
        // Building the catalog eagerly, or materializing unprojected columns,
        // would turn one protected attribute into a dead SQL surface for
        // every principal without its key.
        let db = refusing_fixture();
        let session =
            SqlSession::with_read(&db, &keyless_read()).expect("a keyless session still opens");
        let rows = session
            .query("SELECT name FROM corium.person")
            .await
            .expect("planning a statement that avoids the column")
            .collect()
            .await
            .expect("running it");
        assert_eq!(rows.len(), 1);
    }

    #[tokio::test]
    async fn reading_a_refusing_class_still_fails_loudly() {
        // The `error` policy exists to be a loud failure rather than a quiet
        // hole, so a read that genuinely touches the column must still raise.
        let db = refusing_fixture();
        let session = SqlSession::with_read(&db, &keyless_read()).expect("session opens");
        let refused = match session.query("SELECT secret FROM corium.person").await {
            Err(error) => error,
            Ok(query) => query
                .collect()
                .await
                .expect_err("reading the protected column refuses"),
        };
        assert!(
            refused.to_string().contains("not readable without its key"),
            "{refused}"
        );
    }

    #[tokio::test]
    async fn the_system_fact_table_is_read_when_it_is_scanned() {
        // `corium_sys.datoms` routes around every column projection, so it
        // refuses — but only when a statement actually scans it, which is why
        // it is built lazily rather than at session construction.
        let db = refusing_fixture();
        let session = SqlSession::with_read(&db, &keyless_read()).expect("session opens");
        let refused = match session.query("SELECT e FROM corium_sys.datoms").await {
            Err(error) => error,
            Ok(query) => query
                .collect()
                .await
                .expect_err("the raw fact table refuses"),
        };
        assert!(
            refused.to_string().contains("not readable without its key"),
            "{refused}"
        );
    }

    #[allow(clippy::too_many_lines)]
    fn fixture() -> Db {
        // At or above `FIRST_ATTR_ID`: the low db-partition range belongs to
        // the engine's own attributes, and `Db::new` installs them over any
        // schema that claims one of their ids.
        let name = EntityId::from_raw(100);
        let tags = EntityId::from_raw(101);
        let release_year = EntityId::from_raw(102);
        let status = EntityId::from_raw(103);
        let uuid = EntityId::from_raw(104);
        let mut schema = Schema::default();
        schema.insert(Attribute {
            id: name,
            value_type: ValueType::Str,
            cardinality: Cardinality::One,
            unique: None,
            is_component: false,
            indexed: false,
            no_history: false,
        });
        schema.insert(Attribute {
            id: tags,
            value_type: ValueType::Str,
            cardinality: Cardinality::Many,
            unique: None,
            is_component: false,
            indexed: true,
            no_history: false,
        });
        schema.insert(Attribute {
            id: release_year,
            value_type: ValueType::Long,
            cardinality: Cardinality::One,
            unique: None,
            is_component: false,
            indexed: true,
            no_history: false,
        });
        schema.insert(Attribute {
            id: status,
            value_type: ValueType::Str,
            cardinality: Cardinality::One,
            unique: None,
            is_component: false,
            indexed: false,
            no_history: false,
        });
        schema.insert(Attribute {
            id: uuid,
            value_type: ValueType::Uuid,
            cardinality: Cardinality::One,
            unique: None,
            is_component: false,
            indexed: false,
            no_history: false,
        });
        let mut idents = Idents::default();
        idents.insert(Keyword::parse("artist/name"), name);
        idents.insert(Keyword::parse("artist/tags"), tags);
        idents.insert(Keyword::parse("artist/release-year"), release_year);
        idents.insert(Keyword::parse("status"), status);
        idents.insert(Keyword::parse("artist/uuid"), uuid);
        let e = EntityId::from_raw(1_000);
        let second = EntityId::from_raw(1_001);
        let tx = EntityId::from_raw(1);
        Db::new(schema)
            .with_naming(idents, KeywordInterner::default())
            .with_transaction(
                1,
                &[
                    Datom {
                        e,
                        a: name,
                        v: Value::Str("Boards of Canada".into()),
                        tx,
                        added: true,
                    },
                    Datom {
                        e,
                        a: tags,
                        v: Value::Str("ambient".into()),
                        tx,
                        added: true,
                    },
                    Datom {
                        e,
                        a: tags,
                        v: Value::Str("electronic".into()),
                        tx,
                        added: true,
                    },
                    Datom {
                        e,
                        a: release_year,
                        v: Value::Long(1998),
                        tx,
                        added: true,
                    },
                    Datom {
                        e: second,
                        a: name,
                        v: Value::Str("Tycho".into()),
                        tx,
                        added: true,
                    },
                    Datom {
                        e: second,
                        a: release_year,
                        v: Value::Long(2011),
                        tx,
                        added: true,
                    },
                ],
            )
    }

    fn apply_mutation(
        db: &Db,
        mutation: &SqlMutation,
    ) -> (Db, std::collections::BTreeMap<String, EntityId>) {
        let mut interner = db.interner().clone();
        let items =
            tx_items_from_edn(db, &mut interner, mutation.forms()).expect("mutation forms convert");
        let t = db.basis_t() + 1;
        let tx = EntityId::new(Partition::Tx as u32, t);
        let prepared = prepare(db, items, tx, 2_000).expect("mutation prepares");
        (
            db.clone().with_transaction(t, &prepared.datoms),
            prepared.tempids,
        )
    }

    #[tokio::test]
    async fn missing_many_attribute_is_an_empty_list() {
        let session = SqlSession::new(&fixture()).expect("session");
        let rows = session
            .query("SELECT tags FROM corium.artist WHERE name = 'Tycho'")
            .await
            .expect("query")
            .collect()
            .await
            .expect("rows");
        assert_eq!(rows, vec![vec![SqlValue::List(Vec::new())]]);
    }

    #[tokio::test]
    async fn entity_equality_uses_the_wide_provider_lookup_path() {
        let session = SqlSession::new(&fixture()).expect("session");
        let rows = session
            .query("SELECT name FROM corium.artist WHERE e = 1001")
            .await
            .expect("query")
            .collect()
            .await
            .expect("rows");
        assert_eq!(rows, vec![vec![SqlValue::Text("Tycho".into())]]);

        let missing = session
            .query("SELECT name FROM corium.artist WHERE e = 9999")
            .await
            .expect("query")
            .collect()
            .await
            .expect("rows");
        assert!(missing.is_empty());
    }

    #[tokio::test]
    async fn exact_identifiers_and_indexed_ranges_are_supported() {
        let session = SqlSession::new(&fixture()).expect("session");
        let rows = session
            .query(
                "SELECT name FROM corium.artist \
                 WHERE \"release-year\" >= 2000 ORDER BY name",
            )
            .await
            .expect("query")
            .collect()
            .await
            .expect("rows");
        assert_eq!(rows, vec![vec![SqlValue::Text("Tycho".into())]]);
    }

    #[tokio::test]
    async fn untyped_query_parameters_are_coerced_by_expression_context() {
        let session = SqlSession::new(&fixture()).expect("session");
        let rows = session
            .query_params(
                "SELECT name FROM corium.artist WHERE \"release-year\" = $1",
                &[SqlValue::Unspecified("2011".into())],
            )
            .await
            .expect("contextual parameter")
            .collect()
            .await
            .expect("rows");
        assert_eq!(rows, vec![vec![SqlValue::Text("Tycho".into())]]);
    }

    #[tokio::test]
    async fn wide_table_exposes_lists_with_set_semantics() {
        let session = SqlSession::new(&fixture()).expect("session");
        let query = session
            .query(
                "SELECT e, name, tags FROM corium.artist \
                 WHERE array_has(tags, 'ambient')",
            )
            .await
            .expect("query");
        let rows = query.collect().await.expect("rows");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0], SqlValue::Unsigned(1_000));
        assert_eq!(rows[0][1], SqlValue::Text("Boards of Canada".into()));
        assert_eq!(
            rows[0][2],
            SqlValue::List(vec![
                SqlValue::Text("ambient".into()),
                SqlValue::Text("electronic".into()),
            ])
        );
    }

    #[tokio::test]
    async fn explain_reports_attribute_filter_pushdown() {
        let session = SqlSession::new(&fixture()).expect("session");
        let rows = session
            .query(
                "EXPLAIN SELECT e FROM corium.artist \
                 WHERE name >= 'Boards of Canada' AND array_has(tags, 'ambient')",
            )
            .await
            .expect("explain")
            .collect()
            .await
            .expect("rows");
        let explanation = rows
            .iter()
            .flatten()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join("\n");
        assert!(explanation.contains("partial_filters="));
        assert!(explanation.contains("name >="));
        assert!(explanation.contains("array_has"));
    }

    #[tokio::test]
    async fn history_session_exposes_events_but_not_wide_tables() {
        let session = SqlSession::new(&fixture().history()).expect("session");
        let rows = session
            .query("SELECT count(*) FROM corium_sys.datoms")
            .await
            .expect("event query")
            .collect()
            .await
            .expect("rows");
        assert_eq!(rows, vec![vec![SqlValue::Integer(6)]]);
        assert!(session.query("SELECT * FROM corium.artist").await.is_err());
    }

    #[tokio::test]
    async fn data_definition_and_modification_are_rejected() {
        let session = SqlSession::new(&fixture()).expect("session");
        assert!(
            session
                .query("CREATE TABLE nope AS SELECT 1")
                .await
                .is_err()
        );
        assert!(
            session
                .query("INSERT INTO corium.artist (e) VALUES (42)")
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn insert_plans_transaction_forms_and_returns_generated_entity() {
        let db = fixture();
        let session = SqlSession::new(&db).expect("session");
        let mutation = session
            .mutation(
                "INSERT INTO corium.artist (name, \"release-year\", tags) \
                 VALUES ('Autechre', 1994, ARRAY['electronic']) \
                 RETURNING e, name",
            )
            .await
            .expect("plan")
            .expect("mutation");
        assert_eq!(mutation.kind(), MutationKind::Insert);
        assert_eq!(mutation.expected_basis_t(), db.basis_t());
        assert_eq!(mutation.affected(), 1);

        let (after, tempids) = apply_mutation(&db, &mutation);
        let returned = mutation.finish(&after, &tempids).await.expect("returning");
        assert_eq!(returned.rows.len(), 1);
        assert_eq!(returned.rows[0][1], SqlValue::Text("Autechre".into()));
    }

    #[tokio::test]
    async fn insert_select_and_global_projection_writes_are_supported() {
        let db = fixture();
        let copied = SqlSession::new(&db)
            .expect("session")
            .mutation(
                "INSERT INTO corium.artist (name, \"release-year\") \
                 SELECT name || ' Copy', \"release-year\" \
                 FROM corium.artist WHERE name = 'Tycho' RETURNING name",
            )
            .await
            .expect("insert-select plan")
            .expect("mutation");
        let (after_copy, tempids) = apply_mutation(&db, &copied);
        let returned = copied
            .finish(&after_copy, &tempids)
            .await
            .expect("returning");
        assert_eq!(
            returned.rows,
            vec![vec![SqlValue::Text("Tycho Copy".into())]]
        );

        let global = SqlSession::new(&after_copy)
            .expect("session")
            .mutation("INSERT INTO corium._global (status) VALUES ('active') RETURNING status")
            .await
            .expect("global plan")
            .expect("mutation");
        let (after_global, tempids) = apply_mutation(&after_copy, &global);
        let returned = global
            .finish(&after_global, &tempids)
            .await
            .expect("global returning");
        assert_eq!(returned.rows, vec![vec![SqlValue::Text("active".into())]]);
    }

    #[tokio::test]
    async fn mutation_identifiers_follow_postgres_case_and_duplicate_rules() {
        let db = fixture();
        let mutation = SqlSession::new(&db)
            .expect("session")
            .mutation(
                "UPDATE CORIUM.ARTIST SET NAME = 'BoC' \
                 WHERE name = 'Boards of Canada'",
            )
            .await
            .expect("unquoted identifiers normalize")
            .expect("mutation");
        assert_eq!(mutation.affected(), 1);

        assert!(
            SqlSession::new(&db)
                .expect("session")
                .mutation("UPDATE corium.artist SET name = 'a', name = 'b'")
                .await
                .is_err()
        );
        assert!(
            SqlSession::new(&db)
                .expect("session")
                .mutation("INSERT INTO corium.artist (name, NAME) VALUES ('a', 'b')")
                .await
                .is_err()
        );
        assert!(
            SqlSession::new(&db)
                .expect("session")
                .mutation("UPDATE corium.\"Artist\" SET name = 'a'")
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn untyped_parameters_coerce_to_targets_and_uuid_is_strict() {
        let db = fixture();
        let session = SqlSession::new(&db).expect("session");
        let mutation = session
            .mutation_params(
                "UPDATE corium.artist SET \"release-year\" = $1, uuid = $2 \
                 WHERE name = 'Tycho'",
                &[
                    SqlValue::Unspecified("2024".into()),
                    SqlValue::Unspecified("123e4567-e89b-12d3-a456-426614174000".into()),
                ],
            )
            .await
            .expect("untyped coercion")
            .expect("mutation");
        assert_eq!(mutation.affected(), 1);

        for malformed in ["1", "+123e4567e89b12d3a456426614174000", "123e4567"] {
            assert!(
                session
                    .mutation_params(
                        "UPDATE corium.artist SET uuid = $1 WHERE name = 'Tycho'",
                        &[SqlValue::Unspecified(malformed.into())],
                    )
                    .await
                    .is_err(),
                "accepted malformed UUID {malformed:?}"
            );
        }
    }

    #[tokio::test]
    async fn update_replaces_scalar_and_many_values() {
        let db = fixture();
        let session = SqlSession::new(&db).expect("session");
        let mutation = session
            .mutation(
                "UPDATE corium.artist \
                 SET name = 'BoC', tags = ARRAY['ambient'] \
                 WHERE name = 'Boards of Canada' \
                 RETURNING name, tags",
            )
            .await
            .expect("plan")
            .expect("mutation");
        assert_eq!(mutation.kind(), MutationKind::Update);
        assert_eq!(mutation.affected(), 1);

        let (after, tempids) = apply_mutation(&db, &mutation);
        let returned = mutation.finish(&after, &tempids).await.expect("returning");
        assert_eq!(
            returned.rows,
            vec![vec![
                SqlValue::Text("BoC".into()),
                SqlValue::List(vec![SqlValue::Text("ambient".into())]),
            ]]
        );
    }

    #[tokio::test]
    async fn zero_row_update_returning_preserves_result_columns() {
        let db = fixture();
        let mutation = SqlSession::new(&db)
            .expect("session")
            .mutation(
                "UPDATE corium.artist SET name = 'Nobody' \
                 WHERE name = 'Missing' RETURNING name",
            )
            .await
            .expect("plan")
            .expect("mutation");
        assert_eq!(mutation.affected(), 0);
        assert!(mutation.is_empty());

        let returned = mutation
            .finish(&db, &std::collections::BTreeMap::new())
            .await
            .expect("returning");
        assert_eq!(returned.columns.len(), 1);
        assert_eq!(returned.columns[0].name, "name");
        assert!(returned.rows.is_empty());
    }

    #[tokio::test]
    async fn delete_retracts_only_namespace_attributes_and_returns_old_row() {
        let db = fixture();
        let session = SqlSession::new(&db).expect("session");
        let mutation = session
            .mutation(
                "DELETE FROM corium.artist \
                 WHERE name = 'Tycho' RETURNING e, name",
            )
            .await
            .expect("plan")
            .expect("mutation");
        assert_eq!(mutation.kind(), MutationKind::Delete);
        assert_eq!(mutation.affected(), 1);

        let returned = mutation
            .finish(&db, &std::collections::BTreeMap::new())
            .await
            .expect("pre-delete returning");
        assert_eq!(returned.rows[0][1], SqlValue::Text("Tycho".into()));

        let (after, _) = apply_mutation(&db, &mutation);
        let rows = SqlSession::new(&after)
            .expect("session")
            .query("SELECT name FROM corium.artist WHERE name = 'Tycho'")
            .await
            .expect("query")
            .collect()
            .await
            .expect("rows");
        assert!(rows.is_empty());
    }

    /// A database holding one open saga with a compensation-ledger entry,
    /// built through the ordinary transaction path the registry uses.
    fn saga_fixture() -> Db {
        let mut schema = Schema::default();
        let mut idents = Idents::default();
        corium_db::bootstrap::install(&mut schema, &mut idents);
        let db = Db::new(schema).with_naming(idents, KeywordInterner::default());
        let forms = corium_query::edn::read_all(
            r#"
            [:db/add "s" :db.saga/id #uuid "0000000000000000000000000000002a"]
            [:db/add "s" :db.saga/status :db.saga.status/open]
            [:db/add "s" :db.saga/basis-t 7]
            [:db/add "s" :db.saga/owner "alice"]
            [:db/add "s" :db.saga/expires-at #inst 1700000000000]
            [:db/add "s" :db.saga/description "quarterly reconciliation"]
            [:db/add "s" :db.saga/compensations "c"]
            [:db/add "c" :db.saga.compensation/key "refund:1234"]
            [:db/add "c" :db.saga.compensation/status :db.saga.compensation.status/pending]
            "#,
        )
        .expect("registry forms parse");
        let mut interner = db.interner().clone();
        let items = tx_items_from_edn(&db, &mut interner, &forms).expect("forms convert");
        let idents = db.idents().clone();
        let db = db.with_naming(idents, interner);
        let tx = EntityId::new(Partition::Tx as u32, 1);
        let prepared = prepare(&db, items, tx, 1_000).expect("the open prepares");
        db.with_transaction(1, &prepared.datoms)
    }

    #[tokio::test]
    async fn the_saga_registry_is_a_system_relation() {
        let session = SqlSession::new(&saga_fixture()).expect("session");
        let rows = session
            .query(
                "SELECT id, status, basis_t, owner, description, sealed, compensations \
                 FROM corium_sys.sagas",
            )
            .await
            .expect("query")
            .collect()
            .await
            .expect("rows");
        assert_eq!(
            rows,
            vec![vec![
                SqlValue::Text("0000000000000000000000000000002a".into()),
                SqlValue::Text("open".into()),
                SqlValue::Integer(7),
                SqlValue::Text("alice".into()),
                SqlValue::Text("quarterly reconciliation".into()),
                SqlValue::Boolean(false),
                SqlValue::Unsigned(1),
            ]]
        );
    }

    #[tokio::test]
    async fn the_compensation_ledger_joins_to_its_saga() {
        let session = SqlSession::new(&saga_fixture()).expect("session");
        let rows = session
            .query(
                "SELECT c.key, c.status, c.completed_at, s.status \
                 FROM corium_sys.saga_compensations c \
                 JOIN corium_sys.sagas s ON s.id = c.saga_id",
            )
            .await
            .expect("query")
            .collect()
            .await
            .expect("rows");
        assert_eq!(
            rows,
            vec![vec![
                SqlValue::Text("refund:1234".into()),
                SqlValue::Text(":db.saga.compensation.status/pending".into()),
                SqlValue::Null,
                SqlValue::Text("open".into()),
            ]]
        );
    }
}
