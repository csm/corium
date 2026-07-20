//! Read-only SQL execution over immutable Corium database values.
//!
//! A [`SqlSession`] captures one [`corium_db::Db`] time view. Current,
//! as-of, and since views expose one wide table per attribute namespace;
//! history views expose normalized event relations only.

mod catalog;
mod value;

use arrow::record_batch::RecordBatch;
use corium_db::{Db, DbView};
use datafusion::execution::context::SQLOptions;
use datafusion::physical_plan::SendableRecordBatchStream;
use datafusion::prelude::SessionContext;
use futures::StreamExt as _;
use thiserror::Error;

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
}

/// One read-only SQL environment over a fixed immutable database view.
pub struct SqlSession {
    context: SessionContext,
    basis_t: u64,
    view: DbView,
}

impl SqlSession {
    /// Builds the SQL catalog for `db`.
    ///
    /// Current, as-of, and since views get namespace-derived wide tables in
    /// `corium`; every view gets normalized relations in `corium_sys`.
    ///
    /// # Errors
    /// Returns [`SqlError`] when the database schema cannot be projected.
    pub fn new(db: &Db) -> Result<Self, SqlError> {
        let context = SessionContext::new();
        catalog::register(&context, db)?;
        Ok(Self {
            context,
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
    /// DDL, DML, and session-mutating statements are rejected. Dropping the
    /// returned stream cancels unfinished execution.
    ///
    /// # Errors
    /// Returns [`SqlError`] for SQL parsing, planning, or execution failure.
    pub async fn query(&self, sql: &str) -> Result<SqlQuery, SqlError> {
        let options = SQLOptions::new()
            .with_allow_ddl(false)
            .with_allow_dml(false)
            .with_allow_statements(false);
        let frame = self.context.sql_with_options(sql, options).await?;
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
        Attribute, Cardinality, Datom, EntityId, Keyword, KeywordInterner, Schema, Value, ValueType,
    };
    use corium_db::{Db, Idents};

    use super::*;

    fn fixture() -> Db {
        let name = EntityId::from_raw(10);
        let tags = EntityId::from_raw(11);
        let release_year = EntityId::from_raw(12);
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
        let mut idents = Idents::default();
        idents.insert(Keyword::parse("artist/name"), name);
        idents.insert(Keyword::parse("artist/tags"), tags);
        idents.insert(Keyword::parse("artist/release-year"), release_year);
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
}
