//! Translation of SQL DML into ordinary Corium transaction forms.

use std::collections::BTreeMap;

use corium_core::{Attribute, Cardinality, EntityId, Keyword, TotalF64, Value, ValueType};
use corium_db::{Db, DbView};
use corium_query::edn::Edn;
use sqlparser::ast::{
    AssignmentTarget, Delete, FromTable, Insert, ObjectName, Query, SelectItem, Statement,
    TableFactor, Update,
};
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;

use crate::{SqlColumn, SqlError, SqlRow, SqlSession, SqlValue};

/// Kind of committed SQL mutation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MutationKind {
    /// `INSERT`.
    Insert,
    /// `UPDATE`.
    Update,
    /// `DELETE`.
    Delete,
}

impl MutationKind {
    /// `PostgreSQL` command tag name.
    #[must_use]
    pub const fn command(self) -> &'static str {
        match self {
            Self::Insert => "INSERT",
            Self::Update => "UPDATE",
            Self::Delete => "DELETE",
        }
    }
}

/// Buffered rows produced by a mutation's `RETURNING` clause.
#[derive(Clone, Debug, Default)]
pub struct SqlMutationResult {
    /// Result columns, empty when the statement had no `RETURNING`.
    pub columns: Vec<SqlColumn>,
    /// Returned rows.
    pub rows: Vec<SqlRow>,
}

#[derive(Clone, Debug)]
enum EntitySelector {
    Id(EntityId),
    Temp(String),
}

/// One autocommit mutation planned against an immutable basis.
pub struct SqlMutation {
    kind: MutationKind,
    expected_basis_t: u64,
    forms: Vec<Edn>,
    affected: usize,
    table: String,
    entities: Vec<EntitySelector>,
    returning: Option<String>,
    returning_before: Option<SqlMutationResult>,
    params: Vec<SqlValue>,
}

impl SqlMutation {
    /// Mutation kind.
    #[must_use]
    pub const fn kind(&self) -> MutationKind {
        self.kind
    }

    /// Basis this read/modify/write plan requires.
    #[must_use]
    pub const fn expected_basis_t(&self) -> u64 {
        self.expected_basis_t
    }

    /// Number of rows selected or inserted by the statement.
    #[must_use]
    pub const fn affected(&self) -> usize {
        self.affected
    }

    /// Transaction forms to submit through Corium's normal authenticated and
    /// authorized transactor path.
    #[must_use]
    pub fn forms(&self) -> &[Edn] {
        &self.forms
    }

    /// Whether committing can be skipped because the statement produced no
    /// changes. This is possible for an `UPDATE` or `DELETE` matching no rows.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.forms.is_empty()
    }

    /// Describes the columns produced by `RETURNING` without committing.
    ///
    /// # Errors
    /// Returns [`SqlError`] if the returning projection cannot be planned.
    pub async fn returning_columns(&self, db: &Db) -> Result<Vec<SqlColumn>, SqlError> {
        let Some(returning) = &self.returning else {
            return Ok(Vec::new());
        };
        let query = SqlSession::new(db)?
            .query_params(
                &format!("SELECT {returning} FROM {} WHERE FALSE", self.table),
                &self.params,
            )
            .await?;
        Ok(query.columns().to_vec())
    }

    /// Produces `RETURNING` rows from the committed database value.
    ///
    /// `DELETE RETURNING` is buffered from the pre-commit snapshot. Inserts
    /// and updates are read from `db_after`, resolving generated entity ids
    /// through `tempids`.
    ///
    /// # Errors
    /// Returns [`SqlError`] if a generated id is absent or a returning query
    /// cannot be executed.
    pub async fn finish(
        &self,
        db_after: &Db,
        tempids: &BTreeMap<String, EntityId>,
    ) -> Result<SqlMutationResult, SqlError> {
        if let Some(result) = &self.returning_before {
            return Ok(result.clone());
        }
        let Some(returning) = &self.returning else {
            return Ok(SqlMutationResult::default());
        };
        let session = SqlSession::new(db_after)?;
        let mut result = SqlMutationResult::default();
        for selector in &self.entities {
            let entity = match selector {
                EntitySelector::Id(entity) => *entity,
                EntitySelector::Temp(temp) => *tempids.get(temp).ok_or_else(|| {
                    SqlError::Mutation(format!("transactor did not resolve SQL tempid {temp:?}"))
                })?,
            };
            let sql = format!(
                "SELECT {returning} FROM {} WHERE e = {}",
                self.table,
                entity.raw()
            );
            let query = session.query_params(&sql, &self.params).await?;
            if result.columns.is_empty() {
                result.columns = query.columns().to_vec();
            }
            result.rows.extend(query.collect().await?);
        }
        if result.columns.is_empty() {
            result.columns = self.returning_columns(db_after).await?;
        }
        Ok(result)
    }
}

#[derive(Clone)]
struct Projected {
    id: EntityId,
    ident: Keyword,
    attribute: Attribute,
}

struct Target {
    sql_name: String,
    attributes: BTreeMap<String, Projected>,
}

pub(crate) async fn plan(
    db: &Db,
    sql: &str,
    params: &[SqlValue],
) -> Result<Option<SqlMutation>, SqlError> {
    if db.view() != DbView::Current {
        return Err(SqlError::Mutation(
            "writes require a current database view".into(),
        ));
    }
    let mut statements = Parser::parse_sql(&PostgreSqlDialect {}, sql)?;
    if statements.len() != 1 {
        return Err(SqlError::Mutation(
            "one mutation statement is required".into(),
        ));
    }
    match statements.pop().expect("one statement") {
        Statement::Insert(insert) => plan_insert(db, insert, params).await.map(Some),
        Statement::Update(update) => plan_update(db, update, params).await.map(Some),
        Statement::Delete(delete) => plan_delete(db, delete, params).await.map(Some),
        _ => Ok(None),
    }
}

pub(crate) async fn describe(
    db: &Db,
    sql: &str,
    params: &[SqlValue],
) -> Result<Option<Vec<SqlColumn>>, SqlError> {
    if db.view() != DbView::Current {
        return Err(SqlError::Mutation(
            "writes require a current database view".into(),
        ));
    }
    let statements = Parser::parse_sql(&PostgreSqlDialect {}, sql)?;
    let [statement] = statements.as_slice() else {
        return Err(SqlError::Mutation(
            "one mutation statement is required".into(),
        ));
    };
    let (table_name, returning) = match statement {
        Statement::Insert(insert) => {
            let sqlparser::ast::TableObject::TableName(table_name) = &insert.table else {
                return Err(unsupported("INSERT target must be a table"));
            };
            (table_name, insert.returning.as_deref())
        }
        Statement::Update(update) => (
            table_factor_name(&update.table.relation)?,
            update.returning.as_deref(),
        ),
        Statement::Delete(delete) => {
            let tables = match &delete.from {
                FromTable::WithFromKeyword(tables) | FromTable::WithoutKeyword(tables) => tables,
            };
            let [table] = tables.as_slice() else {
                return Err(unsupported("DELETE requires exactly one table"));
            };
            (
                table_factor_name(&table.relation)?,
                delete.returning.as_deref(),
            )
        }
        _ => return Ok(None),
    };
    let target = target(db, table_name)?;
    let Some(returning) = returning_sql(returning) else {
        return Ok(Some(Vec::new()));
    };
    let query = SqlSession::new(db)?
        .query_params(
            &format!("SELECT {returning} FROM {} WHERE FALSE", target.sql_name),
            params,
        )
        .await?;
    Ok(Some(query.columns().to_vec()))
}

async fn plan_insert(
    db: &Db,
    insert: Insert,
    params: &[SqlValue],
) -> Result<SqlMutation, SqlError> {
    let sqlparser::ast::TableObject::TableName(table_name) = &insert.table else {
        return Err(unsupported("INSERT target must be a table"));
    };
    let target = target(db, table_name)?;
    if insert.columns.is_empty() {
        return Err(unsupported("INSERT requires an explicit column list"));
    }
    if insert.on.is_some()
        || !insert.assignments.is_empty()
        || insert.output.is_some()
        || insert.replace_into
    {
        return Err(unsupported(
            "ON CONFLICT, INSERT SET, OUTPUT, and REPLACE are not supported yet",
        ));
    }
    let source = insert
        .source
        .as_deref()
        .ok_or_else(|| unsupported("INSERT requires VALUES or a query source"))?;
    let values = evaluate_query(db, source, params).await?;
    if values.iter().any(|row| row.len() != insert.columns.len()) {
        return Err(SqlError::Mutation(
            "INSERT source width does not match its column list".into(),
        ));
    }

    let columns = insert
        .columns
        .iter()
        .map(column_name)
        .collect::<Result<Vec<_>, _>>()?;
    let mut forms = Vec::with_capacity(values.len());
    let mut entities = Vec::with_capacity(values.len());
    for (row_index, row) in values.into_iter().enumerate() {
        let temp = format!("__corium_sql_{row_index}");
        let mut entity = EntitySelector::Temp(temp.clone());
        let mut pairs = vec![(Edn::keyword("db/id"), Edn::Str(temp))];
        for (column, value) in columns.iter().zip(row) {
            if column == "e" {
                let id = entity_id(&value)?;
                if target
                    .attributes
                    .values()
                    .any(|attr| !db.values(id, attr.id).is_empty())
                {
                    return Err(SqlError::Mutation(format!(
                        "entity {} already appears in {}",
                        id.raw(),
                        target.sql_name
                    )));
                }
                entity = EntitySelector::Id(id);
                pairs[0].1 = eid(id)?;
                continue;
            }
            let projected = target
                .attributes
                .get(column)
                .ok_or_else(|| unknown_column(column, &target.sql_name))?;
            let desired = desired_values(db, projected, &value)?;
            if desired.is_empty() {
                continue;
            }
            let form = if projected.attribute.cardinality == Cardinality::Many {
                Edn::Vector(
                    desired
                        .iter()
                        .map(|value| value_to_edn(db, value))
                        .collect::<Result<_, _>>()?,
                )
            } else {
                value_to_edn(db, &desired[0])?
            };
            pairs.push((Edn::Keyword(projected.ident.clone()), form));
        }
        if pairs.len() == 1 {
            return Err(SqlError::Mutation(
                "INSERT row must assert at least one non-NULL attribute".into(),
            ));
        }
        forms.push(Edn::Map(pairs));
        entities.push(entity);
    }
    Ok(SqlMutation {
        kind: MutationKind::Insert,
        expected_basis_t: db.basis_t(),
        affected: forms.len(),
        forms,
        table: target.sql_name,
        entities,
        returning: returning_sql(insert.returning.as_deref()),
        returning_before: None,
        params: params.to_vec(),
    })
}

async fn plan_update(
    db: &Db,
    update: Update,
    params: &[SqlValue],
) -> Result<SqlMutation, SqlError> {
    if !update.table.joins.is_empty()
        || update.from.is_some()
        || update.output.is_some()
        || update.limit.is_some()
        || !update.order_by.is_empty()
    {
        return Err(unsupported(
            "joined/from/ordered/limited UPDATE is not supported yet",
        ));
    }
    let table_name = table_factor_name(&update.table.relation)?;
    let target = target(db, table_name)?;
    let mut assignments = Vec::with_capacity(update.assignments.len());
    for assignment in &update.assignments {
        let AssignmentTarget::ColumnName(name) = &assignment.target else {
            return Err(unsupported("tuple assignment is not supported"));
        };
        let column = column_name(name)?;
        if column == "e" {
            return Err(SqlError::Mutation("entity column e is immutable".into()));
        }
        let projected = target
            .attributes
            .get(&column)
            .ok_or_else(|| unknown_column(&column, &target.sql_name))?
            .clone();
        assignments.push((projected, assignment.value.to_string()));
    }
    let selection = update
        .selection
        .as_ref()
        .map(|expr| format!(" WHERE {expr}"))
        .unwrap_or_default();
    let projections = assignments
        .iter()
        .enumerate()
        .map(|(index, (_, expression))| format!("({expression}) AS \"__set_{index}\""))
        .collect::<Vec<_>>()
        .join(", ");
    let sql = if projections.is_empty() {
        format!("SELECT e FROM {}{selection}", target.sql_name)
    } else {
        format!(
            "SELECT e, {projections} FROM {}{selection}",
            target.sql_name
        )
    };
    let rows = evaluate_sql(db, &sql, params).await?;
    let mut forms = Vec::new();
    let mut entities = Vec::with_capacity(rows.len());
    for row in &rows {
        let entity = entity_id(&row[0])?;
        entities.push(EntitySelector::Id(entity));
        for ((projected, _), value) in assignments.iter().zip(&row[1..]) {
            let desired = desired_values(db, projected, value)?;
            let current = db.values(entity, projected.id);
            for old in current.iter().filter(|old| !desired.contains(old)) {
                forms.push(retract(entity, projected, value_to_edn(db, old)?)?);
            }
            for new in desired.iter().filter(|new| !current.contains(new)) {
                forms.push(add(entity, projected, value_to_edn(db, new)?)?);
            }
        }
    }
    Ok(SqlMutation {
        kind: MutationKind::Update,
        expected_basis_t: db.basis_t(),
        forms,
        affected: rows.len(),
        table: target.sql_name,
        entities,
        returning: returning_sql(update.returning.as_deref()),
        returning_before: None,
        params: params.to_vec(),
    })
}

async fn plan_delete(
    db: &Db,
    delete: Delete,
    params: &[SqlValue],
) -> Result<SqlMutation, SqlError> {
    if delete.using.is_some()
        || delete.output.is_some()
        || delete.limit.is_some()
        || !delete.order_by.is_empty()
        || !delete.tables.is_empty()
    {
        return Err(unsupported(
            "multi-table/using/ordered/limited DELETE is not supported yet",
        ));
    }
    let tables = match &delete.from {
        FromTable::WithFromKeyword(tables) | FromTable::WithoutKeyword(tables) => tables,
    };
    let [table] = tables.as_slice() else {
        return Err(unsupported("DELETE requires exactly one table"));
    };
    if !table.joins.is_empty() {
        return Err(unsupported("joined DELETE is not supported yet"));
    }
    let table_name = table_factor_name(&table.relation)?;
    let target = target(db, table_name)?;
    let selection = delete
        .selection
        .as_ref()
        .map(|expr| format!(" WHERE {expr}"))
        .unwrap_or_default();
    let entity_rows = evaluate_sql(
        db,
        &format!("SELECT e FROM {}{selection}", target.sql_name),
        params,
    )
    .await?;
    let entities = entity_rows
        .iter()
        .map(|row| entity_id(&row[0]).map(EntitySelector::Id))
        .collect::<Result<Vec<_>, _>>()?;
    let mut forms = Vec::new();
    for selector in &entities {
        let EntitySelector::Id(entity) = selector else {
            unreachable!()
        };
        for projected in target.attributes.values() {
            for old in db.values(*entity, projected.id) {
                forms.push(retract(*entity, projected, value_to_edn(db, &old)?)?);
            }
        }
    }
    let returning = returning_sql(delete.returning.as_deref());
    let returning_before = match &returning {
        None => None,
        Some(items) => Some(
            evaluate_result(
                db,
                &format!("SELECT {items} FROM {}{selection}", target.sql_name),
                params,
            )
            .await?,
        ),
    };
    Ok(SqlMutation {
        kind: MutationKind::Delete,
        expected_basis_t: db.basis_t(),
        affected: entities.len(),
        forms,
        table: target.sql_name,
        entities,
        returning,
        returning_before,
        params: params.to_vec(),
    })
}

fn target(db: &Db, name: &ObjectName) -> Result<Target, SqlError> {
    let parts = name
        .0
        .iter()
        .map(|part| {
            part.as_ident()
                .map(|ident| ident.value.clone())
                .ok_or_else(|| unsupported("dynamic table names are not supported"))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let [schema, namespace] = parts.as_slice() else {
        return Err(SqlError::Mutation(
            "writable tables must be named corium.<namespace>".into(),
        ));
    };
    if !schema.eq_ignore_ascii_case("corium") {
        return Err(SqlError::Mutation(
            "only corium namespace projections are writable".into(),
        ));
    }
    let actual_namespace = (namespace != "_global").then_some(namespace.as_str());
    let mut attributes = BTreeMap::new();
    for (id, attribute) in db.schema().iter() {
        let Some(ident) = db.idents().ident(*id) else {
            continue;
        };
        if ident.namespace.as_deref() == actual_namespace {
            attributes.insert(
                ident.name.clone(),
                Projected {
                    id: *id,
                    ident: ident.clone(),
                    attribute: attribute.clone(),
                },
            );
        }
    }
    if attributes.is_empty() {
        return Err(SqlError::Mutation(format!(
            "writable projection {name} does not exist"
        )));
    }
    Ok(Target {
        sql_name: name.to_string(),
        attributes,
    })
}

fn table_factor_name(factor: &TableFactor) -> Result<&ObjectName, SqlError> {
    match factor {
        TableFactor::Table { name, args, .. } if args.is_none() => Ok(name),
        _ => Err(unsupported("mutation target must be a plain table")),
    }
}

fn column_name(name: &ObjectName) -> Result<String, SqlError> {
    let [part] = name.0.as_slice() else {
        return Err(unsupported("qualified mutation columns are not supported"));
    };
    part.as_ident()
        .map(|ident| ident.value.clone())
        .ok_or_else(|| unsupported("dynamic column names are not supported"))
}

fn desired_values(
    db: &Db,
    projected: &Projected,
    value: &SqlValue,
) -> Result<Vec<Value>, SqlError> {
    match projected.attribute.cardinality {
        Cardinality::One => match value {
            SqlValue::Null => Ok(Vec::new()),
            SqlValue::List(_) => Err(type_error(projected, "a scalar")),
            value => Ok(vec![sql_value(db, projected, value)?]),
        },
        Cardinality::Many => match value {
            SqlValue::List(values) => {
                let mut result = Vec::with_capacity(values.len());
                for value in values {
                    if matches!(value, SqlValue::Null) {
                        return Err(type_error(projected, "a list without NULL elements"));
                    }
                    let value = sql_value(db, projected, value)?;
                    if !result.contains(&value) {
                        result.push(value);
                    }
                }
                result.sort();
                Ok(result)
            }
            _ => Err(type_error(projected, "an ARRAY")),
        },
    }
}

fn sql_value(db: &Db, projected: &Projected, value: &SqlValue) -> Result<Value, SqlError> {
    let out = match (projected.attribute.value_type, value) {
        (ValueType::Bool, SqlValue::Boolean(value)) => Value::Bool(*value),
        (ValueType::Long, SqlValue::Integer(value)) => Value::Long(*value),
        (ValueType::Long, SqlValue::Unsigned(value)) => {
            Value::Long(i64::try_from(*value).map_err(|_| type_error(projected, "a BIGINT"))?)
        }
        (ValueType::Double, SqlValue::Float(value)) => Value::Double(TotalF64(*value)),
        (ValueType::Double, SqlValue::Integer(value)) => {
            #[allow(clippy::cast_precision_loss)]
            let value = *value as f64;
            Value::Double(TotalF64(value))
        }
        (ValueType::Instant, SqlValue::TimestampMillis(value) | SqlValue::Integer(value)) => {
            Value::Instant(*value)
        }
        (ValueType::Uuid, SqlValue::Text(value)) => {
            let compact = value.replace('-', "");
            Value::Uuid(
                u128::from_str_radix(&compact, 16)
                    .map_err(|_| type_error(projected, "a UUID string"))?,
            )
        }
        (ValueType::Keyword, SqlValue::Text(value)) => {
            let keyword = Keyword::parse(value.strip_prefix(':').unwrap_or(value));
            let id = db.interner().get(&keyword).ok_or_else(|| {
                SqlError::Mutation(format!(
                    "keyword {keyword} is not interned yet; SQL keyword insertion is not supported"
                ))
            })?;
            Value::Keyword(id)
        }
        (ValueType::Str, SqlValue::Text(value)) => Value::Str(value.as_str().into()),
        (ValueType::Bytes, SqlValue::Bytes(value)) => Value::Bytes(value.clone().into()),
        (ValueType::Ref, SqlValue::Unsigned(value)) => Value::Ref(EntityId::from_raw(*value)),
        (ValueType::Ref, SqlValue::Integer(value)) if *value >= 0 => Value::Ref(
            EntityId::from_raw(u64::try_from(*value).expect("nonnegative")),
        ),
        _ => {
            return Err(type_error(
                projected,
                value_type_description(projected.attribute.value_type),
            ));
        }
    };
    Ok(out)
}

fn value_to_edn(db: &Db, value: &Value) -> Result<Edn, SqlError> {
    Ok(match value {
        Value::Bool(value) => Edn::Bool(*value),
        Value::Long(value) => Edn::Long(*value),
        Value::Double(value) => Edn::Double(*value),
        Value::Instant(value) => Edn::Tagged("inst".into(), Box::new(Edn::Long(*value))),
        Value::Uuid(value) => {
            Edn::Tagged("uuid".into(), Box::new(Edn::Str(format!("{value:032x}"))))
        }
        Value::Keyword(id) => {
            let keyword = db.interner().resolve(*id).ok_or_else(|| {
                SqlError::Mutation("new keyword value lost its SQL spelling".into())
            })?;
            Edn::Keyword(keyword.clone())
        }
        Value::Str(value) => Edn::Str(value.to_string()),
        Value::Bytes(value) => {
            use std::fmt::Write as _;

            let hex = value
                .iter()
                .fold(String::with_capacity(value.len() * 2), |mut hex, byte| {
                    let _ = write!(hex, "{byte:02x}");
                    hex
                });
            Edn::Tagged("bytes".into(), Box::new(Edn::Str(hex)))
        }
        Value::Ref(value) => return eid(*value),
    })
}

fn add(entity: EntityId, projected: &Projected, value: Edn) -> Result<Edn, SqlError> {
    Ok(Edn::Vector(vec![
        Edn::keyword("db/add"),
        eid(entity)?,
        Edn::Keyword(projected.ident.clone()),
        value,
    ]))
}

fn retract(entity: EntityId, projected: &Projected, value: Edn) -> Result<Edn, SqlError> {
    Ok(Edn::Vector(vec![
        Edn::keyword("db/retract"),
        eid(entity)?,
        Edn::Keyword(projected.ident.clone()),
        value,
    ]))
}

fn eid(entity: EntityId) -> Result<Edn, SqlError> {
    let raw = i64::try_from(entity.raw()).map_err(|_| {
        SqlError::Mutation(format!(
            "entity id {} exceeds the SQL mutation boundary",
            entity.raw()
        ))
    })?;
    Ok(Edn::Tagged("eid".into(), Box::new(Edn::Long(raw))))
}

fn entity_id(value: &SqlValue) -> Result<EntityId, SqlError> {
    match value {
        SqlValue::Unsigned(value) => Ok(EntityId::from_raw(*value)),
        SqlValue::Integer(value) if *value >= 0 => Ok(EntityId::from_raw(
            u64::try_from(*value).expect("nonnegative"),
        )),
        _ => Err(SqlError::Mutation(
            "entity column e requires a non-negative integer".into(),
        )),
    }
}

async fn evaluate_query(
    db: &Db,
    query: &Query,
    params: &[SqlValue],
) -> Result<Vec<SqlRow>, SqlError> {
    evaluate_sql(db, &query.to_string(), params).await
}

async fn evaluate_sql(db: &Db, sql: &str, params: &[SqlValue]) -> Result<Vec<SqlRow>, SqlError> {
    SqlSession::new(db)?
        .query_params(sql, params)
        .await?
        .collect()
        .await
}

async fn evaluate_result(
    db: &Db,
    sql: &str,
    params: &[SqlValue],
) -> Result<SqlMutationResult, SqlError> {
    let query = SqlSession::new(db)?.query_params(sql, params).await?;
    let columns = query.columns().to_vec();
    let rows = query.collect().await?;
    Ok(SqlMutationResult { columns, rows })
}

fn returning_sql(items: Option<&[SelectItem]>) -> Option<String> {
    items.map(|items| {
        items
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join(", ")
    })
}

fn unsupported(message: &str) -> SqlError {
    SqlError::Mutation(message.into())
}

fn unknown_column(column: &str, table: &str) -> SqlError {
    SqlError::Mutation(format!("column {column:?} does not exist in {table}"))
}

fn type_error(projected: &Projected, expected: &str) -> SqlError {
    SqlError::Mutation(format!("attribute {} expects {expected}", projected.ident))
}

const fn value_type_description(value_type: ValueType) -> &'static str {
    match value_type {
        ValueType::Bool => "a BOOLEAN",
        ValueType::Long => "a BIGINT",
        ValueType::Double => "a DOUBLE",
        ValueType::Instant => "a TIMESTAMP",
        ValueType::Uuid => "a UUID string",
        ValueType::Keyword => "keyword text",
        ValueType::Str => "TEXT",
        ValueType::Bytes => "BYTEA",
        ValueType::Ref => "an entity id",
    }
}
