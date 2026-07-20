use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use arrow::array::{ArrayRef, new_empty_array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use corium_core::{
    AttrId, Attribute, Cardinality, EntityId, IndexOrder, Keyword, TotalF64, Unique, Value,
    ValueType,
};
use corium_db::{Db, DbView};
use datafusion::catalog::{MemTable, MemorySchemaProvider, SchemaProvider, Session};
use datafusion::common::{DataFusionError, Result as DataFusionResult, ScalarValue};
use datafusion::datasource::TableProvider;
use datafusion::logical_expr::{Expr, Operator, TableProviderFilterPushDown, TableType};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionContext;

use crate::SqlError;

const WIDE_SCHEMA: &str = "corium";
const SYSTEM_SCHEMA: &str = "corium_sys";

#[derive(Clone, Debug)]
struct ProjectedAttribute {
    id: AttrId,
    name: String,
    attribute: Attribute,
}

pub(crate) fn register(context: &SessionContext, db: &Db) -> Result<(), SqlError> {
    let catalog = context
        .catalog("datafusion")
        .ok_or_else(|| SqlError::Schema("DataFusion default catalog is unavailable".into()))?;
    let system = Arc::new(MemorySchemaProvider::new());
    catalog.register_schema(SYSTEM_SCHEMA, system.clone())?;
    register_system_tables(&system, db)?;

    if db.view() != DbView::History {
        let wide = Arc::new(MemorySchemaProvider::new());
        catalog.register_schema(WIDE_SCHEMA, wide.clone())?;
        register_wide_tables(&wide, db)?;
    }
    Ok(())
}

fn register_wide_tables(schema: &MemorySchemaProvider, db: &Db) -> Result<(), SqlError> {
    let mut namespaces: BTreeMap<String, Vec<ProjectedAttribute>> = BTreeMap::new();
    for (id, attribute) in db.schema().iter() {
        let Some(ident) = db.idents().ident(*id) else {
            continue;
        };
        if ident.name == "e" {
            return Err(SqlError::Schema(format!(
                "attribute {ident} collides with reserved SQL column e"
            )));
        }
        let namespace = ident.namespace.clone().unwrap_or_else(|| "_global".into());
        namespaces
            .entry(namespace)
            .or_default()
            .push(ProjectedAttribute {
                id: *id,
                name: ident.name.clone(),
                attribute: attribute.clone(),
            });
    }

    for (namespace, mut attributes) in namespaces {
        attributes.sort_by(|left, right| left.name.cmp(&right.name));
        let mut fields = vec![Field::new("e", DataType::UInt64, false)];
        fields.extend(attributes.iter().map(|projected| {
            let value_type = arrow_type(projected.attribute.value_type);
            match projected.attribute.cardinality {
                Cardinality::One => Field::new(&projected.name, value_type, true),
                Cardinality::Many => Field::new(
                    &projected.name,
                    DataType::List(Arc::new(Field::new("item", value_type, false))),
                    false,
                ),
            }
        }));
        schema.register_table(
            namespace,
            Arc::new(WideTable {
                db: db.clone(),
                schema: Arc::new(Schema::new(fields)),
                attributes,
            }),
        )?;
    }
    Ok(())
}

/// A namespace projection materialized at scan time. Keeping this behind a
/// provider avoids making Arrow part of the public API and gives Corium a seam
/// for progressively pushing predicates into its covering indexes.
#[derive(Debug)]
struct WideTable {
    db: Db,
    schema: SchemaRef,
    attributes: Vec<ProjectedAttribute>,
}

#[derive(Clone, Debug)]
enum PushedPredicate {
    Entity(EntityId),
    Attribute {
        attribute: ProjectedAttribute,
        comparison: Comparison,
        value: Value,
    },
}

#[derive(Clone, Copy, Debug)]
enum Comparison {
    Eq,
    Lt,
    LtEq,
    Gt,
    GtEq,
}

#[async_trait]
impl TableProvider for WideTable {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        _limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        let mut candidates: Option<BTreeSet<EntityId>> = None;
        for predicate in filters
            .iter()
            .filter_map(|filter| self.pushed_predicate(filter))
        {
            let matching = self.candidates(&predicate);
            candidates = Some(match candidates {
                None => matching,
                Some(mut current) => {
                    current.retain(|entity| matching.contains(entity));
                    current
                }
            });
        }
        let rows = match candidates {
            Some(entities) => entities
                .into_iter()
                .filter_map(|entity| self.row_for(entity).transpose())
                .collect::<DataFusionResult<Vec<_>>>()?,
            None => self.all_rows()?,
        };
        let table = make_mem_table(self.schema.clone(), &rows)?;
        table.scan(state, projection, &[], None).await
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DataFusionResult<Vec<TableProviderFilterPushDown>> {
        Ok(filters
            .iter()
            .map(|filter| {
                if self.pushed_predicate(filter).is_some() {
                    // Retain the filter in DataFusion while using it to choose
                    // a candidate set from Corium's indexes.
                    TableProviderFilterPushDown::Inexact
                } else {
                    TableProviderFilterPushDown::Unsupported
                }
            })
            .collect())
    }
}

impl WideTable {
    fn pushed_predicate(&self, expression: &Expr) -> Option<PushedPredicate> {
        if let Some(entity) = entity_equality(expression) {
            return Some(PushedPredicate::Entity(entity));
        }
        if let Expr::BinaryExpr(binary) = expression
            && let Some(comparison) = comparison(binary.op)
        {
            if let Some(predicate) =
                self.column_comparison(&binary.left, &binary.right, comparison, Cardinality::One)
            {
                return Some(predicate);
            }
            return self.column_comparison(
                &binary.right,
                &binary.left,
                comparison.reverse(),
                Cardinality::One,
            );
        }
        let Expr::ScalarFunction(function) = expression else {
            return None;
        };
        if function.name() != "array_has" || function.args.len() != 2 {
            return None;
        }
        self.column_comparison(
            &function.args[0],
            &function.args[1],
            Comparison::Eq,
            Cardinality::Many,
        )
    }

    fn column_comparison(
        &self,
        column: &Expr,
        literal: &Expr,
        comparison: Comparison,
        cardinality: Cardinality,
    ) -> Option<PushedPredicate> {
        let Expr::Column(column) = column else {
            return None;
        };
        let attribute = self
            .attributes
            .iter()
            .find(|attribute| {
                attribute.name == column.name && attribute.attribute.cardinality == cardinality
            })?
            .clone();
        let scalar = literal_scalar(literal)?;
        let value = scalar_value(&self.db, attribute.attribute.value_type, scalar)?;
        Some(PushedPredicate::Attribute {
            attribute,
            comparison,
            value,
        })
    }

    fn candidates(&self, predicate: &PushedPredicate) -> BTreeSet<EntityId> {
        match predicate {
            PushedPredicate::Entity(entity) => BTreeSet::from([*entity]),
            PushedPredicate::Attribute {
                attribute,
                comparison,
                value,
            } => {
                let covered = attribute.attribute.indexed || attribute.attribute.unique.is_some();
                if covered {
                    let start = matches!(
                        comparison,
                        Comparison::Eq | Comparison::Gt | Comparison::GtEq
                    )
                    .then_some(value);
                    let end = matches!(comparison, Comparison::Lt).then_some(value);
                    if matches!(comparison, Comparison::Eq) {
                        self.db
                            .index_range(attribute.id, start, end)
                            .take_while(|datom| datom.v == *value)
                            .map(|datom| datom.e)
                            .collect()
                    } else {
                        self.db
                            .index_range(attribute.id, start, end)
                            .filter(|datom| comparison.matches(&datom.v, value))
                            .map(|datom| datom.e)
                            .collect()
                    }
                } else {
                    self.db
                        .datoms_for_attribute(attribute.id)
                        .filter(|datom| comparison.matches(&datom.v, value))
                        .map(|datom| datom.e)
                        .collect()
                }
            }
        }
    }

    fn all_rows(&self) -> DataFusionResult<Vec<Vec<ScalarValue>>> {
        let attribute_ids: BTreeSet<AttrId> = self
            .attributes
            .iter()
            .map(|attribute| attribute.id)
            .collect();
        let mut facts: BTreeMap<EntityId, BTreeMap<AttrId, Vec<Value>>> = BTreeMap::new();
        for datom in self.db.datoms_at(IndexOrder::Eavt) {
            if attribute_ids.contains(&datom.a) {
                facts
                    .entry(datom.e)
                    .or_default()
                    .entry(datom.a)
                    .or_default()
                    .push(datom.v.clone());
            }
        }
        facts
            .iter()
            .map(|(entity, values)| self.make_row(*entity, values))
            .collect()
    }

    fn row_for(&self, entity: EntityId) -> DataFusionResult<Option<Vec<ScalarValue>>> {
        let values = self
            .attributes
            .iter()
            .map(|attribute| (attribute.id, self.db.values(entity, attribute.id)))
            .filter(|(_, values)| !values.is_empty())
            .collect::<BTreeMap<_, _>>();
        if values.is_empty() {
            Ok(None)
        } else {
            self.make_row(entity, &values).map(Some)
        }
    }

    fn make_row(
        &self,
        entity: EntityId,
        values: &BTreeMap<AttrId, Vec<Value>>,
    ) -> DataFusionResult<Vec<ScalarValue>> {
        let mut row = vec![ScalarValue::UInt64(Some(entity.raw()))];
        for projected in &self.attributes {
            let stored = values.get(&projected.id).map_or(&[][..], Vec::as_slice);
            let data_type = arrow_type(projected.attribute.value_type);
            match projected.attribute.cardinality {
                Cardinality::One => match stored {
                    [] => row.push(ScalarValue::try_new_null(&data_type)?),
                    [value] => row.push(value_scalar(&self.db, value)),
                    _ => {
                        return Err(DataFusionError::Execution(format!(
                            "cardinality-one attribute {} has {} live values for entity {}",
                            projected.name,
                            stored.len(),
                            entity.raw()
                        )));
                    }
                },
                Cardinality::Many => {
                    let items = stored
                        .iter()
                        .map(|value| value_scalar(&self.db, value))
                        .collect::<Vec<_>>();
                    row.push(ScalarValue::List(ScalarValue::new_list(
                        &items, &data_type, false,
                    )));
                }
            }
        }
        Ok(row)
    }
}

impl Comparison {
    const fn reverse(self) -> Self {
        match self {
            Self::Eq => Self::Eq,
            Self::Lt => Self::Gt,
            Self::LtEq => Self::GtEq,
            Self::Gt => Self::Lt,
            Self::GtEq => Self::LtEq,
        }
    }

    fn matches(self, actual: &Value, expected: &Value) -> bool {
        match self {
            Self::Eq => actual == expected,
            Self::Lt => actual < expected,
            Self::LtEq => actual <= expected,
            Self::Gt => actual > expected,
            Self::GtEq => actual >= expected,
        }
    }
}

const fn comparison(operator: Operator) -> Option<Comparison> {
    match operator {
        Operator::Eq => Some(Comparison::Eq),
        Operator::Lt => Some(Comparison::Lt),
        Operator::LtEq => Some(Comparison::LtEq),
        Operator::Gt => Some(Comparison::Gt),
        Operator::GtEq => Some(Comparison::GtEq),
        _ => None,
    }
}

fn entity_equality(expression: &Expr) -> Option<EntityId> {
    let Expr::BinaryExpr(binary) = expression else {
        return None;
    };
    if binary.op != Operator::Eq {
        return None;
    }
    column_entity(&binary.left, &binary.right)
        .or_else(|| column_entity(&binary.right, &binary.left))
}

fn column_entity(column: &Expr, value: &Expr) -> Option<EntityId> {
    let Expr::Column(column) = column else {
        return None;
    };
    if column.name != "e" {
        return None;
    }
    literal_entity(value).map(EntityId::from_raw)
}

fn literal_entity(expression: &Expr) -> Option<u64> {
    match expression {
        Expr::Literal(ScalarValue::UInt64(Some(value)), _) => Some(*value),
        Expr::Literal(ScalarValue::Int64(Some(value)), _) if *value >= 0 => {
            u64::try_from(*value).ok()
        }
        Expr::Cast(cast) => literal_entity(&cast.expr),
        Expr::TryCast(cast) => literal_entity(&cast.expr),
        _ => None,
    }
}

fn literal_scalar(expression: &Expr) -> Option<&ScalarValue> {
    match expression {
        Expr::Literal(value, _) => Some(value),
        Expr::Cast(cast) => literal_scalar(&cast.expr),
        Expr::TryCast(cast) => literal_scalar(&cast.expr),
        _ => None,
    }
}

fn scalar_value(db: &Db, value_type: ValueType, scalar: &ScalarValue) -> Option<Value> {
    match value_type {
        ValueType::Bool => match scalar {
            ScalarValue::Boolean(Some(value)) => Some(Value::Bool(*value)),
            _ => None,
        },
        ValueType::Long => scalar_i64(scalar).map(Value::Long),
        ValueType::Double => scalar_f64(scalar).map(|value| Value::Double(TotalF64(value))),
        ValueType::Instant => match scalar {
            ScalarValue::TimestampMillisecond(Some(value), _) => Some(Value::Instant(*value)),
            _ => scalar_i64(scalar).map(Value::Instant),
        },
        ValueType::Uuid => scalar_text(scalar).and_then(parse_uuid).map(Value::Uuid),
        ValueType::Keyword => scalar_text(scalar).and_then(|text| {
            let keyword = Keyword::parse(text.strip_prefix(':').unwrap_or(text));
            db.interner().get(&keyword).map(Value::Keyword)
        }),
        ValueType::Str => scalar_text(scalar).map(|value| Value::Str(value.into())),
        ValueType::Bytes => scalar_bytes(scalar).map(|value| Value::Bytes(value.into())),
        ValueType::Ref => scalar_u64(scalar).map(|value| Value::Ref(EntityId::from_raw(value))),
    }
}

fn scalar_i64(value: &ScalarValue) -> Option<i64> {
    match value {
        ScalarValue::Int8(Some(value)) => Some(i64::from(*value)),
        ScalarValue::Int16(Some(value)) => Some(i64::from(*value)),
        ScalarValue::Int32(Some(value)) => Some(i64::from(*value)),
        ScalarValue::Int64(Some(value)) => Some(*value),
        ScalarValue::UInt8(Some(value)) => Some(i64::from(*value)),
        ScalarValue::UInt16(Some(value)) => Some(i64::from(*value)),
        ScalarValue::UInt32(Some(value)) => Some(i64::from(*value)),
        ScalarValue::UInt64(Some(value)) => i64::try_from(*value).ok(),
        _ => None,
    }
}

fn scalar_u64(value: &ScalarValue) -> Option<u64> {
    match value {
        ScalarValue::UInt8(Some(value)) => Some(u64::from(*value)),
        ScalarValue::UInt16(Some(value)) => Some(u64::from(*value)),
        ScalarValue::UInt32(Some(value)) => Some(u64::from(*value)),
        ScalarValue::UInt64(Some(value)) => Some(*value),
        _ => scalar_i64(value).and_then(|value| u64::try_from(value).ok()),
    }
}

fn scalar_f64(value: &ScalarValue) -> Option<f64> {
    match value {
        ScalarValue::Float16(Some(value)) => Some(f64::from(*value)),
        ScalarValue::Float32(Some(value)) => Some(f64::from(*value)),
        ScalarValue::Float64(Some(value)) => Some(*value),
        _ => None,
    }
}

fn scalar_text(value: &ScalarValue) -> Option<&str> {
    match value {
        ScalarValue::Utf8(Some(value))
        | ScalarValue::Utf8View(Some(value))
        | ScalarValue::LargeUtf8(Some(value)) => Some(value),
        _ => None,
    }
}

fn scalar_bytes(value: &ScalarValue) -> Option<&[u8]> {
    match value {
        ScalarValue::Binary(Some(value))
        | ScalarValue::BinaryView(Some(value))
        | ScalarValue::LargeBinary(Some(value))
        | ScalarValue::FixedSizeBinary(_, Some(value)) => Some(value),
        _ => None,
    }
}

fn parse_uuid(value: &str) -> Option<u128> {
    let compact = value.replace('-', "");
    (compact.len() == 32)
        .then(|| u128::from_str_radix(&compact, 16).ok())
        .flatten()
}

fn register_system_tables(schema: &MemorySchemaProvider, db: &Db) -> Result<(), SqlError> {
    schema.register_table("datoms".into(), datoms_table(db)?)?;
    schema.register_table("attributes".into(), attributes_table(db)?)?;
    schema.register_table("idents".into(), idents_table(db)?)?;
    Ok(())
}

fn datoms_table(db: &Db) -> Result<Arc<dyn TableProvider>, SqlError> {
    let fields = vec![
        Field::new("e", DataType::UInt64, false),
        Field::new("a", DataType::UInt64, false),
        Field::new("attr", DataType::Utf8, true),
        Field::new("value_type", DataType::Utf8, false),
        Field::new("v_bool", DataType::Boolean, true),
        Field::new("v_long", DataType::Int64, true),
        Field::new("v_double", DataType::Float64, true),
        Field::new(
            "v_instant",
            DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
            true,
        ),
        Field::new("v_uuid", DataType::Utf8, true),
        Field::new("v_keyword", DataType::Utf8, true),
        Field::new("v_string", DataType::Utf8, true),
        Field::new("v_bytes", DataType::Binary, true),
        Field::new("v_ref", DataType::UInt64, true),
        Field::new("tx", DataType::UInt64, false),
        Field::new("t", DataType::UInt64, false),
        Field::new("added", DataType::Boolean, false),
    ];
    let mut rows = Vec::new();
    for datom in db.datoms_at(IndexOrder::Eavt) {
        let mut row = vec![
            ScalarValue::UInt64(Some(datom.e.raw())),
            ScalarValue::UInt64(Some(datom.a.raw())),
            ScalarValue::Utf8(db.idents().ident(datom.a).map(ToString::to_string)),
            ScalarValue::Utf8(Some(value_type_name(&datom.v).into())),
        ];
        for expected in [
            ValueType::Bool,
            ValueType::Long,
            ValueType::Double,
            ValueType::Instant,
            ValueType::Uuid,
            ValueType::Keyword,
            ValueType::Str,
            ValueType::Bytes,
            ValueType::Ref,
        ] {
            if datom.v.has_type(expected) {
                row.push(value_scalar(db, &datom.v));
            } else {
                row.push(ScalarValue::try_new_null(&arrow_type(expected))?);
            }
        }
        row.extend([
            ScalarValue::UInt64(Some(datom.tx.raw())),
            ScalarValue::UInt64(Some(datom.tx.sequence())),
            ScalarValue::Boolean(Some(datom.added)),
        ]);
        rows.push(row);
    }
    make_table(fields, &rows)
}

fn attributes_table(db: &Db) -> Result<Arc<dyn TableProvider>, SqlError> {
    let fields = vec![
        Field::new("a", DataType::UInt64, false),
        Field::new("ident", DataType::Utf8, true),
        Field::new("value_type", DataType::Utf8, false),
        Field::new("cardinality", DataType::Utf8, false),
        Field::new("unique", DataType::Utf8, true),
        Field::new("indexed", DataType::Boolean, false),
        Field::new("component", DataType::Boolean, false),
        Field::new("no_history", DataType::Boolean, false),
    ];
    let rows: Vec<Vec<ScalarValue>> = db
        .schema()
        .iter()
        .map(|(id, attribute)| {
            vec![
                ScalarValue::UInt64(Some(id.raw())),
                ScalarValue::Utf8(db.idents().ident(*id).map(ToString::to_string)),
                ScalarValue::Utf8(Some(schema_type_name(attribute.value_type).into())),
                ScalarValue::Utf8(Some(
                    match attribute.cardinality {
                        Cardinality::One => "one",
                        Cardinality::Many => "many",
                    }
                    .into(),
                )),
                ScalarValue::Utf8(attribute.unique.map(|unique| {
                    match unique {
                        Unique::Identity => "identity",
                        Unique::Value => "value",
                    }
                    .into()
                })),
                ScalarValue::Boolean(Some(attribute.indexed)),
                ScalarValue::Boolean(Some(attribute.is_component)),
                ScalarValue::Boolean(Some(attribute.no_history)),
            ]
        })
        .collect();
    make_table(fields, &rows)
}

fn idents_table(db: &Db) -> Result<Arc<dyn TableProvider>, SqlError> {
    let fields = vec![
        Field::new("e", DataType::UInt64, false),
        Field::new("ident", DataType::Utf8, false),
    ];
    let rows: Vec<Vec<ScalarValue>> = db
        .idents()
        .iter()
        .map(|(ident, entity)| {
            vec![
                ScalarValue::UInt64(Some(entity.raw())),
                ScalarValue::Utf8(Some(ident.to_string())),
            ]
        })
        .collect();
    make_table(fields, &rows)
}

fn make_table(
    fields: Vec<Field>,
    rows: &[Vec<ScalarValue>],
) -> Result<Arc<dyn TableProvider>, SqlError> {
    let schema: SchemaRef = Arc::new(Schema::new(fields));
    Ok(Arc::new(make_mem_table(schema, rows)?))
}

fn make_mem_table(schema: SchemaRef, rows: &[Vec<ScalarValue>]) -> DataFusionResult<MemTable> {
    let mut columns = Vec::with_capacity(schema.fields().len());
    for column in 0..schema.fields().len() {
        let scalars = rows
            .iter()
            .map(|row| row[column].clone())
            .collect::<Vec<_>>();
        let array: ArrayRef = if scalars.is_empty() {
            new_empty_array(schema.field(column).data_type())
        } else {
            ScalarValue::iter_to_array(scalars)?
        };
        columns.push(array);
    }
    let batch = RecordBatch::try_new(schema.clone(), columns)?;
    MemTable::try_new(schema, vec![vec![batch]])
}

fn arrow_type(value_type: ValueType) -> DataType {
    match value_type {
        ValueType::Bool => DataType::Boolean,
        ValueType::Long => DataType::Int64,
        ValueType::Double => DataType::Float64,
        ValueType::Instant => DataType::Timestamp(TimeUnit::Millisecond, Some(Arc::from("UTC"))),
        ValueType::Uuid | ValueType::Keyword | ValueType::Str => DataType::Utf8,
        ValueType::Bytes => DataType::Binary,
        ValueType::Ref => DataType::UInt64,
    }
}

fn value_scalar(db: &Db, value: &Value) -> ScalarValue {
    match value {
        Value::Bool(value) => ScalarValue::Boolean(Some(*value)),
        Value::Long(value) => ScalarValue::Int64(Some(*value)),
        Value::Double(value) => ScalarValue::Float64(Some(value.0)),
        Value::Instant(value) => {
            ScalarValue::TimestampMillisecond(Some(*value), Some("UTC".into()))
        }
        Value::Uuid(value) => ScalarValue::Utf8(Some(format!("{value:032x}"))),
        Value::Keyword(value) => ScalarValue::Utf8(Some(
            db.interner()
                .resolve(*value)
                .map_or_else(|| format!("#kw/{value}"), ToString::to_string),
        )),
        Value::Str(value) => ScalarValue::Utf8(Some(value.to_string())),
        Value::Bytes(value) => ScalarValue::Binary(Some(value.to_vec())),
        Value::Ref(value) => ScalarValue::UInt64(Some(value.raw())),
    }
}

const fn schema_type_name(value_type: ValueType) -> &'static str {
    match value_type {
        ValueType::Bool => "bool",
        ValueType::Long => "long",
        ValueType::Double => "double",
        ValueType::Instant => "instant",
        ValueType::Uuid => "uuid",
        ValueType::Keyword => "keyword",
        ValueType::Str => "string",
        ValueType::Bytes => "bytes",
        ValueType::Ref => "ref",
    }
}

fn value_type_name(value: &Value) -> &'static str {
    match value {
        Value::Bool(_) => "bool",
        Value::Long(_) => "long",
        Value::Double(_) => "double",
        Value::Instant(_) => "instant",
        Value::Uuid(_) => "uuid",
        Value::Keyword(_) => "keyword",
        Value::Str(_) => "string",
        Value::Bytes(_) => "bytes",
        Value::Ref(_) => "ref",
    }
}
