//! Core Corium data types: values, datoms, ids, schema, and sortable encoding.

pub mod chunk;
pub mod datom;
pub mod encoding;
pub mod ids;
pub mod keyword;
pub mod schema;
pub mod value;

pub use datom::{Datom, IndexOrder, KEY_TX_SUFFIX_LEN, key_components};
pub use encoding::{
    DecodeError, Encodable, SealPlaintextError, decode_seal_plaintext, encode_seal_plaintext,
    encode_value,
};
pub use ids::{AttrId, EntityId, KwId, Partition, PartitionId, TxId};
pub use keyword::{FIRST_LOCAL_KW_ID, Keyword, KeywordInterner};
pub use schema::{Attribute, Cardinality, Schema, Unique, ValueType};
pub use value::{Sealed, TotalF64, Value};
