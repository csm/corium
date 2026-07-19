//! End-to-end coverage for the selectable storage backends: a mem-backed
//! node runs the whole create/transact/read path with no filesystem state.

use corium_protocol::codec::encode_edn;
use corium_query::edn::Edn;
use corium_query::{QInput, q};
use corium_transactor::StoreSpec;
use corium_transactor::node::{NodeConfig, TransactorNode};

fn schema() -> Vec<u8> {
    encode_edn(&Edn::Vector(vec![
        Edn::Map(vec![
            (Edn::keyword("db/ident"), Edn::keyword("artist/name")),
            (Edn::keyword("db/valueType"), Edn::keyword("db.type/string")),
            (
                Edn::keyword("db/cardinality"),
                Edn::keyword("db.cardinality/one"),
            ),
            (
                Edn::keyword("db/unique"),
                Edn::keyword("db.unique/identity"),
            ),
        ]),
        Edn::Map(vec![
            (Edn::keyword("db/ident"), Edn::keyword("artist/year")),
            (Edn::keyword("db/valueType"), Edn::keyword("db.type/long")),
            (
                Edn::keyword("db/cardinality"),
                Edn::keyword("db.cardinality/one"),
            ),
        ]),
    ]))
}

fn tx() -> Vec<u8> {
    encode_edn(&Edn::Vector(vec![Edn::Map(vec![
        (Edn::keyword("db/id"), Edn::Str("artist".into())),
        (Edn::keyword("artist/name"), Edn::Str("Portishead".into())),
        (Edn::keyword("artist/year"), Edn::Long(1991)),
    ])]))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mem_backend_runs_the_full_create_transact_read_path() {
    let mut config = NodeConfig::new(std::path::PathBuf::from("/nonexistent-mem-node"));
    config.store = StoreSpec::Memory;
    // A mem node touches no filesystem, so a bogus data_dir is harmless.
    let node = TransactorNode::open(config).await.expect("open mem node");

    assert!(node.create_db("mbrainz", &schema()).await.expect("create"));
    let response = node.transact("mbrainz", &tx()).await.expect("transact");
    assert!(response.basis_t > response.basis_before);

    let db = node.db_state("mbrainz").await.expect("db state").db();
    let query = Edn::Vector(vec![
        Edn::keyword("find"),
        Edn::symbol("?year"),
        Edn::keyword("where"),
        Edn::Vector(vec![
            Edn::symbol("?e"),
            Edn::keyword("artist/name"),
            Edn::Str("Portishead".into()),
        ]),
        Edn::Vector(vec![
            Edn::symbol("?e"),
            Edn::keyword("artist/year"),
            Edn::symbol("?year"),
        ]),
    ]);
    let result = q(&query, &[QInput::Db(&db)]).expect("query");
    assert_eq!(result.to_string(), "[[1991]]");
}
