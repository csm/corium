//! Ignored scaling regression benchmark: per-transaction cost must not grow
//! ~linearly with database size (which is what made bulk loads quadratic).
//! Not run by default — timing-based. Run manually with:
//!   `cargo test -p corium-db --release --test scaling_probe -- --ignored --nocapture`
#![allow(clippy::cast_precision_loss, clippy::cast_possible_wrap)]

use std::time::Instant;

use corium_core::{Cardinality, Datom, EntityId, Partition, Schema, Value, ValueType};
use corium_db::{Db, attribute};

fn attr(id: u64) -> EntityId {
    EntityId::new(Partition::Db as u32, id)
}
fn entity(id: u64) -> EntityId {
    EntityId::new(Partition::User as u32, id)
}
fn tx_entity(t: u64) -> EntityId {
    EntityId::new(Partition::Tx as u32, t)
}

#[test]
#[ignore = "timing-based scaling benchmark; run manually"]
fn per_tx_cost_is_roughly_flat() {
    let mut schema = Schema::default();
    schema.insert(attribute(1, ValueType::Str, Cardinality::One, None));
    schema.insert(attribute(2, ValueType::Long, Cardinality::One, None));

    let mut db = Db::new(schema);
    // Warm the index cache so with_transaction derives incrementally, matching
    // the transactor pipeline where prepare/queries materialize the indexes.
    let _ = db.datoms();

    let total = 40_000u64;
    let bucket = 5_000u64;
    let mut bucket_start = Instant::now();
    let mut first_bucket_ns = 0u128;
    let mut last_bucket_ns = 0u128;

    for i in 0..total {
        // Emulate the pipeline: db_before is retained (report holds it), and a
        // fresh child is derived. Keeping `before` alive is exactly what forced
        // the old Arc<Vec> deep copy.
        let before = db.clone();
        let t = before.basis_t() + 1;
        let datoms = [
            Datom {
                e: entity(i),
                a: attr(1),
                v: Value::Str("x".into()),
                tx: tx_entity(t),
                added: true,
            },
            Datom {
                e: entity(i),
                a: attr(2),
                v: Value::Long(i as i64),
                tx: tx_entity(t),
                added: true,
            },
        ];
        db = before.with_transaction(t, &datoms);
        // Touch the derived indexes as prepare()/queries would.
        let _ = db.values(entity(i), attr(1));

        if (i + 1) % bucket == 0 {
            let elapsed = bucket_start.elapsed().as_nanos();
            let lo = i + 1 - bucket;
            println!(
                "releases {:>6}-{:<6} : {:>8.1} us/tx",
                lo,
                i + 1,
                elapsed as f64 / bucket as f64 / 1000.0
            );
            if lo == 0 {
                first_bucket_ns = elapsed;
            }
            last_bucket_ns = elapsed;
            bucket_start = Instant::now();
        }
    }

    let ratio = last_bucket_ns as f64 / first_bucket_ns as f64;
    println!(
        "last/first bucket ratio = {ratio:.2} (linear-per-tx would be ~{})",
        total / bucket
    );
    // With the old O(N)-per-tx copy the final bucket is ~8x the first here;
    // structural sharing keeps it within a small logarithmic factor.
    assert!(
        ratio < 3.0,
        "per-tx cost grew {ratio:.2}x — superlinear regression"
    );
}
