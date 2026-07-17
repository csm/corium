//! M3 query benchmarks over a musicbrainz-style dataset
//! (artists → albums → tracks). Tracked per-commit as the regression
//! baseline; see docs/benchmarks/m3-baseline.md.
#![allow(missing_docs)] // criterion macros generate undocumented items

use corium_core::{
    Attribute, Cardinality, Datom, EntityId, Keyword, KeywordInterner, Partition, Schema, Unique,
    Value, ValueType,
};
use corium_db::{Db, Idents};
use corium_query::edn::read_one;
use corium_query::{QInput, QueryCache, pull, q};
use criterion::{Criterion, criterion_group, criterion_main};
use std::hint::black_box;

const ARTIST_NAME: u64 = 100;
const ARTIST_COUNTRY: u64 = 101;
const ALBUM_TITLE: u64 = 102;
const ALBUM_ARTIST: u64 = 103;
const ALBUM_YEAR: u64 = 104;
const TRACK_NAME: u64 = 105;
const TRACK_ALBUM: u64 = 106;
const TRACK_DURATION: u64 = 107;

const ARTISTS: u64 = 100;
const ALBUMS_PER_ARTIST: u64 = 3;
const TRACKS_PER_ALBUM: u64 = 10;

fn attr(n: u64) -> EntityId {
    EntityId::new(Partition::Db as u32, n)
}

fn entity(n: u64) -> EntityId {
    EntityId::new(Partition::User as u32, n)
}

#[allow(clippy::too_many_lines)]
fn dataset() -> Db {
    let mut schema = Schema::default();
    let mut idents = Idents::default();
    let mut install =
        |id: u64, ident: &str, value_type: ValueType, unique: Option<Unique>, indexed: bool| {
            schema.insert(Attribute {
                id: attr(id),
                value_type,
                cardinality: Cardinality::One,
                unique,
                is_component: false,
                indexed,
                no_history: false,
            });
            idents.insert(Keyword::parse(ident), attr(id));
        };
    install(
        ARTIST_NAME,
        "artist/name",
        ValueType::Str,
        Some(Unique::Identity),
        true,
    );
    install(
        ARTIST_COUNTRY,
        "artist/country",
        ValueType::Str,
        None,
        false,
    );
    install(ALBUM_TITLE, "album/title", ValueType::Str, None, false);
    install(ALBUM_ARTIST, "album/artist", ValueType::Ref, None, false);
    install(ALBUM_YEAR, "album/year", ValueType::Long, None, true);
    install(TRACK_NAME, "track/name", ValueType::Str, None, false);
    install(TRACK_ALBUM, "track/album", ValueType::Ref, None, false);
    install(
        TRACK_DURATION,
        "track/duration",
        ValueType::Long,
        None,
        false,
    );

    let tx = EntityId::new(Partition::Tx as u32, 1);
    let mut datoms = Vec::new();
    let mut add = |e: EntityId, a: u64, v: Value| {
        datoms.push(Datom {
            e,
            a: attr(a),
            v,
            tx,
            added: true,
        });
    };
    let mut next = 0_u64;
    let mut alloc = || {
        next += 1;
        entity(next)
    };
    let countries = ["US", "UK", "DE", "JP", "BR"];
    for artist_index in 0..ARTISTS {
        let artist = alloc();
        add(
            artist,
            ARTIST_NAME,
            Value::Str(format!("artist-{artist_index}").into()),
        );
        add(
            artist,
            ARTIST_COUNTRY,
            Value::Str(countries[usize::try_from(artist_index).expect("fits") % 5].into()),
        );
        for album_index in 0..ALBUMS_PER_ARTIST {
            let album = alloc();
            add(
                album,
                ALBUM_TITLE,
                Value::Str(format!("album-{artist_index}-{album_index}").into()),
            );
            add(album, ALBUM_ARTIST, Value::Ref(artist));
            add(
                album,
                ALBUM_YEAR,
                Value::Long(1970 + i64::try_from((artist_index + album_index) % 50).expect("fits")),
            );
            for track_index in 0..TRACKS_PER_ALBUM {
                let track = alloc();
                add(
                    track,
                    TRACK_NAME,
                    Value::Str(format!("track-{artist_index}-{album_index}-{track_index}").into()),
                );
                add(track, TRACK_ALBUM, Value::Ref(album));
                add(
                    track,
                    TRACK_DURATION,
                    Value::Long(120 + i64::try_from(track_index * 17 % 240).expect("fits")),
                );
            }
        }
    }
    let db = Db::new(schema)
        .with_naming(idents, KeywordInterner::default())
        .with_transaction(1, &datoms);
    // Materialize indexes and statistics outside the timed sections.
    let _ = db.datoms();
    let _ = db.planner_stats();
    db
}

fn benches(c: &mut Criterion) {
    let db = dataset();
    let cache = QueryCache::new();

    let point = read_one("[:find ?e . :in $ ?name :where [?e :artist/name ?name]]").expect("edn");
    c.bench_function("point_lookup_unique_attr", |b| {
        let parsed = cache.parse(&point).expect("parse");
        b.iter(|| {
            let inputs = [
                QInput::Db(&db),
                QInput::Edn(read_one("\"artist-42\"").expect("edn")),
            ];
            black_box(
                corium_query::run(&parsed, &inputs, corium_query::ExecOptions::default())
                    .expect("run"),
            )
        });
    });

    let join = read_one(
        "[:find ?track ?duration
          :in $ ?name
          :where [?a :artist/name ?name]
                 [?al :album/artist ?a]
                 [?t :track/album ?al]
                 [?t :track/name ?track]
                 [?t :track/duration ?duration]]",
    )
    .expect("edn");
    c.bench_function("join_heavy_artist_tracks", |b| {
        let parsed = cache.parse(&join).expect("parse");
        b.iter(|| {
            let inputs = [
                QInput::Db(&db),
                QInput::Edn(read_one("\"artist-7\"").expect("edn")),
            ];
            black_box(
                corium_query::run(&parsed, &inputs, corium_query::ExecOptions::default())
                    .expect("run"),
            )
        });
    });

    let aggregate = read_one(
        "[:find ?country (count ?t) (avg ?d)
          :where [?a :artist/country ?country]
                 [?al :album/artist ?a]
                 [?t :track/album ?al]
                 [?t :track/duration ?d]]",
    )
    .expect("edn");
    c.bench_function("aggregate_group_by_country", |b| {
        b.iter(|| black_box(q(&aggregate, &[QInput::Db(&db)]).expect("run")));
    });

    let pattern =
        read_one("[:artist/name {:album/_artist [:album/title {:track/_album [:track/name]}]}]")
            .expect("edn");
    let artist_e = db
        .lookup(attr(ARTIST_NAME), &Value::Str("artist-3".into()))
        .expect("artist");
    c.bench_function("pull_heavy_artist_discography", |b| {
        b.iter(|| black_box(pull(&db, &pattern, artist_e).expect("pull")));
    });

    let as_of = db.as_of(1);
    let _ = as_of.datoms();
    let range =
        read_one("[:find (count ?al) . :where [?al :album/year ?y] [(>= ?y 2000)] [(< ?y 2005)]]")
            .expect("edn");
    c.bench_function("as_of_view_range_count", |b| {
        b.iter(|| black_box(q(&range, &[QInput::Db(&as_of)]).expect("run")));
    });
}

criterion_group!(query_benches, benches);
criterion_main!(query_benches);
