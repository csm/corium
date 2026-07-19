//! `mbrainz-import-json` — stream a filtered subset of the official
//! `MusicBrainz` JSON release dump directly from its `.tar.xz` archive.
//!
//! Only release-level metadata and credited artists are imported. The large
//! nested media, track, recording, relationship, alias, tag, and genre trees
//! are intentionally ignored so a useful decade-sized catalog remains
//! practical on a laptop.

use std::collections::HashSet;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::process::ExitCode;

use clap::Parser;
use corium_mbrainz::endpoints;
use corium_peer::{Admin, ConnectConfig, Connection};
use corium_query::edn::{Edn, read_all};
use serde_json::Value as Json;
use tokio::sync::mpsc;
use xz2::read::XzDecoder;

const RELEASE_MEMBER: &str = "mbdump/release";

/// Stream a year range from the official `MusicBrainz` JSON release dump.
#[derive(Clone, Debug, Parser)]
#[command(name = "mbrainz-import-json", about)]
struct Args {
    /// `MusicBrainz` `release.tar.xz` JSON dump.
    #[arg(long)]
    releases: PathBuf,
    /// First release year to include (inclusive).
    #[arg(long, default_value_t = 1990)]
    from_year: i64,
    /// Last release year to include (inclusive).
    #[arg(long, default_value_t = 1999)]
    to_year: i64,
    /// Maximum selected releases to import (useful for a trial run).
    #[arg(long)]
    limit: Option<u64>,
    /// Release entities per transaction.
    #[arg(long, default_value_t = 1000)]
    batch: usize,
    /// Transactor endpoint (`http://host:port` or `host:port`).
    #[arg(long, default_value = "http://127.0.0.1:4334")]
    transactor: String,
    /// Database name to create and load.
    #[arg(long, default_value = "mbrainz")]
    db: String,
    /// Bearer token, if the transactor requires one.
    #[arg(long)]
    token: Option<String>,
    /// Schema file (a vector of attribute maps).
    #[arg(long, default_value = "examples/musicbrainz/schema.edn")]
    schema: PathBuf,
    /// Skip schema creation (the database already exists).
    #[arg(long)]
    skip_create: bool,
}

#[derive(Debug)]
struct ImportBatch {
    artists: Vec<Edn>,
    releases: Vec<Edn>,
    scanned: u64,
    selected: u64,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct ScanStats {
    scanned: u64,
    selected: u64,
    artists: u64,
}

#[tokio::main]
async fn main() -> ExitCode {
    let _ = rustls::crypto::ring::default_provider().install_default();
    match run(Args::parse()).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(message) => {
            eprintln!("mbrainz-import-json: {message}");
            ExitCode::FAILURE
        }
    }
}

async fn run(args: Args) -> Result<(), String> {
    if args.from_year > args.to_year {
        return Err(format!(
            "--from-year ({}) must not be after --to-year ({})",
            args.from_year, args.to_year
        ));
    }
    if args.limit == Some(0) {
        return Err("--limit must be greater than zero".to_owned());
    }

    let (endpoint, url) = endpoints(&args.transactor, &args.db)?;
    ensure_database(&args, &endpoint).await?;

    let mut config = ConnectConfig::new(&endpoint, &args.db);
    config.token = args.token.clone();
    let connection = Connection::connect(config)
        .await
        .map_err(|error| format!("cannot connect to {url}: {error}"))?;

    let (sender, mut receiver) = mpsc::channel::<ImportBatch>(2);
    let scan_args = args.clone();
    let producer = tokio::task::spawn_blocking(move || {
        scan_archive(&scan_args, |batch| {
            sender
                .blocking_send(batch)
                .map_err(|_| "database importer stopped before archive scan completed".to_owned())
        })
    });

    let mut transactions = 0_u64;
    let mut transaction_error = None;
    while let Some(batch) = receiver.recv().await {
        match transact_batch(&connection, batch.artists, batch.releases, args.batch).await {
            Ok(committed) => transactions += committed,
            Err(error) => {
                transaction_error = Some(error);
                break;
            }
        }
        println!(
            "… scanned {} releases; selected {}; {} transactions",
            batch.scanned, batch.selected, transactions
        );
    }
    drop(receiver);

    let scan_result = producer
        .await
        .map_err(|error| format!("archive scanner task failed: {error}"))?;
    if let Some(error) = transaction_error {
        return Err(error);
    }
    let stats = scan_result?;

    let db = connection
        .sync()
        .await
        .map_err(|error| format!("final sync failed: {error}"))?;
    println!(
        "imported {} releases and {} credited artists after scanning {} records in {transactions} transactions; database basis-t is {}",
        stats.selected,
        stats.artists,
        stats.scanned,
        db.basis_t()
    );
    Ok(())
}

async fn ensure_database(args: &Args, endpoint: &str) -> Result<(), String> {
    if args.skip_create {
        println!("skipping schema creation for {:?}", args.db);
        return Ok(());
    }

    let schema_text = std::fs::read_to_string(&args.schema)
        .map_err(|error| format!("cannot read schema {}: {error}", args.schema.display()))?;
    let schema = read_all(&schema_text)
        .map_err(|error| format!("bad schema EDN: {error}"))?
        .into_iter()
        .flat_map(flatten_form)
        .collect::<Vec<_>>();
    let mut admin = Admin::connect(endpoint, args.token.clone(), None)
        .await
        .map_err(|error| format!("cannot reach transactor at {endpoint}: {error}"))?;
    let created = admin
        .create_database(&args.db, &schema)
        .await
        .map_err(|error| format!("create database failed: {error}"))?;
    println!(
        "database {:?}: {} ({} schema attributes)",
        args.db,
        if created {
            "created"
        } else {
            "already existed"
        },
        schema.len()
    );
    Ok(())
}

fn flatten_form(form: Edn) -> Vec<Edn> {
    match form {
        Edn::Vector(items) | Edn::List(items) => items,
        other => vec![other],
    }
}

async fn transact_batch(
    connection: &Connection,
    artists: Vec<Edn>,
    releases: Vec<Edn>,
    batch_size: usize,
) -> Result<u64, String> {
    let mut transactions = 0;
    for chunk in artists.chunks(batch_size.max(1)) {
        connection
            .transact(chunk.to_vec())
            .await
            .map_err(|error| format!("artist transaction failed: {error}"))?;
        transactions += 1;
    }
    connection
        .transact(releases)
        .await
        .map_err(|error| format!("release transaction failed: {error}"))?;
    Ok(transactions + 1)
}

fn scan_archive(
    args: &Args,
    emit: impl FnMut(ImportBatch) -> Result<(), String>,
) -> Result<ScanStats, String> {
    let file = File::open(&args.releases)
        .map_err(|error| format!("cannot open {}: {error}", args.releases.display()))?;
    let decoder = XzDecoder::new(BufReader::with_capacity(1 << 20, file));
    let mut archive = tar::Archive::new(decoder);
    let entries = archive
        .entries()
        .map_err(|error| format!("cannot read tar archive: {error}"))?;
    for entry in entries {
        let entry = entry.map_err(|error| format!("cannot read tar entry: {error}"))?;
        let path = entry
            .path()
            .map_err(|error| format!("cannot read tar entry path: {error}"))?;
        if path.as_ref() == Path::new(RELEASE_MEMBER) {
            return scan_release_lines(BufReader::with_capacity(1 << 20, entry), args, emit);
        }
    }
    Err(format!(
        "archive {} has no {RELEASE_MEMBER} member",
        args.releases.display()
    ))
}

fn scan_release_lines<R: BufRead>(
    mut reader: R,
    args: &Args,
    mut emit: impl FnMut(ImportBatch) -> Result<(), String>,
) -> Result<ScanStats, String> {
    let batch_size = args.batch.max(1);
    let mut stats = ScanStats::default();
    let mut seen_artists = HashSet::<u128>::new();
    let mut artists = Vec::new();
    let mut releases = Vec::with_capacity(batch_size);
    let mut line = String::new();

    loop {
        line.clear();
        let bytes = reader
            .read_line(&mut line)
            .map_err(|error| format!("reading {RELEASE_MEMBER}: {error}"))?;
        if bytes == 0 {
            break;
        }
        stats.scanned += 1;
        let json: Json = serde_json::from_str(&line).map_err(|error| {
            format!(
                "bad JSON at {RELEASE_MEMBER} line {}: {error}",
                stats.scanned
            )
        })?;
        let Some(import) = transform_release(&json, args.from_year, args.to_year)? else {
            continue;
        };
        for artist in import.artists {
            if seen_artists.insert(artist.id) {
                artists.push(artist.entity);
                stats.artists += 1;
            }
        }
        releases.push(import.release);
        stats.selected += 1;

        if releases.len() >= batch_size {
            emit(ImportBatch {
                artists: std::mem::take(&mut artists),
                releases: std::mem::replace(&mut releases, Vec::with_capacity(batch_size)),
                scanned: stats.scanned,
                selected: stats.selected,
            })?;
        }
        if args.limit.is_some_and(|limit| stats.selected >= limit) {
            break;
        }
    }

    if !releases.is_empty() {
        emit(ImportBatch {
            artists,
            releases,
            scanned: stats.scanned,
            selected: stats.selected,
        })?;
    }
    Ok(stats)
}

struct ReleaseImport {
    artists: Vec<ArtistImport>,
    release: Edn,
}

struct ArtistImport {
    id: u128,
    entity: Edn,
}

fn transform_release(
    json: &Json,
    from_year: i64,
    to_year: i64,
) -> Result<Option<ReleaseImport>, String> {
    let Some(year) = json.get("date").and_then(Json::as_str).and_then(year_of) else {
        return Ok(None);
    };
    if !(from_year..=to_year).contains(&year) {
        return Ok(None);
    }

    let release_id = required_mbid(json, "release")?;
    let title = required_string(json, "title", "release")?;
    let credits = json
        .get("artist-credit")
        .and_then(Json::as_array)
        .map_or(&[][..], Vec::as_slice);

    let mut artists = Vec::new();
    let mut artist_refs = Vec::new();
    let mut release_artist_ids = HashSet::new();
    for credit in credits {
        let Some(artist) = credit.get("artist") else {
            continue;
        };
        let artist_id = required_mbid(artist, "credited artist")?;
        if release_artist_ids.insert(artist_id) {
            artist_refs.push(lookup_ref("artist/gid", artist_id));
            artists.push(ArtistImport {
                id: artist_id,
                entity: artist_entity(artist, artist_id)?,
            });
        }
    }

    let mut pairs = vec![
        pair("db/id", Edn::Str(format!("release-{release_id:032x}"))),
        pair("release/gid", uuid(release_id)),
        pair("release/name", Edn::Str(title.to_owned())),
        pair("release/year", Edn::Long(year)),
    ];
    if !artist_refs.is_empty() {
        pairs.push(pair("release/artists", Edn::Vector(artist_refs)));
    }
    let artist_credit = credits
        .iter()
        .filter_map(|credit| {
            credit.get("name").and_then(Json::as_str).map(|name| {
                format!(
                    "{}{}",
                    name,
                    credit
                        .get("joinphrase")
                        .and_then(Json::as_str)
                        .unwrap_or_default()
                )
            })
        })
        .collect::<String>();
    push_string(&mut pairs, "release/artistCredit", &artist_credit);
    push_enum(
        &mut pairs,
        "release/status",
        "release.status",
        json.get("status"),
    );
    push_enum(
        &mut pairs,
        "release/type",
        "release.type",
        json.pointer("/release-group/primary-type"),
    );
    push_keyword_raw(
        &mut pairs,
        "release/country",
        "country",
        json.get("country"),
    );
    push_keyword_raw(
        &mut pairs,
        "release/language",
        "language",
        json.pointer("/text-representation/language"),
    );

    Ok(Some(ReleaseImport {
        artists,
        release: entity_map(pairs),
    }))
}

fn artist_entity(json: &Json, id: u128) -> Result<Edn, String> {
    let name = required_string(json, "name", "credited artist")?;
    let mut pairs = vec![
        pair("db/id", Edn::Str(format!("artist-{id:032x}"))),
        pair("artist/gid", uuid(id)),
        pair("artist/name", Edn::Str(name.to_owned())),
    ];
    if let Some(sort_name) = json.get("sort-name").and_then(Json::as_str) {
        push_string(&mut pairs, "artist/sortName", sort_name);
    }
    push_enum(&mut pairs, "artist/type", "artist.type", json.get("type"));
    push_enum(
        &mut pairs,
        "artist/gender",
        "artist.gender",
        json.get("gender"),
    );
    push_keyword_raw(&mut pairs, "artist/country", "country", json.get("country"));
    if let Some(year) = json
        .pointer("/life-span/begin")
        .and_then(Json::as_str)
        .and_then(year_of)
    {
        pairs.push(pair("artist/startYear", Edn::Long(year)));
    }
    if let Some(year) = json
        .pointer("/life-span/end")
        .and_then(Json::as_str)
        .and_then(year_of)
    {
        pairs.push(pair("artist/endYear", Edn::Long(year)));
    }
    Ok(entity_map(pairs))
}

fn required_mbid(json: &Json, context: &str) -> Result<u128, String> {
    let id = required_string(json, "id", context)?;
    let compact = id.replace('-', "");
    if compact.len() != 32 {
        return Err(format!("{context} has invalid MBID {id:?}"));
    }
    u128::from_str_radix(&compact, 16).map_err(|_| format!("{context} has invalid MBID {id:?}"))
}

fn required_string<'a>(json: &'a Json, field: &str, context: &str) -> Result<&'a str, String> {
    json.get(field)
        .and_then(Json::as_str)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| format!("{context} has no {field:?} string"))
}

fn year_of(date: &str) -> Option<i64> {
    let year = date.get(..4)?;
    year.bytes()
        .all(|byte| byte.is_ascii_digit())
        .then(|| year.parse().ok())
        .flatten()
}

fn normalize_enum(value: &str) -> String {
    value
        .split(|character: char| !character.is_ascii_alphanumeric())
        .filter(|part| !part.is_empty())
        .map(str::to_ascii_lowercase)
        .collect::<Vec<_>>()
        .join("-")
}

fn push_string(pairs: &mut Vec<(Edn, Edn)>, attribute: &str, value: &str) {
    if !value.is_empty() {
        pairs.push(pair(attribute, Edn::Str(value.to_owned())));
    }
}

fn push_enum(pairs: &mut Vec<(Edn, Edn)>, attribute: &str, namespace: &str, value: Option<&Json>) {
    let Some(value) = value.and_then(Json::as_str) else {
        return;
    };
    let normalized = normalize_enum(value);
    if !normalized.is_empty() {
        pairs.push(pair(
            attribute,
            Edn::keyword(&format!("{namespace}/{normalized}")),
        ));
    }
}

fn push_keyword_raw(
    pairs: &mut Vec<(Edn, Edn)>,
    attribute: &str,
    namespace: &str,
    value: Option<&Json>,
) {
    let Some(value) = value
        .and_then(Json::as_str)
        .filter(|value| !value.is_empty())
    else {
        return;
    };
    pairs.push(pair(
        attribute,
        Edn::keyword(&format!("{namespace}/{value}")),
    ));
}

fn pair(attribute: &str, value: Edn) -> (Edn, Edn) {
    (Edn::keyword(attribute), value)
}

fn entity_map(mut pairs: Vec<(Edn, Edn)>) -> Edn {
    pairs.sort_by(|left, right| left.0.cmp(&right.0));
    Edn::Map(pairs)
}

fn uuid(id: u128) -> Edn {
    Edn::Tagged("uuid".to_owned(), Box::new(Edn::Str(format!("{id:032x}"))))
}

fn lookup_ref(attribute: &str, id: u128) -> Edn {
    Edn::Vector(vec![Edn::keyword(attribute), uuid(id)])
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;
    use tar::{Builder, Header};
    use xz2::write::XzEncoder;

    const IN_RANGE: &str = r#"{
      "id":"2c4b28c2-30a0-4f89-86e8-a515c5fef512",
      "title":"Carboni",
      "date":"1992",
      "country":"IT",
      "status":"Official",
      "text-representation":{"language":"ita"},
      "release-group":{"primary-type":"Album"},
      "artist-credit":[
        {"name":"Luca Carboni","joinphrase":"", "artist":{
          "id":"69a5a5ae-d8b0-44e5-94ef-8973b374da84",
          "name":"Luca Carboni","sort-name":"Carboni, Luca",
          "type":"Person","country":"IT"
        }}
      ]
    }"#;

    fn args() -> Args {
        Args {
            releases: PathBuf::from("unused.tar.xz"),
            from_year: 1990,
            to_year: 1999,
            limit: None,
            batch: 2,
            transactor: "localhost:4334".to_owned(),
            db: "mbrainz".to_owned(),
            token: None,
            schema: PathBuf::from("schema.edn"),
            skip_create: true,
        }
    }

    fn record() -> String {
        let json: Json = serde_json::from_str(IN_RANGE).expect("JSON");
        serde_json::to_string(&json).expect("compact JSON")
    }

    #[test]
    fn transforms_release_and_credited_artist() {
        let json: Json = serde_json::from_str(IN_RANGE).expect("JSON");
        let import = transform_release(&json, 1990, 1999)
            .expect("transform")
            .expect("selected");
        assert_eq!(import.artists.len(), 1);
        assert!(import.release.to_string().contains(":release/year 1992"));
        assert!(
            import
                .release
                .to_string()
                .contains(":release.status/official")
        );
        assert!(
            import.artists[0]
                .entity
                .to_string()
                .contains(":artist/name \"Luca Carboni\"")
        );
    }

    #[test]
    fn excludes_missing_and_out_of_range_dates() {
        let mut json: Json = serde_json::from_str(IN_RANGE).expect("JSON");
        json["date"] = Json::String("2000-01-01".to_owned());
        assert!(
            transform_release(&json, 1990, 1999)
                .expect("transform")
                .is_none()
        );
        json["date"] = Json::Null;
        assert!(
            transform_release(&json, 1990, 1999)
                .expect("transform")
                .is_none()
        );
    }

    #[test]
    fn batches_lines_and_deduplicates_artists() {
        let record = record();
        let input = format!("{record}\n{record}\n");
        let mut batches = Vec::new();
        let stats = scan_release_lines(Cursor::new(input), &args(), |batch| {
            batches.push(batch);
            Ok(())
        })
        .expect("scan");
        assert_eq!(
            stats,
            ScanStats {
                scanned: 2,
                selected: 2,
                artists: 1
            }
        );
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].artists.len(), 1);
        assert_eq!(batches[0].releases.len(), 2);
    }

    #[test]
    fn stops_after_limit() {
        let record = record();
        let input = format!("{record}\n{record}\n");
        let mut limited = args();
        limited.limit = Some(1);
        let stats = scan_release_lines(Cursor::new(input), &limited, |_| Ok(())).expect("scan");
        assert_eq!(stats.scanned, 1);
        assert_eq!(stats.selected, 1);
    }

    #[test]
    fn scans_release_member_inside_xz_archive() {
        let file = tempfile::NamedTempFile::new().expect("temporary archive");
        let encoder = XzEncoder::new(file.reopen().expect("reopen archive"), 1);
        let mut archive = Builder::new(encoder);

        let input = format!("{}\n", record());
        let mut header = Header::new_gnu();
        header.set_size(u64::try_from(input.len()).expect("fixture length"));
        header.set_mode(0o644);
        header.set_cksum();
        archive
            .append_data(&mut header, RELEASE_MEMBER, input.as_bytes())
            .expect("append release member");
        archive
            .into_inner()
            .expect("finish tar")
            .finish()
            .expect("finish xz");

        let mut archive_args = args();
        archive_args.releases = file.path().to_owned();
        let mut batches = Vec::new();
        let stats = scan_archive(&archive_args, |batch| {
            batches.push(batch);
            Ok(())
        })
        .expect("scan archive");
        assert_eq!(stats.selected, 1);
        assert_eq!(stats.artists, 1);
        assert_eq!(batches.len(), 1);
    }
}
