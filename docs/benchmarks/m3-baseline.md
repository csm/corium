# M3 benchmark baseline

Recorded from `cargo bench -p corium-query` (criterion, default profile) at
milestone M3 completion. Dataset: musicbrainz-style graph of 100 artists ×
3 albums × 10 tracks (3,400 entities, ~10,400 datoms), built once with
indexes and statistics materialized before timing.

Hardware note: numbers are from the CI-class container that produced the
milestone; they are a *relative* regression baseline, not absolute targets.
Re-record on hardware changes.

| Benchmark | Median | What it measures |
|---|---|---|
| `point_lookup_unique_attr` | 1.85 µs | entity by unique attribute (AVET prefix), cached parse |
| `join_heavy_artist_tracks` | 88.2 µs | 4-clause join artist → albums → tracks (VAET reverse-ref prefixes) |
| `aggregate_group_by_country` | 6.8 ms | whole-dataset 3-hop join with grouping (count + avg per country) |
| `pull_heavy_artist_discography` | 19.4 µs | nested pull with two reverse-ref levels |
| `as_of_view_range_count` | 164 µs | AVET value-range predicate count on an as-of view |

Observations at baseline:

- Point lookup meets the "sub-ms warm" performance posture from
  query-engine.md by ~3 orders of magnitude.
- The join and aggregate suites are dominated by per-frame `BTreeMap`
  cloning in the executor; batch/slot-based frames are the obvious next
  optimization if these regress into requirements.
- Adding the two-component VAET prefix (bound attribute + bound ref value)
  during this milestone took `join_heavy_artist_tracks` from 2.1 ms to
  88 µs and `aggregate_group_by_country` from ~210 ms to 6.8 ms.
