# Shopping Sessions (Builders Buildathon, Databricks track)

Team Indus Platonics. A hand-written Rust pipeline that turns the messy
click-stream CSVs in `/data` into the exact `user_daily` and `campaign_daily`
Parquet tables defined in the track's `rules.md`.

## Design

1. **Small tables into direct-indexed arrays.** Products are `p<n>` and
   campaigns `c_<n>`, so lookups index an array by `n` (a free minimal perfect
   hash); any other spelling falls back to a hash map, so lookups stay exact.
2. **Order-preserving packed IDs.** An ID of 1 to 12 chars of `[0-9a-z]` packs
   into a `u64` in base 37 (0 = padding), so integer order equals byte order and
   "byte-wise smallest ID" is an integer compare. Other IDs get a flagged hash
   key plus a side registry of their bytes.
3. **Links:** parallel parse, union-find, name = smallest ID per component.
4. **Events and clicks:** memory-mapped, cut into 2 MiB chunks parsed on every
   core. Events are radix-partitioned by event ID into 2048 cache-sized buckets.
5. **Dedup** per bucket with a small open-addressing table (min `ingest_ts`
   wins), then validation of the kept copy; survivors are partitioned by person.
6. **Sessions:** per person bucket, sort by (person, ts), cut at gaps over 30
   min, credit with a two-pointer sweep over the person's clicks, and count
   assists with a sliding 7-day window over a campaign-count set.
7. **Parquet:** one `user_daily` file per thread, one `campaign_daily` file.

## Sources

Written from scratch for this event. Crates used: `memchr`, `memmap2`,
`parquet` (Apache Arrow Rust). Techniques are the well-known ones from the One
Billion Row Challenge (mmap + chunked parallel parsing) and cache-conscious
radix partitioning.

## Run

    cargo build --release
    DATA_DIR=dev OUTPUT_DIR=out target/release/solver

Released under the MIT License.
