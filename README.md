# Shopping Sessions (Builders Buildathon, Databricks track)

Team Indus Platonics. A hand-written Rust pipeline that turns the messy
click-stream CSVs in `/data` into the exact `user_daily` and `campaign_daily`
Parquet tables defined in the track's `rules.md`.

## Design

1. **Small tables into direct-indexed arrays.** Products are `p<n>` and
   campaigns `c_<n>`, so lookups index an array by `n` (a free minimal perfect
   hash); any other spelling falls back to a hash map, so lookups stay exact.
2. **Order-preserving packed IDs.** An ID of 1 to 10 chars of `[0-9a-z]` packs
   into a `u64` at 6 bits per char, so integer order equals byte order and
   "byte-wise smallest ID" is an integer compare. Packing is SIMD (rank by
   vector compares) plus two BMI2 `pext`. Other IDs get a flagged hash key plus
   a side registry of their bytes.
3. **Links** run on their own thread, concurrently with parsing: union-find,
   name = smallest ID per component.
4. **Input** is read with `pread` in ~2 MiB chunks on every core. The scanner
   builds comma/quote/newline bitmasks 64 bytes at a time (AVX2), visits only
   commas and newlines, and decides quoting branch-free with `popcnt`.
5. **Partitioning** writes through per-bucket write-combining buffers flushed
   with non-temporal stores into slab buckets; partition counts scale with the
   input so each bucket's working set stays near L2 size.
6. **Dedup** per event bucket: one pass builds `(event_id -> min ingest)` and
   records the losing copies, a second sequential pass skips them.
7. **Sessions:** per person bucket, a counting sort groups records by a dense
   person index (no comparison sort of the bucket), each person's few records
   are sorted by time, sessions are cut at gaps over 30 min, credit uses a
   two-pointer sweep and assists a sliding 7-day campaign-count window.
8. **Parquet:** one `user_daily` file per thread, one `campaign_daily` file.

## Correctness

- `scripts/reference.py` is a slow, literal Python implementation of
  `rules.md`, written separately from the Rust code. It passes the 36 fixtures
  and the published dev answer.
- `scripts/diff_seeds.sh` diffs the solver against it on generator seeds
  (10 seeds checked, including a window containing Feb 29, 2028).
- Unit tests fuzz the SIMD scanner and the SIMD ID packer against their
  scalar versions.

## Sources and credits

Written for this event by our team. Crates: `memchr`, `memmap2`, `libc`,
`bytes`, `parquet` (Apache Arrow Rust); all MIT or Apache-2.0.

Ideas learned from other teams' public write-ups and then implemented our own
way (no code copied):
- Write-combining buffers with non-temporal stores, `read` instead of `mmap`,
  and the dated experiment log as a method: VibDiv (`Vibhor7-7/shopping-sessions`).
- Huge-page partition slabs and running link work concurrently with input:
  `mwkhettab/shopping-sessions-cpp`.
- Background: software write-combining for radix partitioning (Schuh et al.,
  SIGMOD 2016; Balkesen et al., ICDE 2013), simdjson-style structural bitmasks
  (Langdale & Lemire, VLDB J. 2019), and One Billion Row Challenge write-ups.

## Run

    cargo build --release
    DATA_DIR=dev OUTPUT_DIR=out target/release/solver

Released under the MIT License.
