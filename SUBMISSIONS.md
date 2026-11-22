# Submissions (Databricks Shopping Sessions, team Indus Platonics)

Scores are the scorer's time on the hidden ~20M-event practice set. Board
noise between machines is up to ~20%, so compare with care.

| When (ET) | ID | Commit | Score | What changed |
| --- | --- | --- | --- | --- |
| Sat 2:21 AM | BB-CB8C2083 | `38c3a23` | 2.62 s | v1: mmap + chunked parse, 2048 eid buckets, per-bucket sort |
| Sat 2:27 AM | BB-501B4798 | `f209821` | 2.26 s | v2: SIMD scanner, prefetched link lookups, compact records |
| Sat 3:21 AM | BB-E0B60673 | `f16182b` | **1.17 s** | F: huge-page slabs + write-combining NT stores, AVX2 (x86-64-v3), date memo, `_exit`, pread 256 KiB chunks, concurrent links, inline click routing, counting-sort person grouping, PEXT ID packing, branch-free quotes, INT32 counts, adaptive bucket counts |
| Sat 3:28 AM | BB-E47F9E64 | `7cd272d` | 1.33 s | G: F with 4 KiB pages instead of huge pages (huge pages win on the scorer) |
| Sat 4:22 AM | BB-DD347833 | `fec3cdd` | 1.20 s | P: F + AVX-512BW classifier (runtime-dispatched), static binary, presorted-run check (research-brief build) |
| Sat 4:29 AM | BB-BC8D2E27 | `74914a4` | 1.24 s | R2: F + dedup survivors routed by raw user ID, persons resolved per bucket (-4% CPU locally) |
| Sat 5:22 AM | BB-C346EA67 | `ed298e9` | 1.23 s | R2 + profile-guided optimisation (PGO trained in the Docker build) |
| Sat 5:30 AM | BB-A9DDAAE9 | `f16182b` | 1.18 s | F resubmitted unchanged to measure noise: F is genuinely ~1.17-1.18 |

## Lessons

- F re-scored 1.18 s (was 1.17): F's time is real. Builds containing R2's
  extra resolve pass (R2 1.24, R2+PGO 1.23) are ~5% slower on the scorer even
  though R2 used 4% less CPU locally. Build on F, not R2.
- F, P and R2 scored 1.17 / 1.20 / 1.24 s: inside board noise. Changes worth a
  few % locally do not show on the board; only a large change (15%+) can.

- The scorer prefers huge pages (F 1.17 s vs G 1.33 s), unlike this dev VM.
- Teardown after the last stage is ~36 ms with huge pages, ~96 ms without.
- Local timing noise is ±10%; judge local changes by CPU-seconds over
  interleaved runs (`/tmp/bench20.py`), and by the scorer.
- Rows in the input are shuffled, so per-person "presorted run" tricks do not apply.
- Every submission must pass: cargo test, 36 fixtures, dev answer, 10 reference
  seeds (`scripts/diff_seeds.sh`), and a 20M diff against a scored build.

## Sat Nov 21 (late morning)
- BB-FD15B5A2 — F+PGO @ 82847cb — **1.17 s** (ties best F)
- BB-7DBB92F3 — F+static (crt-static, x86-64-v3) @ 03fd731 — **1.18 s** (neutral; within ±20% noise)
- Board: rank ~111/273. Top: 0.30 / 0.31 / 0.31 / 0.42. Top entries decode generator-scrambled IDs to dense indices (authors opted out of reuse; rules forbid dataset-specific hard-coding) — not pursued.
- Next: fuse post-dedup passes (group+sessions+attribution), batched field extraction, minimal Parquet writer.
- BB-DC1C92B8 — hot @ 925e82e: mask-algebra scanner (local bench 42 -> 37 ns/line) — **1.21 s**
- BB-05E28C9C — nt-off @ f94c9ec: same + cached slab stores — **1.22 s**
- Lesson: the local scanner gain did not show on the scorer (slightly worse, ~3%, within drift). Cached vs non-temporal stores: no difference.
- BB-E59E1813 — control rerun of F+PGO @ 82847cb — **1.19 s** (same code scored 1.17 earlier: ~2% drift)
- BB-E0BC035F — hot @ 5b244c4: branch-free time-of-day, SWAR 13-digit epoch, word-compare event type (bench 83 -> 57 ns/line) — **1.08 s** NEW BEST
- BB-C0A67123 — pread-b @ feff65a (writer etc., pread) — 1.0990 s; BB-55421D68 same with mmap reads — 1.1254 s (mmap hurts ~2.5%)
- BB-96A166C4 / BB-21CCABA8 — scorer probes: parse ends at ~0.58 s, write ends at ~1.13 s internal (fixed overhead ~0)
- BB-1DB53ABD — exitrel @ f0d1c7a (parallel slab release before exit) — 1.1017 s (neutral)
- BB-3AA013A0 — X_BITS=10 — 1.0895 s; BB-D39F9DA5 — X_BITS=11 — 2.3090 s (much worse)

## Scorer v5 (Sat ~9 PM): new IDs (per-seed keyed bijection, 1 in 8 with a 32-char hex suffix); board rescored
- BB-11F9334D X_BITS=8 — 2.1489 s; BB-C7B60C15 X_BITS=10 rerun — 2.0129 s (our old code on v5 data: long IDs hit the slow fallback path)
- BB-0A4029F9 — longids @ cd96da9: word hash, no registry for event_id, deduplicated per-thread names — **1.2249 s**
- BB-48FD127F — longids @ 34672a6: allocation-free link-name compare — **1.1767 s**
- BB-7AA8FAEB — longids @ 9538a0f: append-only name log, sorted after parse — **1.1652 s** (54th; #1 is 0.60)

## C++ pipeline (Somesh Agrawal), scorer v5
| Commit | Branch | Change | Score |
|---|---|---|---|
| 1e23e0d | cpp-somesh | baseline, -O3 -march=x86-64-v3, static | **0.7497 s** (best) |
| a0a1ede | cpp-clx | -march=cascadelake | 0.7535 s |
| 1723887 | cpp-unitdist | SS_UNIT=256, SS_DIST=24 | 0.7521 s |
| 8e9be04 | cpp-chunk4m | SS_CHUNK=4 MiB | 0.7838 s |
| 4a9822a | cpp-pgo | GCC 13 profile-guided build | 0.7562 s |
