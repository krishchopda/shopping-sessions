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

## Lessons

- F, P and R2 scored 1.17 / 1.20 / 1.24 s: inside board noise. Changes worth a
  few % locally do not show on the board; only a large change (15%+) can.

- The scorer prefers huge pages (F 1.17 s vs G 1.33 s), unlike this dev VM.
- Teardown after the last stage is ~36 ms with huge pages, ~96 ms without.
- Local timing noise is ±10%; judge local changes by CPU-seconds over
  interleaved runs (`/tmp/bench20.py`), and by the scorer.
- Rows in the input are shuffled, so per-person "presorted run" tricks do not apply.
- Every submission must pass: cargo test, 36 fixtures, dev answer, 10 reference
  seeds (`scripts/diff_seeds.sh`), and a 20M diff against a scored build.
