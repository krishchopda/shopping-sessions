//! Shopping Sessions pipeline.
//!
//! Stages:
//!  1. Small tables (manifest, products, campaigns) into direct-indexed arrays.
//!  2. Links: parallel parse, union-find, person name = byte-wise smallest ID.
//!  3. Events + clicks: parallel parse of mmapped chunks. Events are radix-
//!     partitioned by event_id into cache-sized buckets; clicks by person.
//!  4. Dedup per event bucket (small open-addressing table), validate the
//!     kept copy, re-partition survivors by person.
//!  5. Per person bucket: sort, sessionize, credit + assists (sliding window).
//!  6. Parquet out: one user_daily file per thread, one campaign_daily file.
//!
//! IDs are packed into a u64 whose integer order equals byte order when the ID
//! is 1..=12 chars of [0-9a-z] (base 37, 0 = padding). Anything else gets a
//! FALLBACK-flagged hash key and its bytes are kept in a side registry.

use memchr::memchr;
use memmap2::Mmap;
use std::collections::HashMap;
use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

const DAY: i64 = 86_400_000;
const WEEK: i64 = 7 * DAY;
const GAP: i64 = 1_800_000;
const FALLBACK: u64 = 1 << 63;
const CHUNK: usize = 2 << 20;
const EV_BITS: u32 = 8; // 256 event buckets
const PB_BITS: u32 = 9; // 512 person buckets

// ---------------------------------------------------------------- keys

#[inline(always)]
fn rank(c: u8) -> u64 {
    match c {
        b'0'..=b'9' => (c - b'0' + 1) as u64,
        b'a'..=b'z' => (c - b'a' + 11) as u64,
        _ => 0,
    }
}

#[inline(always)]
fn pack(s: &[u8]) -> Option<u64> {
    if s.is_empty() || s.len() > 12 {
        return None;
    }
    let mut v = 0u64;
    for &c in s {
        let r = rank(c);
        if r == 0 {
            return None;
        }
        v = v * 37 + r;
    }
    for _ in s.len()..12 {
        v *= 37;
    }
    Some(v)
}

fn unpack(mut v: u64, out: &mut Vec<u8>) {
    let mut buf = [0u8; 12];
    for i in (0..12).rev() {
        buf[i] = (v % 37) as u8;
        v /= 37;
    }
    for &r in &buf {
        if r == 0 {
            break;
        }
        out.push(if r <= 10 { b'0' + r - 1 } else { b'a' + r - 11 });
    }
}

#[inline(always)]
fn hash_bytes(s: &[u8]) -> u64 {
    let mut h: u64 = 0xcbf29ce484222325;
    for &c in s {
        h ^= c as u64;
        h = h.wrapping_mul(0x100000001b3);
    }
    h ^= h >> 33;
    h = h.wrapping_mul(0xff51afd7ed558ccd);
    h ^= h >> 33;
    h
}

type Reg = Vec<(u64, Vec<u8>)>;

#[inline(always)]
fn key_of(s: &[u8], reg: &mut Reg) -> u64 {
    match pack(s) {
        Some(k) => k,
        None => {
            let k = FALLBACK | (hash_bytes(s) >> 1);
            reg.push((k, s.to_vec()));
            k
        }
    }
}

#[inline(always)]
fn mix(k: u64) -> u64 {
    k.wrapping_mul(0x9E37_79B9_7F4A_7C15)
}

fn bytes_of(k: u64, reg: &HashMap<u64, Vec<u8>>) -> Vec<u8> {
    if k & FALLBACK == 0 {
        let mut v = Vec::with_capacity(12);
        unpack(k, &mut v);
        v
    } else {
        reg.get(&k).cloned().unwrap_or_default()
    }
}

#[inline(always)]
fn key_less(a: u64, b: u64, reg: &HashMap<u64, Vec<u8>>) -> bool {
    if (a | b) & FALLBACK == 0 {
        a < b
    } else {
        bytes_of(a, reg) < bytes_of(b, reg)
    }
}

// ---------------------------------------------------------------- csv

/// Split a line into exactly N fields per the dialect. false = malformed.
#[inline(always)]
fn split<'a, const N: usize>(line: &'a [u8], out: &mut [&'a [u8]; N]) -> bool {
    let len = line.len();
    if len == 0 {
        return false;
    }
    let mut n = 0;
    let mut start = 0;
    loop {
        if n >= N {
            return false;
        }
        let end;
        if start < len && line[start] == b'"' {
            let q = match memchr(b'"', &line[start + 1..]) {
                Some(q) => start + 1 + q,
                None => return false,
            };
            out[n] = &line[start + 1..q];
            end = q + 1;
            if end < len && line[end] != b',' {
                return false;
            }
        } else {
            let mut j = start;
            while j < len {
                let c = line[j];
                if c == b',' {
                    break;
                }
                if c == b'"' {
                    return false;
                }
                j += 1;
            }
            out[n] = &line[start..j];
            end = j;
        }
        n += 1;
        if end >= len {
            break;
        }
        start = end + 1;
        if start == len {
            if n >= N {
                return false;
            }
            out[n] = &line[len..];
            n += 1;
            break;
        }
    }
    n == N
}

// ---------------------------------------------------------------- time

#[inline(always)]
fn dig2(b: &[u8]) -> Option<i64> {
    let (a, c) = (b[0].wrapping_sub(b'0'), b[1].wrapping_sub(b'0'));
    if a < 10 && c < 10 {
        Some((a * 10 + c) as i64)
    } else {
        None
    }
}

#[inline(always)]
fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let doy = (153 * ((m + 9) % 12) + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

#[inline(always)]
fn civil(s: &[u8]) -> Option<i64> {
    if s[4] != b'-' || s[7] != b'-' || s[10] != b'T' || s[13] != b':' || s[16] != b':' {
        return None;
    }
    let y = dig2(&s[0..2])? * 100 + dig2(&s[2..4])?;
    let mo = dig2(&s[5..7])?;
    let d = dig2(&s[8..10])?;
    let h = dig2(&s[11..13])?;
    let mi = dig2(&s[14..16])?;
    let se = dig2(&s[17..19])?;
    if !(1..=12).contains(&mo) || d < 1 || h > 23 || mi > 59 || se > 59 {
        return None;
    }
    let leap = y % 4 == 0 && (y % 100 != 0 || y % 400 == 0);
    let dim = match mo {
        2 => {
            if leap {
                29
            } else {
                28
            }
        }
        4 | 6 | 9 | 11 => 30,
        _ => 31,
    };
    if d > dim {
        return None;
    }
    Some((days_from_civil(y, mo, d) * 86_400 + h * 3600 + mi * 60 + se) * 1000)
}

/// Parse a ts; None = bad. An F3 too big for i64 comes back as i64::MAX (outside every window).
#[inline(always)]
fn parse_ts(s: &[u8]) -> Option<i64> {
    let n = s.len();
    if n == 0 {
        return None;
    }
    if n >= 20 && s[4] == b'-' {
        if n == 20 {
            if s[19] != b'Z' {
                return None;
            }
            return civil(s);
        }
        if n == 25 {
            let sign = match s[19] {
                b'+' => 1,
                b'-' => -1,
                _ => return None,
            };
            if s[22] != b':' {
                return None;
            }
            let oh = dig2(&s[20..22])?;
            let om = dig2(&s[23..25])?;
            if oh > 23 || om > 59 {
                return None;
            }
            return Some(civil(s)? - sign * (oh * 60 + om) * 60_000);
        }
        return None;
    }
    let mut v: i64 = 0;
    let mut over = false;
    for &c in s {
        let d = c.wrapping_sub(b'0');
        if d >= 10 {
            return None;
        }
        if !over {
            match v.checked_mul(10).and_then(|x| x.checked_add(d as i64)) {
                Some(x) => v = x,
                None => over = true,
            }
        }
    }
    Some(if over { i64::MAX } else { v })
}

#[inline(always)]
fn parse_price(s: &[u8]) -> Option<u32> {
    if s.is_empty() {
        return None;
    }
    let mut v: u64 = 0;
    for &c in s {
        let d = c.wrapping_sub(b'0');
        if d >= 10 {
            return None;
        }
        v = v * 10 + d as u64;
        if v > 10_000_000 {
            v = 10_000_001;
        }
    }
    if (1..=10_000_000).contains(&v) {
        Some(v as u32)
    } else {
        None
    }
}

// ---------------------------------------------------------------- lookups

/// `<prefix><decimal without leading zero>` -> number.
#[inline(always)]
fn canon_num(s: &[u8], prefix: &[u8]) -> Option<usize> {
    if s.len() <= prefix.len() || &s[..prefix.len()] != prefix || s.len() > prefix.len() + 9 {
        return None;
    }
    let d = &s[prefix.len()..];
    if d[0] == b'0' {
        return None;
    }
    let mut v = 0usize;
    for &c in d {
        let x = c.wrapping_sub(b'0');
        if x >= 10 {
            return None;
        }
        v = v * 10 + x as usize;
    }
    Some(v)
}

/// Direct-indexed table for `<prefix><n>` keys (a free minimal perfect hash),
/// with a HashMap for anything not in that shape.
struct Lookup {
    prefix: &'static [u8],
    arr: Vec<u32>,
    other: HashMap<Vec<u8>, u32>,
}

impl Lookup {
    fn new(prefix: &'static [u8], items: &[(Vec<u8>, u32)]) -> Self {
        let mut max = 0;
        for (k, _) in items {
            if let Some(n) = canon_num(k, prefix) {
                if n < (1 << 24) {
                    max = max.max(n + 1);
                }
            }
        }
        let mut arr = vec![0u32; max];
        let mut other = HashMap::new();
        for (k, v) in items {
            match canon_num(k, prefix) {
                Some(n) if n < (1 << 24) => arr[n] = v + 1,
                _ => {
                    other.insert(k.clone(), *v);
                }
            }
        }
        Lookup { prefix, arr, other }
    }
    #[inline(always)]
    fn get(&self, s: &[u8]) -> Option<u32> {
        match canon_num(s, self.prefix) {
            Some(n) if n < (1 << 24) => match self.arr.get(n) {
                Some(&x) if x != 0 => Some(x - 1),
                _ => None,
            },
            _ => self.other.get(s).copied(),
        }
    }
}

/// Open-addressing u64 -> u64 map, keys != u64::MAX; key and value share a cache line.
struct U64Map {
    mask: usize,
    e: Vec<[u64; 2]>,
}
impl U64Map {
    fn with_capacity(n: usize) -> Self {
        let cap = (n * 2).next_power_of_two().max(16);
        U64Map { mask: cap - 1, e: vec![[u64::MAX, 0]; cap] }
    }
    #[inline(always)]
    fn start(&self, k: u64) -> usize {
        (mix(k) >> 20) as usize & self.mask
    }
    #[inline(always)]
    fn prefetch(&self, k: u64) {
        #[cfg(target_arch = "x86_64")]
        unsafe {
            use std::arch::x86_64::*;
            _mm_prefetch(self.e.as_ptr().add(self.start(k)) as *const i8, _MM_HINT_T0);
        }
    }
    #[inline(always)]
    fn slot(&self, k: u64) -> usize {
        let mut i = self.start(k);
        loop {
            let x = self.e[i][0];
            if x == k || x == u64::MAX {
                return i;
            }
            i = (i + 1) & self.mask;
        }
    }
    fn insert(&mut self, k: u64, v: u64) {
        let i = self.slot(k);
        self.e[i] = [k, v];
    }
    #[inline(always)]
    fn get(&self, k: u64) -> Option<u64> {
        let i = self.slot(k);
        if self.e[i][0] == k {
            Some(self.e[i][1])
        } else {
            None
        }
    }
}

// ---------------------------------------------------------------- records

#[derive(Clone, Copy)]
struct Ev {
    eid: u64,
    person: u64,
    ts: u32,     // ms since window_start (valid events only)
    ingest: i32, // whole seconds relative to window_start
    price: u32,
    cat: u16,
    flags: u8, // bit0 valid, bit1 purchase
}

#[derive(Clone, Copy)]
struct Sv {
    person: u64,
    ts: i64,
    price: u32,
    cat: u16,
    purchase: bool,
}

#[derive(Clone, Copy)]
struct Ck {
    person: u64,
    ts: i64,
    cid: u64,
    camp: u32,
}

// ---------------------------------------------------------------- files

struct Chunk {
    file: usize,
    start: usize,
    end: usize,
}

fn list_csv(dir: &Path) -> Vec<PathBuf> {
    let mut v: Vec<PathBuf> = match fs::read_dir(dir) {
        Ok(rd) => rd
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .filter(|p| p.extension().map(|x| x == "csv").unwrap_or(false))
            .collect(),
        Err(_) => vec![],
    };
    v.sort();
    v
}

fn map_files(paths: &[PathBuf]) -> Vec<Mmap> {
    paths
        .iter()
        .filter_map(|p| {
            let f = File::open(p).expect("open");
            if f.metadata().map(|m| m.len()).unwrap_or(0) == 0 {
                return None;
            }
            Some(unsafe { Mmap::map(&f).expect("mmap") })
        })
        .collect()
}

fn chunks(maps: &[Mmap]) -> Vec<Chunk> {
    let mut out = vec![];
    for (fi, m) in maps.iter().enumerate() {
        let len = m.len();
        let body = match memchr(b'\n', m) {
            Some(i) => i + 1,
            None => len,
        };
        let mut s = body;
        while s < len {
            let mut e = (s + CHUNK).min(len);
            if e < len {
                e = match memchr(b'\n', &m[e..]) {
                    Some(i) => e + i + 1,
                    None => len,
                };
            }
            out.push(Chunk { file: fi, start: s, end: e });
            s = e;
        }
    }
    out
}

#[inline(always)]
fn for_lines(data: &[u8], mut f: impl FnMut(&[u8])) {
    let mut p = 0;
    let len = data.len();
    while p < len {
        let nl = match memchr(b'\n', &data[p..]) {
            Some(i) => p + i,
            None => len,
        };
        f(&data[p..nl]);
        p = nl + 1;
    }
}

fn read_small(path: &Path) -> Vec<(Vec<u8>, Vec<u8>)> {
    let data = fs::read(path).unwrap_or_default();
    let body = match memchr(b'\n', &data) {
        Some(i) => &data[i + 1..],
        None => &data[data.len()..],
    };
    let mut out = vec![];
    for_lines(body, |line| {
        let mut f: [&[u8]; 2] = [&[]; 2];
        if split(line, &mut f) && !f[0].is_empty() {
            out.push((f[0].to_vec(), f[1].to_vec()));
        }
    });
    out
}

fn manifest_ts(text: &str, key: &str) -> i64 {
    let i = text.find(key).expect("manifest key") + key.len();
    let rest = &text[i..];
    let rest = &rest[rest.find(':').unwrap() + 1..];
    let rest = &rest[rest.find('"').unwrap() + 1..];
    let q = rest.find('"').unwrap();
    parse_ts(rest[..q].as_bytes()).expect("manifest ts")
}

/// Structural bitmasks (comma, quote, newline) for 64 bytes.
#[inline(always)]
fn masks64(p: &[u8; 64]) -> (u64, u64, u64) {
    #[cfg(target_arch = "x86_64")]
    unsafe {
        use std::arch::x86_64::*;
        let (cm, qm, nm) = (_mm_set1_epi8(b',' as i8), _mm_set1_epi8(b'"' as i8), _mm_set1_epi8(b'\n' as i8));
        let mut c = 0u64;
        let mut q = 0u64;
        let mut n = 0u64;
        for k in 0..4 {
            let v = _mm_loadu_si128(p.as_ptr().add(k * 16) as *const __m128i);
            c |= (_mm_movemask_epi8(_mm_cmpeq_epi8(v, cm)) as u32 as u64) << (k * 16);
            q |= (_mm_movemask_epi8(_mm_cmpeq_epi8(v, qm)) as u32 as u64) << (k * 16);
            n |= (_mm_movemask_epi8(_mm_cmpeq_epi8(v, nm)) as u32 as u64) << (k * 16);
        }
        (c, q, n)
    }
    #[cfg(not(target_arch = "x86_64"))]
    {
        let (mut c, mut q, mut n) = (0u64, 0u64, 0u64);
        for (i, &b) in p.iter().enumerate() {
            c |= ((b == b',') as u64) << i;
            q |= ((b == b'"') as u64) << i;
            n |= ((b == b'\n') as u64) << i;
        }
        (c, q, n)
    }
}

/// Visit every well-formed line of `data` as exactly N unquoted fields.
/// Only structural characters are touched; malformed lines are skipped.
#[inline(always)]
fn scan_lines<'a, const N: usize>(data: &'a [u8], mut f: impl FnMut(&[&'a [u8]; N])) {
    let len = data.len();
    let mut fields: [&'a [u8]; N] = [&[]; N];
    let mut nf = 0usize;
    let mut bad = false;
    let mut fstart = 0usize;
    let mut nq = 0u32;
    let mut qfirst = 0usize;
    let mut qlast = 0usize;
    let mut line_start = 0usize;

    macro_rules! close_field {
        ($pos:expr) => {{
            let pos = $pos;
            let v: &'a [u8] = if nq == 0 {
                &data[fstart..pos]
            } else if nq == 2 && qfirst == fstart && qlast + 1 == pos {
                &data[fstart + 1..pos - 1]
            } else {
                bad = true;
                &[]
            };
            if nf < N {
                fields[nf] = v;
            } else {
                bad = true;
            }
            nf += 1;
            nq = 0;
            fstart = pos + 1;
        }};
    }

    let mut base = 0usize;
    let mut tail;
    while base < len {
        let (c, q, n) = if base + 64 <= len {
            masks64(data[base..base + 64].try_into().unwrap())
        } else {
            tail = [0u8; 64];
            tail[..len - base].copy_from_slice(&data[base..]);
            masks64(&tail)
        };
        let mut m = c | q | n;
        while m != 0 {
            let i = m.trailing_zeros();
            let bit = 1u64 << i;
            m &= m - 1;
            let pos = base + i as usize;
            if q & bit != 0 {
                if nq == 0 {
                    qfirst = pos;
                }
                qlast = pos;
                nq += 1;
            } else {
                close_field!(pos);
                if n & bit != 0 {
                    if !bad && nf == N && pos > line_start {
                        f(&fields);
                    }
                    nf = 0;
                    bad = false;
                    line_start = pos + 1;
                }
            }
        }
        base += 64;
    }
    if line_start < len {
        close_field!(len);
        if !bad && nf == N {
            f(&fields);
        }
    }
}

// ---------------------------------------------------------------- parallel helper

fn par_for<T: Send>(nthreads: usize, ntasks: usize, init: impl Fn() -> T + Sync, work: impl Fn(&mut T, usize) + Sync) -> Vec<T> {
    let next = AtomicUsize::new(0);
    std::thread::scope(|sc| {
        let hs: Vec<_> = (0..nthreads)
            .map(|_| {
                sc.spawn(|| {
                    let mut st = init();
                    loop {
                        let i = next.fetch_add(1, Ordering::Relaxed);
                        if i >= ntasks {
                            break;
                        }
                        work(&mut st, i);
                    }
                    st
                })
            })
            .collect();
        hs.into_iter().map(|h| h.join().unwrap()).collect()
    })
}

// ---------------------------------------------------------------- main

struct UserRow {
    person: u64,
    day: i32,
    sessions: i64,
    events: i64,
    revenue: i64,
    top: i32,
}

struct SessSt {
    rows: Vec<UserRow>,
    cd: Vec<[i64; 4]>, // per (camp, day): sessions, purchasing, revenue, assisted
    cnt: Vec<u32>,
    pos: Vec<u32>,
    active: Vec<u32>,
    catsum: Vec<i64>,
    touched: Vec<u16>,
}

fn flush_day(st: &mut SessSt, person: u64, day: i64, s: i64, ev: i64, rv: i64) {
    let mut top = -1i32;
    let mut best = -1i64;
    for &c in &st.touched {
        let v = st.catsum[c as usize];
        if v > best || (v == best && (c as i32) < top) {
            best = v;
            top = c as i32;
        }
    }
    for &c in &st.touched {
        st.catsum[c as usize] = 0;
    }
    st.touched.clear();
    st.rows.push(UserRow { person, day: day as i32, sessions: s, events: ev, revenue: rv, top });
}

fn main() {
    let data_dir = PathBuf::from(std::env::var("DATA_DIR").unwrap_or("/data".into()));
    let out_dir = PathBuf::from(std::env::var("OUTPUT_DIR").unwrap_or("/output".into()));
    let nt: usize = std::env::var("NCPU")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or_else(|| std::thread::available_parallelism().map(|n| n.get()).unwrap_or(8))
        .max(1);
    let t0 = std::time::Instant::now();
    let lap = |what: &str| eprintln!("{:>8.1} ms  {}", t0.elapsed().as_secs_f64() * 1e3, what);

    // ---- small tables
    let manifest = fs::read_to_string(data_dir.join("manifest.json")).expect("manifest");
    let ws = manifest_ts(&manifest, "\"window_start\"");
    let we = manifest_ts(&manifest, "\"window_end\"");
    let cws = ws - WEEK;
    let day0 = ws.div_euclid(DAY);
    let nd = ((we - 1).div_euclid(DAY) - day0 + 1).max(1) as usize;

    let prods = read_small(&data_dir.join("products.csv"));
    let mut cat_names: Vec<Vec<u8>> = prods.iter().map(|(_, c)| c.clone()).collect();
    cat_names.push(b"unknown".to_vec());
    cat_names.sort();
    cat_names.dedup();
    let unknown_cat = cat_names.iter().position(|c| c == b"unknown").unwrap() as u16;
    let prod_items: Vec<(Vec<u8>, u32)> = prods
        .iter()
        .map(|(p, c)| (p.clone(), cat_names.binary_search(c).unwrap() as u32))
        .collect();
    let prod_lu = Lookup::new(b"p", &prod_items);
    let ncat = cat_names.len();

    let camps = read_small(&data_dir.join("campaigns.csv"));
    let camp_items: Vec<(Vec<u8>, u32)> = camps.iter().enumerate().map(|(i, (c, _))| (c.clone(), i as u32)).collect();
    let camp_lu = Lookup::new(b"c_", &camp_items);
    let ncamp = camps.len();
    let organic = ncamp as u32;
    lap("small tables");

    // ---- links
    let link_maps = map_files(&list_csv(&data_dir.join("links")));
    let link_chunks = chunks(&link_maps);
    let link_parts = par_for(
        nt,
        link_chunks.len(),
        || (Vec::<(u64, u64)>::new(), Reg::new()),
        |(pairs, reg), i| {
            let c = &link_chunks[i];
            scan_lines(&link_maps[c.file][c.start..c.end], |f: &[&[u8]; 2]| {
                if f[0].is_empty() || f[1].is_empty() {
                    return;
                }
                let a = key_of(f[0], reg);
                let b = key_of(f[1], reg);
                if a != b {
                    pairs.push((a, b));
                }
            });
        },
    );
    let mut reg: HashMap<u64, Vec<u8>> = HashMap::new();
    let mut npairs = 0;
    for (p, r) in &link_parts {
        npairs += p.len();
        for (k, b) in r {
            reg.insert(*k, b.clone());
        }
    }
    let mut idx = U64Map::with_capacity(npairs * 2 + 16);
    let mut ids: Vec<u64> = Vec::with_capacity(npairs * 2);
    let mut parent: Vec<u32> = Vec::with_capacity(npairs * 2);
    fn find(parent: &mut [u32], mut x: u32) -> u32 {
        while parent[x as usize] != x {
            let p = parent[x as usize];
            parent[x as usize] = parent[p as usize];
            x = p;
        }
        x
    }
    fn id_of(k: u64, idx: &mut U64Map, ids: &mut Vec<u64>, parent: &mut Vec<u32>) -> u32 {
        if let Some(v) = idx.get(k) {
            return v as u32;
        }
        let n = ids.len() as u32;
        idx.insert(k, n as u64);
        ids.push(k);
        parent.push(n);
        n
    }
    for (p, _) in &link_parts {
        for &(a, b) in p {
            let ia = id_of(a, &mut idx, &mut ids, &mut parent);
            let ib = id_of(b, &mut idx, &mut ids, &mut parent);
            let ra = find(&mut parent, ia);
            let rb = find(&mut parent, ib);
            if ra != rb {
                parent[ra.max(rb) as usize] = ra.min(rb);
            }
        }
    }
    drop(link_parts);
    let mut name: Vec<u64> = vec![u64::MAX; ids.len()];
    for i in 0..ids.len() {
        let r = find(&mut parent, i as u32) as usize;
        let k = ids[i];
        if name[r] == u64::MAX || key_less(k, name[r], &reg) {
            name[r] = k;
        }
    }
    let mut person_of = U64Map::with_capacity(ids.len());
    for i in 0..ids.len() {
        let r = find(&mut parent, i as u32) as usize;
        person_of.insert(ids[i], name[r]);
    }
    lap(&format!("links: {} pairs, {} ids", npairs, ids.len()));
    drop((idx, parent, name, ids));

    // ---- events + clicks
    let ev_maps = map_files(&list_csv(&data_dir.join("events")));
    let ck_maps = map_files(&list_csv(&data_dir.join("clicks")));
    let ev_chunks = chunks(&ev_maps);
    let ck_chunks = chunks(&ck_maps);
    let n_evb = 1usize << EV_BITS;
    let n_pb = 1usize << PB_BITS;
    let total_tasks = ev_chunks.len() + ck_chunks.len();

    const BATCH: usize = 32;
    struct ParseSt {
        evb: Vec<Vec<Ev>>,
        ckb: Vec<Vec<Ck>>,
        reg: Reg,
        pend: Vec<(Ev, u64)>,
        cpend: Vec<(Ck, u64)>,
    }
    #[inline(always)]
    fn flush_ev(st: &mut ParseSt, person_of: &U64Map) {
        for i in 0..st.pend.len() {
            let (mut e, uid) = st.pend[i];
            e.person = person_of.get(uid).unwrap_or(uid);
            let b = (mix(e.eid) >> (64 - EV_BITS)) as usize;
            st.evb[b].push(e);
        }
        st.pend.clear();
    }
    #[inline(always)]
    fn flush_ck(st: &mut ParseSt, person_of: &U64Map) {
        for i in 0..st.cpend.len() {
            let (mut c, uid) = st.cpend[i];
            c.person = person_of.get(uid).unwrap_or(uid);
            let b = (mix(c.person) >> (64 - PB_BITS)) as usize;
            st.ckb[b].push(c);
        }
        st.cpend.clear();
    }
    let person_of_ref = &person_of;
    let mut parsed = par_for(
        nt,
        total_tasks,
        || ParseSt {
            evb: (0..n_evb).map(|_| Vec::new()).collect(),
            ckb: (0..n_pb).map(|_| Vec::new()).collect(),
            reg: Reg::new(),
            pend: Vec::with_capacity(BATCH),
            cpend: Vec::with_capacity(BATCH),
        },
        |st, i| {
            if i < ev_chunks.len() {
                let c = &ev_chunks[i];
                let est = (c.end - c.start) / 70 / n_evb + 8;
                scan_lines(&ev_maps[c.file][c.start..c.end], |f: &[&[u8]; 7]| {
                    if f[0].is_empty() {
                        return;
                    }
                    let eid = key_of(f[0], &mut st.reg);
                    let ingest_ms = parse_ts(f[3]).unwrap_or(i64::MIN / 2);
                    let ingest = (ingest_ms - ws).div_euclid(1000).clamp(i32::MIN as i64, i32::MAX as i64) as i32;
                    let mut valid = !f[1].is_empty() && !f[5].is_empty();
                    let ts = match parse_ts(f[2]) {
                        Some(t) if t >= ws && t < we => t,
                        _ => {
                            valid = false;
                            ws
                        }
                    };
                    let mut purchase = false;
                    let mut price = 0u32;
                    match f[4] {
                        b"view" | b"cart" => {}
                        b"purchase" => {
                            purchase = true;
                            match parse_price(f[6]) {
                                Some(p) => price = p,
                                None => valid = false,
                            }
                        }
                        _ => valid = false,
                    }
                    if valid && ingest_ms - ts > DAY {
                        valid = false;
                    }
                    let flags = valid as u8 | ((purchase as u8) << 1);
                    if valid {
                        let uid = key_of(f[1], &mut st.reg);
                        let cat = if purchase { prod_lu.get(f[5]).map(|c| c as u16).unwrap_or(unknown_cat) } else { 0 };
                        person_of_ref.prefetch(uid);
                        st.pend.push((Ev { eid, person: 0, ts: (ts - ws) as u32, ingest, price, cat, flags }, uid));
                        if st.pend.len() == BATCH {
                            flush_ev(st, person_of_ref);
                        }
                    } else {
                        let b = (mix(eid) >> (64 - EV_BITS)) as usize;
                        let v = &mut st.evb[b];
                        if v.capacity() == 0 {
                            v.reserve(est);
                        }
                        v.push(Ev { eid, person: 0, ts: 0, ingest, price: 0, cat: 0, flags });
                    }
                });
                flush_ev(st, person_of_ref);
            } else {
                let c = &ck_chunks[i - ev_chunks.len()];
                scan_lines(&ck_maps[c.file][c.start..c.end], |f: &[&[u8]; 4]| {
                    if f[0].is_empty() || f[1].is_empty() {
                        return;
                    }
                    let ts = match parse_ts(f[3]) {
                        Some(t) if t >= cws && t < we => t,
                        _ => return,
                    };
                    let camp = match camp_lu.get(f[2]) {
                        Some(c) => c,
                        None => return,
                    };
                    let cid = key_of(f[0], &mut st.reg);
                    let uid = key_of(f[1], &mut st.reg);
                    person_of_ref.prefetch(uid);
                    st.cpend.push((Ck { person: 0, ts, cid, camp }, uid));
                    if st.cpend.len() == BATCH {
                        flush_ck(st, person_of_ref);
                    }
                });
                flush_ck(st, person_of_ref);
            }
        },
    );
    for st in &mut parsed {
        for (k, b) in st.reg.drain(..) {
            reg.entry(k).or_insert(b);
        }
    }
    lap("parse events+clicks");

    // ---- dedup
    let parsed_ref = &parsed;
    let dedup = par_for(
        nt,
        n_evb,
        || ((0..n_pb).map(|_| Vec::<Sv>::new()).collect::<Vec<_>>(), Vec::<u32>::new(), Vec::<Ev>::new()),
        |(out, table, all), b| {
            all.clear();
            for s in parsed_ref {
                all.extend_from_slice(&s.evb[b]);
            }
            let n = all.len();
            if n == 0 {
                return;
            }
            let cap = (n * 2).next_power_of_two();
            let mask = cap - 1;
            table.clear();
            table.resize(cap, u32::MAX);
            for i in 0..n {
                let e = &all[i];
                let mut h = (mix(e.eid) >> 8) as usize & mask;
                loop {
                    let t = table[h];
                    if t == u32::MAX {
                        table[h] = i as u32;
                        break;
                    }
                    let o = &all[t as usize];
                    if o.eid == e.eid {
                        if e.ingest < o.ingest {
                            table[h] = i as u32;
                        }
                        break;
                    }
                    h = (h + 1) & mask;
                }
            }
            for &t in table.iter() {
                if t == u32::MAX {
                    continue;
                }
                let e = &all[t as usize];
                if e.flags & 1 == 0 {
                    continue;
                }
                let pb = (mix(e.person) >> (64 - PB_BITS)) as usize;
                out[pb].push(Sv { person: e.person, ts: ws + e.ts as i64, price: e.price, cat: e.cat, purchase: e.flags & 2 != 0 });
            }
        },
    );
    for st in &mut parsed {
        st.evb = Vec::new();
    }
    lap("dedup");

    // ---- sessions
    let dedup_ref = &dedup;
    let parsed_ref = &parsed;
    let reg_ref = &reg;
    let sess = par_for(
        nt,
        n_pb,
        || SessSt {
            rows: Vec::new(),
            cd: vec![[0; 4]; (ncamp + 1) * nd],
            cnt: vec![0; ncamp + 1],
            pos: vec![0; ncamp + 1],
            active: Vec::new(),
            catsum: vec![0; ncat],
            touched: Vec::new(),
        },
        |st, b| {
            let mut evs: Vec<Sv> = Vec::new();
            for (o, _, _) in dedup_ref {
                evs.extend_from_slice(&o[b]);
            }
            if evs.is_empty() {
                return;
            }
            evs.sort_unstable_by_key(|e| (e.person, e.ts));
            let mut cks: Vec<Ck> = Vec::new();
            for s in parsed_ref {
                cks.extend_from_slice(&s.ckb[b]);
            }
            cks.sort_unstable_by_key(|c| (c.person, c.ts));

            let mut ci = 0usize;
            let mut i = 0usize;
            while i < evs.len() {
                let p = evs[i].person;
                let mut j = i;
                while j < evs.len() && evs[j].person == p {
                    j += 1;
                }
                while ci < cks.len() && cks[ci].person < p {
                    ci += 1;
                }
                let mut ce = ci;
                while ce < cks.len() && cks[ce].person == p {
                    ce += 1;
                }
                let pc = &cks[ci..ce];
                ci = ce;

                let (mut lo, mut hi) = (0usize, 0usize);
                let mut cur_day: i64 = i64::MIN;
                let (mut d_sess, mut d_events, mut d_rev) = (0i64, 0i64, 0i64);

                let mut k = i;
                while k < j {
                    let start = evs[k].ts;
                    let day = start.div_euclid(DAY);
                    if day != cur_day {
                        if cur_day != i64::MIN {
                            flush_day(st, p, cur_day, d_sess, d_events, d_rev);
                        }
                        cur_day = day;
                        d_sess = 0;
                        d_events = 0;
                        d_rev = 0;
                    }
                    let mut last = start;
                    let mut m = k;
                    let mut rev = 0i64;
                    let mut purch = false;
                    while m < j && evs[m].ts - last <= GAP {
                        let e = &evs[m];
                        last = e.ts;
                        if e.purchase {
                            purch = true;
                            rev += e.price as i64;
                            let c = e.cat as usize;
                            if st.catsum[c] == 0 {
                                st.touched.push(e.cat);
                            }
                            st.catsum[c] += e.price as i64;
                        }
                        m += 1;
                    }
                    let nev = (m - k) as i64;
                    d_sess += 1;
                    d_events += nev;
                    d_rev += rev;

                    while hi < pc.len() && pc[hi].ts <= start {
                        let c = pc[hi].camp as usize;
                        if st.cnt[c] == 0 {
                            st.pos[c] = st.active.len() as u32;
                            st.active.push(c as u32);
                        }
                        st.cnt[c] += 1;
                        hi += 1;
                    }
                    while lo < hi && pc[lo].ts < start - WEEK {
                        let c = pc[lo].camp as usize;
                        st.cnt[c] -= 1;
                        if st.cnt[c] == 0 {
                            let at = st.pos[c] as usize;
                            let lastc = *st.active.last().unwrap();
                            st.active[at] = lastc;
                            st.pos[lastc as usize] = at as u32;
                            st.active.pop();
                        }
                        lo += 1;
                    }
                    let di = (day - day0) as usize;
                    let credited = if lo < hi {
                        let tmax = pc[hi - 1].ts;
                        let mut r = hi - 1;
                        let mut best = r;
                        while r > lo && pc[r - 1].ts == tmax {
                            r -= 1;
                            if key_less(pc[r].cid, pc[best].cid, reg_ref) {
                                best = r;
                            }
                        }
                        pc[best].camp
                    } else {
                        organic
                    };
                    let row = &mut st.cd[credited as usize * nd + di];
                    row[0] += 1;
                    row[1] += purch as i64;
                    row[2] += rev;
                    for ai in 0..st.active.len() {
                        let c = st.active[ai] as usize;
                        st.cd[c * nd + di][3] += 1;
                    }
                    k = m;
                }
                flush_day(st, p, cur_day, d_sess, d_events, d_rev);
                for &c in &st.active {
                    st.cnt[c as usize] = 0;
                }
                st.active.clear();
                i = j;
            }
        },
    );
    drop(dedup);
    lap("sessions");

    // ---- output
    let ud = out_dir.join("user_daily");
    let cdd = out_dir.join("campaign_daily");
    fs::create_dir_all(&ud).unwrap();
    fs::create_dir_all(&cdd).unwrap();
    std::thread::scope(|sc| {
        let any = sess.iter().any(|st| !st.rows.is_empty());
        for (t, st) in sess.iter().enumerate() {
            if st.rows.is_empty() && (any || t > 0) {
                continue;
            }
            let path = ud.join(format!("part-{t:03}.parquet"));
            let cat_names = &cat_names;
            let reg = &reg;
            sc.spawn(move || write_user(&path, &st.rows, cat_names, reg));
        }
        let mut agg = vec![[0i64; 4]; (ncamp + 1) * nd];
        for st in &sess {
            for (a, b) in agg.iter_mut().zip(st.cd.iter()) {
                for q in 0..4 {
                    a[q] += b[q];
                }
            }
        }
        let mut rows = vec![];
        for c in 0..=ncamp {
            for d in 0..nd {
                let r = agg[c * nd + d];
                if r[0] != 0 || r[3] != 0 {
                    rows.push((c, d, r));
                }
            }
        }
        write_campaign(&cdd.join("part-000.parquet"), &rows, &camps, day0);
    });
    lap("write");
}

// ---------------------------------------------------------------- parquet

use parquet::basic::Compression;
use parquet::data_type::{ByteArray, ByteArrayType, Int32Type, Int64Type};
use parquet::file::properties::{EnabledStatistics, WriterProperties};
use parquet::file::writer::SerializedFileWriter;
use parquet::schema::parser::parse_message_type;

fn props() -> Arc<WriterProperties> {
    Arc::new(
        WriterProperties::builder()
            .set_compression(Compression::UNCOMPRESSED)
            .set_dictionary_enabled(false)
            .set_statistics_enabled(EnabledStatistics::None)
            .build(),
    )
}

fn write_user(path: &Path, rows: &[UserRow], cats: &[Vec<u8>], reg: &HashMap<u64, Vec<u8>>) {
    let schema = Arc::new(
        parse_message_type(
            "message user_daily { REQUIRED BYTE_ARRAY user_id (UTF8); REQUIRED INT32 day (DATE); REQUIRED INT64 sessions; REQUIRED INT64 events; REQUIRED INT64 revenue_cents; OPTIONAL BYTE_ARRAY top_category (UTF8); }",
        )
        .unwrap(),
    );
    let f = File::create(path).unwrap();
    let mut w = SerializedFileWriter::new(f, schema, props()).unwrap();
    let mut rg = w.next_row_group().unwrap();
    let mut col = 0;
    let cat_ba: Vec<ByteArray> = cats.iter().map(|c| ByteArray::from(c.clone())).collect();
    while let Some(mut c) = rg.next_column().unwrap() {
        match col {
            0 => {
                let v: Vec<ByteArray> = rows.iter().map(|r| ByteArray::from(bytes_of(r.person, reg))).collect();
                c.typed::<ByteArrayType>().write_batch(&v, None, None).unwrap();
            }
            1 => {
                let v: Vec<i32> = rows.iter().map(|r| r.day).collect();
                c.typed::<Int32Type>().write_batch(&v, None, None).unwrap();
            }
            2 | 3 | 4 => {
                let v: Vec<i64> = rows
                    .iter()
                    .map(|r| match col {
                        2 => r.sessions,
                        3 => r.events,
                        _ => r.revenue,
                    })
                    .collect();
                c.typed::<Int64Type>().write_batch(&v, None, None).unwrap();
            }
            _ => {
                let defs: Vec<i16> = rows.iter().map(|r| (r.top >= 0) as i16).collect();
                let v: Vec<ByteArray> = rows.iter().filter(|r| r.top >= 0).map(|r| cat_ba[r.top as usize].clone()).collect();
                c.typed::<ByteArrayType>().write_batch(&v, Some(&defs), None).unwrap();
            }
        }
        c.close().unwrap();
        col += 1;
    }
    rg.close().unwrap();
    w.close().unwrap();
}

fn write_campaign(path: &Path, rows: &[(usize, usize, [i64; 4])], camps: &[(Vec<u8>, Vec<u8>)], day0: i64) {
    let schema = Arc::new(
        parse_message_type(
            "message campaign_daily { REQUIRED BYTE_ARRAY campaign_id (UTF8); REQUIRED BYTE_ARRAY channel (UTF8); REQUIRED INT32 day (DATE); REQUIRED INT64 sessions; REQUIRED INT64 purchasing_sessions; REQUIRED INT64 revenue_cents; REQUIRED INT64 assisted_sessions; }",
        )
        .unwrap(),
    );
    let f = File::create(path).unwrap();
    let mut w = SerializedFileWriter::new(f, schema, props()).unwrap();
    let mut rg = w.next_row_group().unwrap();
    let mut col = 0;
    let ncamp = camps.len();
    while let Some(mut c) = rg.next_column().unwrap() {
        match col {
            0 | 1 => {
                let v: Vec<ByteArray> = rows
                    .iter()
                    .map(|(ci, _, _)| {
                        if *ci == ncamp {
                            ByteArray::from(b"organic".to_vec())
                        } else if col == 0 {
                            ByteArray::from(camps[*ci].0.clone())
                        } else {
                            ByteArray::from(camps[*ci].1.clone())
                        }
                    })
                    .collect();
                c.typed::<ByteArrayType>().write_batch(&v, None, None).unwrap();
            }
            2 => {
                let v: Vec<i32> = rows.iter().map(|(_, d, _)| (day0 + *d as i64) as i32).collect();
                c.typed::<Int32Type>().write_batch(&v, None, None).unwrap();
            }
            _ => {
                let q = match col {
                    3 => 0,
                    4 => 1,
                    5 => 2,
                    _ => 3,
                };
                let v: Vec<i64> = rows.iter().map(|(_, _, r)| r[q]).collect();
                c.typed::<Int64Type>().write_batch(&v, None, None).unwrap();
            }
        }
        c.close().unwrap();
        col += 1;
    }
    rg.close().unwrap();
    w.close().unwrap();
}
