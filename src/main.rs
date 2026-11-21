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

mod pq;
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
const CHUNK: usize = 256 << 10; // L2-sized: the pread copy stays in cache

// ---------------------------------------------------------------- keys

const RANK: [u8; 256] = {
    let mut t = [0u8; 256];
    let mut c = 0;
    while c < 10 {
        t[b'0' as usize + c] = 1 + c as u8;
        c += 1;
    }
    let mut c = 0;
    while c < 26 {
        t[b'a' as usize + c] = 11 + c as u8;
        c += 1;
    }
    t
};

/// 1..=10 chars of [0-9a-z] -> 6 bits per char, zero padded: integer order == byte order.
#[inline(always)]
fn pack(s: &[u8]) -> Option<u64> {
    let n = s.len();
    if n == 0 || n > 10 {
        return None;
    }
    let mut v = 0u64;
    for &c in s {
        let r = RANK[c as usize];
        if r == 0 {
            return None;
        }
        v = (v << 6) | r as u64;
    }
    Some(v << (6 * (10 - n)))
}

/// Same encoding as `pack`, for a field that has at least 16 readable bytes
/// from its start (the pread buffer is padded). SIMD rank + BMI2 pext.
#[inline(always)]
fn pack_padded(s: &[u8]) -> Option<u64> {
    #[cfg(all(target_arch = "x86_64", target_feature = "avx2", target_feature = "bmi2"))]
    unsafe {
        use std::arch::x86_64::*;
        let n = s.len();
        if n == 0 || n > 10 {
            return None;
        }
        let raw = _mm_loadu_si128(s.as_ptr() as *const __m128i);
        // zero the bytes past the field
        let idx = _mm_setr_epi8(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15);
        let keep = _mm_cmpgt_epi8(_mm_set1_epi8(n as i8), idx);
        let v = _mm_and_si128(raw, keep);
        // ranks: '0'..'9' -> 1..10, 'a'..'z' -> 11..36, anything else -> 0
        let d = _mm_sub_epi8(v, _mm_set1_epi8(b'0' as i8));
        let is_d = _mm_cmpeq_epi8(_mm_min_epu8(d, _mm_set1_epi8(9)), d);
        let a = _mm_sub_epi8(v, _mm_set1_epi8(b'a' as i8));
        let is_a = _mm_cmpeq_epi8(_mm_min_epu8(a, _mm_set1_epi8(25)), a);
        let rank = _mm_or_si128(
            _mm_and_si128(is_d, _mm_add_epi8(d, _mm_set1_epi8(1))),
            _mm_and_si128(is_a, _mm_add_epi8(a, _mm_set1_epi8(11))),
        );
        let ok = (_mm_movemask_epi8(_mm_or_si128(is_d, is_a)) as u32) & ((1u32 << n) - 1);
        if ok != (1u32 << n) - 1 {
            return None;
        }
        // big-endian char order, then 6 bits per char
        let rev = _mm_shuffle_epi8(rank, _mm_setr_epi8(15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0));
        let hi = _mm_extract_epi64(rev, 1) as u64; // chars 0..7
        let lo = _mm_extract_epi64(rev, 0) as u64; // chars 8..15
        let a8 = _pext_u64(hi, 0x3f3f_3f3f_3f3f_3f3f);
        let b2 = _pext_u64(lo, 0x3f3f_0000_0000_0000);
        return Some((a8 << 12) | b2);
    }
    #[allow(unreachable_code)]
    pack(s)
}

#[inline(always)]
fn key_of_padded(s: &[u8], reg: &mut Reg) -> u64 {
    match pack_padded(s) {
        Some(k) => k,
        None => key_of(s, reg),
    }
}

fn unpack(v: u64, out: &mut Vec<u8>) {
    for i in (0..10).rev() {
        let r = ((v >> (6 * i)) & 63) as u8;
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

/// Memo of "YYYY-MM-DD" -> epoch ms for the dates around the window.
/// Written once in main before any parsing threads start.
static mut DATE_TAB: [(u64, u16, i64); 256] = [(0, 0, 0); 256];

#[inline(always)]
fn date_slot(s: &[u8]) -> usize {
    ((((s[6] & 15) as usize) << 5) | ((((s[8] & 15) as usize) * 10 + (s[9] & 15) as usize) & 31)) & 255
}

fn fill_date_tab(from_day: i64, to_day: i64) {
    for d in from_day..=to_day {
        // civil_from_days
        let z = d + 719_468;
        let era = z.div_euclid(146_097);
        let doe = z - era * 146_097;
        let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
        let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
        let mp = (5 * doy + 2) / 153;
        let day = doy - (153 * mp + 2) / 5 + 1;
        let m = if mp < 10 { mp + 3 } else { mp - 9 };
        let y = yoe + era * 400 + (m <= 2) as i64;
        if !(0..=9999).contains(&y) {
            continue;
        }
        let txt = format!("{:04}-{:02}-{:02}", y, m, day);
        let b = txt.as_bytes();
        let k8 = u64::from_le_bytes(b[0..8].try_into().unwrap());
        let k2 = u16::from_le_bytes(b[8..10].try_into().unwrap());
        let i = date_slot(b);
        unsafe {
            if DATE_TAB[i].0 == 0 {
                DATE_TAB[i] = (k8, k2, d * DAY);
            }
        }
    }
}

#[inline(always)]
fn civil(s: &[u8]) -> Option<i64> {
    let k8 = u64::from_le_bytes(s[0..8].try_into().unwrap());
    let k2 = u16::from_le_bytes(s[8..10].try_into().unwrap());
    let e = unsafe { *std::ptr::addr_of!(DATE_TAB).cast::<(u64, u16, i64)>().add(date_slot(s)) };
    if e.0 == k8 && e.1 == k2 && e.0 != 0 {
        // branch-free "THH:MM:SS"
        let d = |i: usize| s[i].wrapping_sub(b'0');
        let (h1, h2, m1, m2, s1, s2) = (d(11), d(12), d(14), d(15), d(17), d(18));
        let h = h1 as i64 * 10 + h2 as i64;
        let mi = m1 as i64 * 10 + m2 as i64;
        let se = s1 as i64 * 10 + s2 as i64;
        let ok = (h1 < 10) & (h2 < 10) & (m1 < 10) & (m2 < 10) & (s1 < 10) & (s2 < 10)
            & (s[10] == b'T') & (s[13] == b':') & (s[16] == b':') & (h < 24) & (mi < 60) & (se < 60);
        return if ok { Some(e.2 + (h * 3600 + mi * 60 + se) * 1000) } else { None };
    }
    civil_slow(s)
}

/// event_type: 0 = view/cart, 1 = purchase, 2 = anything else.
#[inline(always)]
fn etype(s: &[u8]) -> u8 {
    match s.len() {
        4 => {
            let x = u32::from_le_bytes(s[0..4].try_into().unwrap());
            if x == u32::from_le_bytes(*b"view") || x == u32::from_le_bytes(*b"cart") { 0 } else { 2 }
        }
        8 => {
            if u64::from_le_bytes(s[0..8].try_into().unwrap()) == u64::from_le_bytes(*b"purchase") { 1 } else { 2 }
        }
        _ => 2,
    }
}

/// 8 ASCII digits (little-endian load, first char lowest) -> value, or None.
#[inline(always)]
fn digits8(x: u64) -> Option<u64> {
    let ok = (x & 0xF0F0_F0F0_F0F0_F0F0) == 0x3030_3030_3030_3030
        && (x.wrapping_add(0x0606_0606_0606_0606) & 0xF0F0_F0F0_F0F0_F0F0) == 0x3030_3030_3030_3030;
    if !ok {
        return None;
    }
    let mut v = x & 0x0F0F_0F0F_0F0F_0F0F;
    v = v.wrapping_mul(2561) >> 8;
    v = (v & 0x00FF_00FF_00FF_00FF).wrapping_mul(6_553_601) >> 16;
    v = (v & 0x0000_FFFF_0000_FFFF).wrapping_mul(42_949_672_960_001) >> 32;
    Some(v)
}

#[cold]
fn civil_slow(s: &[u8]) -> Option<i64> {
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
    if n == 13 {
        let a = digits8(u64::from_le_bytes(s[0..8].try_into().unwrap()))?;
        let b = digits8(u64::from_le_bytes(s[5..13].try_into().unwrap()))?;
        return Some((a * 100_000 + b % 100_000) as i64);
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
    /// optional membership filter (1 bit per hash slot): a clear bit = absent
    filt: Vec<u64>,
}
impl U64Map {
    fn with_capacity(n: usize) -> Self {
        let cap = (n * 2).next_power_of_two().max(16);
        U64Map { mask: cap - 1, e: vec![[u64::MAX, 0]; cap], filt: Vec::new() }
    }
    #[inline(always)]
    fn start(&self, k: u64) -> usize {
        (mix(k) >> 20) as usize & self.mask
    }
    const FBITS: u32 = 24;
    /// Build the membership filter from the stored keys.
    fn build_filter(&mut self) {
        let mut f = vec![0u64; 1 << (Self::FBITS - 6)];
        for &[k, _] in &self.e {
            if k != u64::MAX {
                let h = (mix(k) >> (64 - Self::FBITS)) as usize;
                f[h >> 6] |= 1 << (h & 63);
            }
        }
        self.filt = f;
    }
    #[inline(always)]
    fn maybe(&self, k: u64) -> bool {
        let h = (mix(k) >> (64 - Self::FBITS)) as usize;
        unsafe { *self.filt.get_unchecked(h >> 6) >> (h & 63) & 1 != 0 }
    }
    /// get() behind the filter; the filter must have been built.
    #[inline(always)]
    fn get_f(&self, k: u64) -> Option<u64> {
        if self.maybe(k) {
            self.get(k)
        } else {
            None
        }
    }
    #[inline(always)]
    fn prefetch_f(&self, k: u64) {
        if self.maybe(k) {
            self.prefetch(k);
        }
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
    ts: u32, // ms since window_start
    pc: u32, // price << 8 | category; price 0 = not a purchase
}

#[derive(Clone, Copy)]
struct Ck {
    person: u64,
    ts: i64,
    cid: u64,
    camp: u32,
}

// ---------------------------------------------------------------- huge-page slabs

/// Fixed-capacity buckets carved from one anonymous mmap region advised as
/// huge pages (one fault per 2 MiB instead of per 4 KiB), with a spill Vec per
/// bucket for the rare overflow. Pushes go through a small write-combining
/// buffer per bucket (WC records, cache resident) that is flushed to the slab
/// with non-temporal stores, so no slab line is ever read just to be written.
const WC: usize = 8;
struct Buckets<T: Copy> {
    ptr: *mut T,
    bytes: usize,
    cap: usize,
    lens: Vec<u32>,
    spill: Vec<Vec<T>>,
    wc: Vec<T>,
    wn: Vec<u8>,
}
unsafe impl<T: Copy + Send> Send for Buckets<T> {}
unsafe impl<T: Copy + Sync> Sync for Buckets<T> {}

/// Non-temporal slab stores (default on); X_NT=0 switches to cached stores.
static PROF: [AtomicUsize; 8] = [const { AtomicUsize::new(0) }; 8];
#[inline(always)]
fn prof(i: usize, t: std::time::Instant) -> std::time::Instant {
    let n = std::time::Instant::now();
    PROF[i].fetch_add((n - t).as_nanos() as usize, Ordering::Relaxed);
    n
}
static USE_NT: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(DEFAULT_NT);
const DEFAULT_NT: bool = true;

#[inline(always)]
unsafe fn stream_copy<T: Copy>(dst: *mut T, src: *const T, n: usize) {
    let bytes = n * std::mem::size_of::<T>();
    #[cfg(all(target_arch = "x86_64", target_feature = "avx"))]
    if USE_NT.load(Ordering::Relaxed) {
        use std::arch::x86_64::*;
        if bytes % 32 == 0 && (dst as usize) % 32 == 0 {
            let mut o = 0;
            while o < bytes {
                let v = _mm256_loadu_si256((src as *const u8).add(o) as *const __m256i);
                _mm256_stream_si256((dst as *mut u8).add(o) as *mut __m256i, v);
                o += 32;
            }
            return;
        }
    }
    std::ptr::copy_nonoverlapping(src as *const u8, dst as *mut u8, bytes);
}

#[inline(always)]
fn sfence() {
    #[cfg(target_arch = "x86_64")]
    unsafe {
        std::arch::x86_64::_mm_sfence();
    }
}

/// Released slab regions, reused by later stages so their pages (already
/// faulted and zeroed once) are not zeroed again by the kernel.
static POOL: std::sync::Mutex<Vec<(usize, usize)>> = std::sync::Mutex::new(Vec::new());

impl<T: Copy> Buckets<T> {
    fn new(nb: usize, cap: usize) -> Self {
        let cap = (cap.max(1) + WC - 1) / WC * WC;
        let need = (nb * cap * std::mem::size_of::<T>()).max(4096);
        let reused = {
            let mut pool = POOL.lock().unwrap();
            let mut best: Option<usize> = None;
            for (i, &(_, sz)) in pool.iter().enumerate() {
                if sz >= need && best.map_or(true, |j| sz < pool[j].1) {
                    best = Some(i);
                }
            }
            best.map(|i| pool.swap_remove(i))
        };
        let mut wc = Vec::with_capacity(nb * WC);
        unsafe { wc.set_len(nb * WC) };
        if let Some((p, sz)) = reused {
            return Buckets { ptr: p as *mut T, bytes: sz, cap, lens: vec![0; nb], spill: (0..nb).map(|_| Vec::new()).collect(), wc, wn: vec![0; nb] };
        }
        let bytes = need;
        let ptr = unsafe {
            let p = libc::mmap(
                std::ptr::null_mut(),
                bytes,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_PRIVATE | libc::MAP_ANONYMOUS | libc::MAP_NORESERVE,
                -1,
                0,
            );
            assert!(p != libc::MAP_FAILED, "mmap slab");
            if std::env::var_os("X_NOTHP").is_none() {
                libc::madvise(p, bytes, libc::MADV_HUGEPAGE);
            }
            p as *mut T
        };
        let mut wc = Vec::with_capacity(nb * WC);
        unsafe { wc.set_len(nb * WC) };
        Buckets { ptr, bytes, cap, lens: vec![0; nb], spill: (0..nb).map(|_| Vec::new()).collect(), wc, wn: vec![0; nb] }
    }
    #[inline(always)]
    fn push(&mut self, b: usize, x: T) {
        let n = self.wn[b] as usize;
        unsafe { *self.wc.get_unchecked_mut(b * WC + n) = x };
        if n + 1 < WC {
            self.wn[b] = n as u8 + 1;
        } else {
            self.wn[b] = 0;
            let l = self.lens[b] as usize;
            if l + WC <= self.cap {
                unsafe { stream_copy(self.ptr.add(b * self.cap + l), self.wc.as_ptr().add(b * WC), WC) };
                self.lens[b] = (l + WC) as u32;
            } else {
                self.spill[b].extend_from_slice(&self.wc[b * WC..b * WC + WC]);
            }
        }
    }
    /// Drain the write-combining buffers; call once the owner is done pushing.
    fn finish(&mut self) {
        for b in 0..self.lens.len() {
            let n = self.wn[b] as usize;
            if n == 0 {
                continue;
            }
            self.wn[b] = 0;
            let l = self.lens[b] as usize;
            if self.spill[b].is_empty() && l + n <= self.cap {
                unsafe { std::ptr::copy_nonoverlapping(self.wc.as_ptr().add(b * WC), self.ptr.add(b * self.cap + l), n) };
                self.lens[b] = (l + n) as u32;
            } else {
                let tmp: Vec<T> = self.wc[b * WC..b * WC + n].to_vec();
                self.spill[b].extend_from_slice(&tmp);
            }
        }
        sfence();
    }
    #[inline(always)]
    fn main(&self, b: usize) -> &[T] {
        unsafe { std::slice::from_raw_parts(self.ptr.add(b * self.cap), self.lens[b] as usize) }
    }
    #[inline(always)]
    fn len(&self, b: usize) -> usize {
        self.lens[b] as usize + self.spill[b].len() + self.wn[b] as usize
    }
    #[inline(always)]
    fn parts(&self, b: usize) -> (&[T], &[T]) {
        (self.main(b), &self.spill[b])
    }
}
impl<T: Copy> Drop for Buckets<T> {
    fn drop(&mut self) {
        if self.bytes >= (4 << 20) {
            POOL.lock().unwrap().push((self.ptr as usize, self.bytes));
        } else {
            unsafe { libc::munmap(self.ptr as *mut libc::c_void, self.bytes) };
        }
    }
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

fn open_files(paths: &[PathBuf]) -> Vec<File> {
    paths
        .iter()
        .filter_map(|p| {
            let f = File::open(p).expect("open");
            if f.metadata().map(|m| m.len()).unwrap_or(0) == 0 {
                return None;
            }
            Some(f)
        })
        .collect()
}

/// Read a chunk with pread into a reusable buffer (cheaper than walking
/// 4 KiB file page tables through mmap on a VM).
#[inline(always)]
fn read_chunk<'a>(buf: &'a mut Vec<u8>, f: &File, c: &Chunk) -> &'a [u8] {
    use std::os::unix::fs::FileExt;
    let n = c.end - c.start;
    if buf.len() < n + 64 {
        buf.resize(n + 64, 0);
    }
    f.read_exact_at(&mut buf[..n], c.start as u64).expect("pread");
    &buf[..n]
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
    #[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
    unsafe {
        use std::arch::x86_64::*;
        let (cm, qm, nm) = (_mm256_set1_epi8(b',' as i8), _mm256_set1_epi8(b'"' as i8), _mm256_set1_epi8(b'\n' as i8));
        let lo = _mm256_loadu_si256(p.as_ptr() as *const __m256i);
        let hi = _mm256_loadu_si256(p.as_ptr().add(32) as *const __m256i);
        let m = |v: __m256i, c: __m256i| _mm256_movemask_epi8(_mm256_cmpeq_epi8(v, c)) as u32 as u64;
        return (m(lo, cm) | (m(hi, cm) << 32), m(lo, qm) | (m(hi, qm) << 32), m(lo, nm) | (m(hi, nm) << 32));
    }
    #[cfg(all(target_arch = "x86_64", not(target_feature = "avx2")))]
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
/// Quote validity is decided per 64-byte word with mask algebra: a quote is
/// legal only at a field's first or last byte (and not both, i.e. a lone `"`
/// field). Within a line with no illegal quote, a field is wrapped iff its
/// first byte is a quote, and it is malformed iff exactly one end is a quote.
/// `data` must start at a line start. A virtual newline is placed at `len`,
/// so a final line without '\n' needs no special case.
#[inline(always)]
fn scan_lines<'a, const N: usize>(data: &'a [u8], mut f: impl FnMut(&[&'a [u8]; N])) {
    let len = data.len();
    if len == 0 {
        return;
    }
    let last = len - 1;
    let mut fields: [&'a [u8]; N] = [&[]; N];
    let mut nf = 0usize;
    let mut bad = false;
    let mut fstart = 0usize;
    let mut line_start = 0usize;
    let mut carry_d = 1u64; // virtual delimiter before the first byte
    let mut base = 0usize;
    let mut tail;
    while base <= len {
        let (c, q, mut n) = if base + 64 <= len {
            masks64(data[base..base + 64].try_into().unwrap())
        } else {
            tail = [0u8; 64];
            tail[..len - base].copy_from_slice(&data[base..]);
            let (c, q, n) = masks64(&tail);
            (c, q, n | (1u64 << (len - base)))
        };
        if base + 64 > len {
            // bits past the virtual newline are padding
            let keep = u64::MAX >> (63 - (len - base));
            n &= keep;
        }
        let d = c | n;
        // is the byte right after this word a delimiter (or the virtual end)?
        let next_d = if base + 64 < len {
            let b = unsafe { *data.get_unchecked(base + 64) };
            (b == b',') | (b == b'\n')
        } else {
            true
        };
        let s = (d << 1) | carry_d; // first byte of a field
        let e = (d >> 1) | ((next_d as u64) << 63); // last byte of a field
        let mut bw = q & !(s ^ e); // interior quote, or a lone-quote field
        carry_d = d >> 63;
        let mut m = d;
        while m != 0 {
            let i = m.trailing_zeros() as usize;
            m &= m - 1;
            let pos = base + i;
            let flen = pos - fstart;
            let (fq, lq) = unsafe { (*data.get_unchecked(fstart.min(last)) == b'"', *data.get_unchecked(pos.wrapping_sub(1).min(last)) == b'"') };
            let long = flen >= 2;
            bad |= (fq & long) != (lq & long);
            let w = (fq & long) as usize;
            let v: &'a [u8] = unsafe { data.get_unchecked(fstart + w..pos - w) };
            if nf < N {
                fields[nf] = v;
            } else {
                bad = true;
            }
            nf += 1;
            fstart = pos + 1;
            if n >> i & 1 != 0 {
                let upto = u64::MAX >> (63 - i);
                bad |= bw & upto != 0;
                bw &= !upto;
                if !bad && nf == N && pos > line_start {
                    f(&fields);
                }
                nf = 0;
                bad = false;
                line_start = pos + 1;
            }
        }
        bad |= bw != 0;
        base += 64;
    }
}

/// Visit every well-formed line of `data` as exactly N unquoted fields.
/// Only structural characters are touched; malformed lines are skipped.
#[inline(always)]
#[allow(dead_code)]
fn scan_lines_ref<'a, const N: usize>(data: &'a [u8], mut f: impl FnMut(&[&'a [u8]; N])) {
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

#[inline(always)]
#[allow(dead_code)]
fn scan_lines_old<'a, const N: usize>(data: &'a [u8], mut f: impl FnMut(&[&'a [u8]; N])) {
    let len = data.len();
    let mut fields: [&'a [u8]; N] = [&[]; N];
    let mut nf = 0usize;
    let mut bad = false;
    let mut fstart = 0usize;
    let mut line_start = 0usize;
    let mut prev_q = 0u64;
    let mut prev_base = 0usize;

    #[inline(always)]
    fn quotes_slow(d: &[u8]) -> u32 {
        d.iter().filter(|&&c| c == b'"').count() as u32
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
        let mut m = c | n;
        while m != 0 {
            let i = m.trailing_zeros() as usize;
            m &= m - 1;
            let pos = base + i;
            let below = (1u64 << i) - 1;
            let qc = if fstart >= base {
                (q & below & !((1u64 << (fstart - base)) - 1)).count_ones()
            } else if fstart >= prev_base {
                (prev_q >> (fstart - prev_base)).count_ones() + (q & below).count_ones()
            } else {
                quotes_slow(&data[fstart..pos])
            };
            // branch-free: wrapped iff exactly 2 quotes, at the field's two ends
            let flen = pos - fstart;
            let (fq, lq) = unsafe { (*data.get_unchecked(fstart) == b'"', *data.get_unchecked(pos.wrapping_sub(1).min(pos)) == b'"') };
            let wrapped = (qc == 2) & (flen >= 2) & fq & lq;
            bad |= (qc != 0) & !wrapped;
            let w = wrapped as usize;
            let v: &'a [u8] = unsafe { data.get_unchecked(fstart + w..pos - w) };
            if nf < N {
                fields[nf] = v;
            } else {
                bad = true;
            }
            nf += 1;
            fstart = pos + 1;
            if n >> i & 1 != 0 {
                if !bad && nf == N && pos > line_start {
                    f(&fields);
                }
                nf = 0;
                bad = false;
                line_start = pos + 1;
            }
        }
        prev_q = q;
        prev_base = base;
        base += 64;
    }
    if line_start < len {
        let pos = len;
        let qc = quotes_slow(&data[fstart..pos]);
        let flen = pos - fstart;
        let wrapped = qc == 2 && flen >= 2 && data[fstart] == b'"' && data[pos - 1] == b'"';
        bad |= qc != 0 && !wrapped;
        let w = wrapped as usize;
        if nf < N {
            fields[nf] = &data[fstart + w..pos - w];
        } else {
            bad = true;
        }
        nf += 1;
        if !bad && nf == N {
            f(&fields);
        }
    }
}


#[allow(dead_code)]
fn parse_bench(path: &Path) {
    let data = fs::read(path).unwrap();
    let cut = data.iter().rposition(|&b| b == b'\n').unwrap() + 1;
    let mut buf = data[..cut].to_vec();
    buf.extend_from_slice(&[0u8; 64]);
    let data = &buf[..cut];
    let lines = data.iter().filter(|&&b| b == b'\n').count() as f64;
    let mut reg: Reg = Vec::new();
    let mut best = [f64::MAX; 5];
    for _ in 0..7 {
        for v in 0..5 {
            let t = std::time::Instant::now();
            let mut acc = 0u64;
            scan_lines(data, |f: &[&[u8]; 7]| {
                if v >= 1 {
                    acc = acc.wrapping_add(key_of_padded(f[0], &mut reg));
                    acc = acc.wrapping_add(key_of_padded(f[1], &mut reg));
                }
                if v >= 2 {
                    acc = acc.wrapping_add(parse_ts(f[2]).unwrap_or(7) as u64);
                    acc = acc.wrapping_add(parse_ts(f[3]).unwrap_or(9) as u64);
                }
                if v >= 3 {
                    let k = match etype(f[4]) { 0 => 1, 1 => parse_price(f[6]).unwrap_or(3) as u64, _ => 5 };
                    acc = acc.wrapping_add(k);
                }
                if v >= 4 {
                    acc = acc.wrapping_add(f[5].len() as u64);
                }
                acc = acc.wrapping_add(f[2].len() as u64);
            });
            std::hint::black_box(acc);
            best[v] = best[v].min(t.elapsed().as_secs_f64());
        }
    }
    let ns: Vec<String> = best.iter().map(|b| format!("{:.1}", b * 1e9 / lines)).collect();
    eprintln!("scan / +keys / +ts / +validate / +cat: {}", ns.join(" / "));
}

#[allow(dead_code)]
fn scan_bench(path: &Path) {
    let data = fs::read(path).unwrap();
    let data = &data[..data.len().min(400 << 20)];
    let cut = data.iter().rposition(|&b| b == b'\n').unwrap() + 1;
    let data = &data[..cut];
    let (mut bo, mut bn) = (f64::MAX, f64::MAX);
    for _ in 0..7 {
        let t = std::time::Instant::now();
        let mut acc = 0usize;
        scan_lines_old(data, |f: &[&[u8]; 7]| acc = acc.wrapping_add(f[2].len() ^ f[6].len()));
        bo = bo.min(t.elapsed().as_secs_f64());
        let t = std::time::Instant::now();
        let mut acc2 = 0usize;
        scan_lines(data, |f: &[&[u8]; 7]| acc2 = acc2.wrapping_add(f[2].len() ^ f[6].len()));
        bn = bn.min(t.elapsed().as_secs_f64());
        assert_eq!(acc, acc2);
    }
    let lines = data.iter().filter(|&&b| b == b'\n').count() as f64;
    eprintln!("old {:.1} ns/line  new {:.1} ns/line", bo * 1e9 / lines, bn * 1e9 / lines);
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
                        sfence();
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
    evs: Vec<Sv>,
    cks: Vec<Ck>,
    pt: PTab,
    pidx: Vec<u32>,
    cidx: Vec<u32>,
    persons: Vec<u64>,
    eoff: Vec<u32>,
    coff: Vec<u32>,
    cur: Vec<u32>,
}

/// Person -> dense index, open addressing, grows at half load.
struct PTab {
    mask: usize,
    e: Vec<(u64, u32)>,
}
impl PTab {
    fn reset(&mut self, hint: usize) {
        let cap = (hint * 2).next_power_of_two().max(64);
        self.mask = cap - 1;
        self.e.clear();
        self.e.resize(cap, (u64::MAX, 0));
    }
    #[inline(always)]
    fn get_or_insert(&mut self, k: u64, persons: &mut Vec<u64>) -> u32 {
        let mut i = (mix(k) >> 24) as usize & self.mask;
        loop {
            let x = self.e[i];
            if x.0 == k {
                return x.1;
            }
            if x.0 == u64::MAX {
                let ix = persons.len() as u32;
                persons.push(k);
                self.e[i] = (k, ix);
                if persons.len() * 2 > self.e.len() {
                    self.grow(persons);
                }
                return ix;
            }
            i = (i + 1) & self.mask;
        }
    }
    fn grow(&mut self, persons: &[u64]) {
        let cap = self.e.len() * 2;
        self.mask = cap - 1;
        self.e.clear();
        self.e.resize(cap, (u64::MAX, 0));
        for (ix, &k) in persons.iter().enumerate() {
            let mut i = (mix(k) >> 24) as usize & self.mask;
            while self.e[i].0 != u64::MAX {
                i = (i + 1) & self.mask;
            }
            self.e[i] = (k, ix as u32);
        }
    }
    #[inline(always)]
    fn get(&self, k: u64) -> u32 {
        let mut i = (mix(k) >> 24) as usize & self.mask;
        loop {
            let x = self.e[i];
            if x.0 == k {
                return x.1;
            }
            if x.0 == u64::MAX {
                return u32::MAX;
            }
            i = (i + 1) & self.mask;
        }
    }
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
    if let Ok(v) = std::env::var("X_NT") {
        USE_NT.store(v != "0", Ordering::Relaxed);
    }
    if let Ok(p) = std::env::var("X_PARSEBENCH") {
        fill_date_tab(20630, 20660);
        parse_bench(Path::new(&p));
        return;
    }
    if let Ok(p) = std::env::var("X_SCANBENCH") {
        scan_bench(Path::new(&p));
        return;
    }
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
    fill_date_tab(day0 - 10, (we - 1).div_euclid(DAY) + 7);
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
    assert!(ncat <= 256, "too many categories");

    let camps = read_small(&data_dir.join("campaigns.csv"));
    let camp_items: Vec<(Vec<u8>, u32)> = camps.iter().enumerate().map(|(i, (c, _))| (c.clone(), i as u32)).collect();
    let camp_lu = Lookup::new(b"c_", &camp_items);
    let ncamp = camps.len();
    let organic = ncamp as u32;
    lap("small tables");

    // ---- links run on their own thread, concurrently with event/click parsing
    let link_dir = data_dir.join("links");
    let links: Arc<std::sync::OnceLock<(U64Map, HashMap<u64, Vec<u8>>)>> = Arc::new(std::sync::OnceLock::new());
    let links_w = links.clone();
    let link_handle = std::thread::spawn(move || {
        let r = build_links(&link_dir, 2);
        let _ = links_w.set(r);
    });

    // ---- events + clicks
    let ev_maps = map_files(&list_csv(&data_dir.join("events")));
    let ck_maps = map_files(&list_csv(&data_dir.join("clicks")));
    let ev_chunks = chunks(&ev_maps);
    let ck_chunks = chunks(&ck_maps);
    let ev_files = open_files(&list_csv(&data_dir.join("events")));
    let ck_files = open_files(&list_csv(&data_dir.join("clicks")));
    let total_tasks = ev_chunks.len() + ck_chunks.len();
    // bench switches: X_MMAP reads chunks in place; X_STOP exits right after parsing
    // Read chunks straight from the page cache (no copy). The last chunk of a
    // file is copied into a padded buffer, since ID packing over-reads 16 bytes.
    let use_mmap = std::env::var_os("X_MMAP").is_some();
    let stop_after_parse = std::env::var("X_STOP").is_ok();
    let ev_bytes: usize = ev_maps.iter().map(|m| m.len()).sum();
    let ck_bytes: usize = ck_maps.iter().map(|m| m.len()).sum();
    // Bucket counts follow the input size so each bucket's working set
    // (dedup table, person grouping) stays around L2 size: ~40k events each.
    let est_events = ev_bytes / 62 + 1;
    let bits_for = |n: usize| -> u32 { (usize::BITS - (n / 40_000).leading_zeros()).clamp(8, 11) };
    // X_BITS forces a partition width (tests the 60M-scale code path on small data)
    let forced: Option<u32> = std::env::var("X_BITS").ok().and_then(|v| v.parse().ok());
    let ev_bits: u32 = forced.unwrap_or_else(|| bits_for(est_events));
    let pb_bits: u32 = forced.unwrap_or_else(|| bits_for(est_events));
    let n_evb = 1usize << ev_bits;
    let n_pb = 1usize << pb_bits;
    // generous per-thread reservations: untouched capacity costs no page faults
    let ev_res = ev_bytes / 62 / nt / n_evb * 5 / 4 + 64;
    let ck_res = ck_bytes / 45 / nt / n_pb * 3 / 2 + 64;
    let sv_res = ev_bytes / 62 / nt / n_pb * 5 / 4 + 64;

    const BATCH: usize = 32;
    struct ParseSt {
        evb: Buckets<Ev>,
        ckb: Buckets<Ck>,
        reg: Reg,
        buf: Vec<u8>,
        cpend: Vec<Ck>,
    }
    let mut parsed = par_for(
        nt,
        total_tasks,
        || ParseSt {
            evb: Buckets::new(n_evb, ev_res),
            ckb: Buckets::new(n_pb, ck_res),
            reg: Reg::new(),
            buf: Vec::new(),
            cpend: Vec::with_capacity(BATCH),
        },
        |st, i| {
            if i < ev_chunks.len() {
                let c = &ev_chunks[i];
                let mut buf = std::mem::take(&mut st.buf);
                let data: &[u8] = if use_mmap && c.end + 64 <= ev_maps[c.file].len() { &ev_maps[c.file][c.start..c.end] } else { read_chunk(&mut buf, &ev_files[c.file], c) };
                scan_lines(data, |f: &[&[u8]; 7]| {
                    if f[0].is_empty() {
                        return;
                    }
                    let eid = key_of_padded(f[0], &mut st.reg);
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
                    match etype(f[4]) {
                        0 => {}
                        1 => {
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
                        let uid = key_of_padded(f[1], &mut st.reg);
                        let cat = if purchase { prod_lu.get(f[5]).map(|c| c as u16).unwrap_or(unknown_cat) } else { 0 };
                        let b = (mix(eid) >> (64 - ev_bits)) as usize;
                        st.evb.push(b, Ev { eid, person: uid, ts: (ts - ws) as u32, ingest, price, cat, flags });
                    } else {
                        let b = (mix(eid) >> (64 - ev_bits)) as usize;
                        st.evb.push(b, Ev { eid, person: 0, ts: 0, ingest, price: 0, cat: 0, flags });
                    }
                });
                st.buf = buf;
            } else {
                let c = &ck_chunks[i - ev_chunks.len()];
                // click chunks are queued after every event chunk; the link
                // thread has normally finished by the time one starts
                let (po, _) = links.wait();
                let mut buf = std::mem::take(&mut st.buf);
                let data: &[u8] = if use_mmap && c.end + 64 <= ck_maps[c.file].len() { &ck_maps[c.file][c.start..c.end] } else { read_chunk(&mut buf, &ck_files[c.file], c) };
                scan_lines(data, |f: &[&[u8]; 4]| {
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
                    let cid = key_of_padded(f[0], &mut st.reg);
                    let uid = key_of_padded(f[1], &mut st.reg);
                    po.prefetch(uid);
                    st.cpend.push(Ck { person: uid, ts, cid, camp });
                    if st.cpend.len() == BATCH {
                        for k in 0..BATCH {
                            let mut c = st.cpend[k];
                            c.person = po.get(c.person).unwrap_or(c.person);
                            st.ckb.push((mix(c.person) >> (64 - pb_bits)) as usize, c);
                        }
                        st.cpend.clear();
                    }
                });
                for k in 0..st.cpend.len() {
                    let mut c = st.cpend[k];
                    c.person = po.get(c.person).unwrap_or(c.person);
                    st.ckb.push((mix(c.person) >> (64 - pb_bits)) as usize, c);
                }
                st.cpend.clear();
                st.buf = buf;
            }
        },
    );
    for st in &mut parsed {
        st.evb.finish();
        st.ckb.finish();
    }
    lap("parse events+clicks");
    if stop_after_parse {
        unsafe { libc::_exit(0) };
    }
    link_handle.join().unwrap();
    let (person_of, mut reg) = Arc::try_unwrap(links).ok().expect("links still shared").into_inner().expect("links built");
    for st in &mut parsed {
        for (k, b) in st.reg.drain(..) {
            reg.entry(k).or_insert(b);
        }
    }
    lap("links joined");
    let person_of_ref = &person_of;

    // clicks were resolved and partitioned by person during parsing
    let ck_parts: Vec<Buckets<Ck>> = parsed.iter_mut().map(|st| std::mem::replace(&mut st.ckb, Buckets::new(1, 1))).collect();

    // ---- dedup
    let parsed_ref = &parsed;
    let dedup = par_for(
        nt,
        n_evb,
        || (Buckets::<Sv>::new(n_pb, sv_res), Vec::<(u64, i32, u32)>::new(), Vec::<u32>::new(), Vec::<Sv>::with_capacity(BATCH)),
        |(out, table, losers, pend), b| {
            let n: usize = parsed_ref.iter().map(|s| s.evb.len(b)).sum();
            if n == 0 {
                return;
            }
            let cap = (n + n / 2).next_power_of_two().max(64);
            let mask = cap - 1;
            let t0 = std::time::Instant::now();
            table.clear();
            table.resize(cap, (u64::MAX, 0, 0));
            losers.clear();
            // pass 1: (eid -> min ingest, winner id); record every loser id
            for (ti, s) in parsed_ref.iter().enumerate() {
                let (m0, m1) = s.evb.parts(b);
                for (i, e) in m0.iter().chain(m1.iter()).enumerate() {
                    let id = ((ti as u32) << 26) | i as u32;
                    let mut h = (mix(e.eid) >> 8) as usize & mask;
                    loop {
                        let t = &mut table[h];
                        if t.0 == u64::MAX {
                            *t = (e.eid, e.ingest, id);
                            break;
                        }
                        if t.0 == e.eid {
                            if e.ingest < t.1 {
                                losers.push(t.2);
                                t.1 = e.ingest;
                                t.2 = id;
                            } else {
                                losers.push(id);
                            }
                            break;
                        }
                        h = (h + 1) & mask;
                    }
                }
            }
            let t0 = prof(0, t0);
            losers.sort_unstable();
            let t0 = prof(1, t0);
            // pass 2: sequential scan, skipping losers and invalid kept copies
            let mut li = 0usize;
            for (ti, s) in parsed_ref.iter().enumerate() {
                let (m0, m1) = s.evb.parts(b);
                for (i, e) in m0.iter().chain(m1.iter()).enumerate() {
                    let id = ((ti as u32) << 26) | i as u32;
                    if li < losers.len() && losers[li] == id {
                        li += 1;
                        continue;
                    }
                    if e.flags & 1 == 0 {
                        continue;
                    }
                    person_of_ref.prefetch(e.person);
                    pend.push(Sv { person: e.person, ts: e.ts, pc: if e.flags & 2 != 0 { (e.price << 8) | e.cat as u32 } else { 0 } });
                    if pend.len() == BATCH {
                        for mut v in pend.drain(..) {
                            v.person = person_of_ref.get(v.person).unwrap_or(v.person);
                            out.push((mix(v.person) >> (64 - pb_bits)) as usize, v);
                        }
                    }
                }
            }
            for mut v in pend.drain(..) {
                v.person = person_of_ref.get(v.person).unwrap_or(v.person);
                out.push((mix(v.person) >> (64 - pb_bits)) as usize, v);
            }
            prof(2, t0);
        },
    );
    for st in &mut parsed {
        st.evb = Buckets::new(1, 1);
    }
    let mut dedup = dedup;
    for d in &mut dedup {
        d.0.finish();
    }
    lap("dedup");

    // ---- sessions
    let dedup_ref = &dedup;
    let ck_parts_ref = &ck_parts;
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
            evs: Vec::new(),
            cks: Vec::new(),
            pt: PTab { mask: 0, e: Vec::new() },
            pidx: Vec::new(),
            cidx: Vec::new(),
            persons: Vec::new(),
            eoff: Vec::new(),
            coff: Vec::new(),
            cur: Vec::new(),
        },
        |st, b| {
            // Group by person in linear time: dense person index via a small
            // hash table, counting sort by index, then sort each person's few
            // records by time.
            let n: usize = dedup_ref.iter().map(|d| d.0.len(b)).sum();
            if n == 0 {
                return;
            }
            let t0 = std::time::Instant::now();
            let mut evs = std::mem::take(&mut st.evs);
            let mut cks = std::mem::take(&mut st.cks);
            st.pt.reset(n / 4 + 64);
            st.pidx.clear();
            st.persons.clear();
            for (o, _, _, _) in dedup_ref.iter() {
                let (m0, m1) = o.parts(b);
                for e in m0.iter().chain(m1.iter()) {
                    let ix = st.pt.get_or_insert(e.person, &mut st.persons);
                    st.pidx.push(ix);
                }
            }
            let np = st.persons.len();
            st.eoff.clear();
            st.eoff.resize(np + 1, 0);
            for &ix in &st.pidx {
                st.eoff[ix as usize + 1] += 1;
            }
            for k in 0..np {
                st.eoff[k + 1] += st.eoff[k];
            }
            st.cur.clear();
            st.cur.extend_from_slice(&st.eoff[..np]);
            evs.clear();
            evs.reserve(n);
            unsafe { evs.set_len(n) };
            let mut k = 0usize;
            for (o, _, _, _) in dedup_ref.iter() {
                let (m0, m1) = o.parts(b);
                for e in m0.iter().chain(m1.iter()) {
                    let ix = st.pidx[k] as usize;
                    k += 1;
                    let at = st.cur[ix];
                    st.cur[ix] = at + 1;
                    evs[at as usize] = *e;
                }
            }
            for q in 0..np {
                let (x, y) = (st.eoff[q] as usize, st.eoff[q + 1] as usize);
                if y - x > 1 {
                    evs[x..y].sort_unstable_by_key(|e| e.ts);
                }
            }
            let t0 = prof(3, t0);
            // clicks of the same people (clicks of people without events are dropped)
            st.cidx.clear();
            st.coff.clear();
            st.coff.resize(np + 1, 0);
            for s in ck_parts_ref {
                let (m0, m1) = s.parts(b);
                for c in m0.iter().chain(m1.iter()) {
                    let ix = st.pt.get(c.person);
                    st.cidx.push(ix);
                    if ix != u32::MAX {
                        st.coff[ix as usize + 1] += 1;
                    }
                }
            }
            for q in 0..np {
                st.coff[q + 1] += st.coff[q];
            }
            let nc = st.coff[np] as usize;
            st.cur.clear();
            st.cur.extend_from_slice(&st.coff[..np]);
            cks.clear();
            cks.reserve(nc);
            unsafe { cks.set_len(nc) };
            let mut k = 0usize;
            for s in ck_parts_ref {
                let (m0, m1) = s.parts(b);
                for c in m0.iter().chain(m1.iter()) {
                    let ix = st.cidx[k];
                    k += 1;
                    if ix == u32::MAX {
                        continue;
                    }
                    let at = st.cur[ix as usize];
                    st.cur[ix as usize] = at + 1;
                    cks[at as usize] = *c;
                }
            }
            for q in 0..np {
                let (x, y) = (st.coff[q] as usize, st.coff[q + 1] as usize);
                if y - x > 1 {
                    cks[x..y].sort_unstable_by_key(|c| c.ts);
                }
            }
            let t0 = prof(4, t0);
            for q in 0..np {
                let p = st.persons[q];
                let (i, j) = (st.eoff[q] as usize, st.eoff[q + 1] as usize);
                let pc = &cks[st.coff[q] as usize..st.coff[q + 1] as usize];
                let (mut lo, mut hi) = (0usize, 0usize);
                let mut cur_day: i64 = i64::MIN;
                let (mut d_sess, mut d_events, mut d_rev) = (0i64, 0i64, 0i64);

                let mut k = i;
                while k < j {
                    let start = ws + evs[k].ts as i64;
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
                    let mut last = evs[k].ts;
                    let mut m = k;
                    let mut rev = 0i64;
                    let mut purch = false;
                    while m < j && (evs[m].ts - last) as i64 <= GAP {
                        let e = &evs[m];
                        last = e.ts;
                        if e.pc != 0 {
                            let price = (e.pc >> 8) as i64;
                            let c = (e.pc & 255) as usize;
                            purch = true;
                            rev += price;
                            if st.catsum[c] == 0 {
                                st.touched.push(c as u16);
                            }
                            st.catsum[c] += price;
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
            }
            prof(5, t0);
            st.evs = evs;
            st.cks = cks;
        },
    );
    drop(dedup);
    lap("sessions");

    // ---- output
    let crate_pq = std::env::var_os("X_PQCRATE").is_some();
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
            sc.spawn(move || if crate_pq { write_user(&path, &st.rows, cat_names, reg) } else { write_user_fast(&path, &st.rows, cat_names, reg) });
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
        if crate_pq { write_campaign(&cdd.join("part-000.parquet"), &rows, &camps, day0) } else { write_campaign_fast(&cdd.join("part-000.parquet"), &rows, &camps, day0) }
    });
    lap("write");
    if std::env::var_os("X_PROF").is_some() {
        let names = ["dedup pass1", "dedup sort", "dedup pass2", "sess group ev", "sess group ck", "sess walk"];
        for (i, n) in names.iter().enumerate() {
            eprintln!("{:>14}: {:7.1} ms cpu", n, PROF[i].load(Ordering::Relaxed) as f64 / 1e6);
        }
    }
    // PGO training runs must exit normally so the profile gets written.
    if std::env::var_os("X_PGO_TRAIN").is_some() {
        std::process::exit(0);
    }
    // Skip destructors and allocator teardown: the kernel reclaims everything.
    unsafe { libc::_exit(0) };
}


/// Parse links, union-find, and map every linked ID to its person name.
fn build_links(dir: &Path, nt: usize) -> (U64Map, HashMap<u64, Vec<u8>>) {
    let link_maps = map_files(&list_csv(dir));
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
    // at most 2 * npairs distinct IDs; this keeps the load at or under 2/3
    let mut idx = U64Map::with_capacity(npairs + npairs / 2 + 16);
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
    // Only IDs whose person is another ID are stored: a miss means "itself".
    // Fewer entries keep the table small enough to stay mostly in cache.
    let roots: Vec<usize> = (0..ids.len()).map(|i| find(&mut parent, i as u32) as usize).collect();
    let moved = (0..ids.len()).filter(|&i| name[roots[i]] != ids[i]).count();
    let mut person_of = U64Map::with_capacity(moved * 3 / 4 + 16);
    for i in 0..ids.len() {
        let p = name[roots[i]];
        if p != ids[i] {
            person_of.insert(ids[i], p);
        }
    }
    eprintln!("links: {} pairs, {} ids, {} stored, {} slots", npairs, ids.len(), moved, person_of.mask + 1);
    (person_of, reg)
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
            .set_write_batch_size(1 << 20)
            .set_data_page_row_count_limit(1 << 22)
            .set_data_page_size_limit(64 << 20)
            .build(),
    )
}

fn write_user(path: &Path, rows: &[UserRow], cats: &[Vec<u8>], reg: &HashMap<u64, Vec<u8>>) {
    let schema = Arc::new(
        parse_message_type(
            "message user_daily { REQUIRED BYTE_ARRAY user_id (UTF8); REQUIRED INT32 day (DATE); REQUIRED INT32 sessions; REQUIRED INT32 events; REQUIRED INT64 revenue_cents; OPTIONAL BYTE_ARRAY top_category (UTF8); }",
        )
        .unwrap(),
    );
    let f = std::io::BufWriter::with_capacity(8 << 20, File::create(path).unwrap());
    let mut w = SerializedFileWriter::new(f, schema, props()).unwrap();
    let mut rg = w.next_row_group().unwrap();
    let mut col = 0;
    let cat_ba: Vec<ByteArray> = cats.iter().map(|c| ByteArray::from(c.clone())).collect();
    while let Some(mut c) = rg.next_column().unwrap() {
        match col {
            0 => {
                let mut buf: Vec<u8> = Vec::with_capacity(rows.len() * 10);
                let mut ends: Vec<usize> = Vec::with_capacity(rows.len());
                for r in rows {
                    if r.person & FALLBACK == 0 {
                        unpack(r.person, &mut buf);
                    } else {
                        buf.extend_from_slice(&bytes_of(r.person, reg));
                    }
                    ends.push(buf.len());
                }
                let all = bytes::Bytes::from(buf);
                let mut st = 0;
                let v: Vec<ByteArray> = ends
                    .iter()
                    .map(|&e| {
                        let x = ByteArray::from(all.slice(st..e));
                        st = e;
                        x
                    })
                    .collect();
                c.typed::<ByteArrayType>().write_batch(&v, None, None).unwrap();
            }
            1 => {
                let v: Vec<i32> = rows.iter().map(|r| r.day).collect();
                c.typed::<Int32Type>().write_batch(&v, None, None).unwrap();
            }
            2 | 3 => {
                let v: Vec<i32> = rows.iter().map(|r| if col == 2 { r.sessions as i32 } else { r.events as i32 }).collect();
                c.typed::<Int32Type>().write_batch(&v, None, None).unwrap();
            }
            4 => {
                let v: Vec<i64> = rows.iter().map(|r| r.revenue).collect();
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

fn write_user_fast(path: &Path, rows: &[UserRow], cats: &[Vec<u8>], reg: &HashMap<u64, Vec<u8>>) {
    let n = rows.len();
    let mut ub: Vec<u8> = Vec::with_capacity(n * 10);
    let mut ue: Vec<u32> = Vec::with_capacity(n);
    for r in rows {
        if r.person & FALLBACK == 0 {
            unpack(r.person, &mut ub);
        } else {
            ub.extend_from_slice(&bytes_of(r.person, reg));
        }
        ue.push(ub.len() as u32);
    }
    let mut cb: Vec<u8> = Vec::new();
    let mut ce: Vec<u32> = Vec::new();
    let mut defs: Vec<bool> = Vec::with_capacity(n);
    for r in rows {
        defs.push(r.top >= 0);
        if r.top >= 0 {
            cb.extend_from_slice(&cats[r.top as usize]);
            ce.push(cb.len() as u32);
        }
    }
    let cols = [
        pq::Col::Str { name: "user_id", bytes: ub, ends: ue, defs: None },
        pq::Col::I32 { name: "day", date: true, v: rows.iter().map(|r| r.day).collect() },
        pq::Col::I32 { name: "sessions", date: false, v: rows.iter().map(|r| r.sessions as i32).collect() },
        pq::Col::I32 { name: "events", date: false, v: rows.iter().map(|r| r.events as i32).collect() },
        pq::Col::I64 { name: "revenue_cents", v: rows.iter().map(|r| r.revenue).collect() },
        pq::Col::Str { name: "top_category", bytes: cb, ends: ce, defs: Some(defs) },
    ];
    pq::write(path, n, &cols).unwrap();
}

fn write_campaign_fast(path: &Path, rows: &[(usize, usize, [i64; 4])], camps: &[(Vec<u8>, Vec<u8>)], day0: i64) {
    let ncamp = camps.len();
    let (mut ib, mut ie, mut chb, mut che) = (Vec::new(), Vec::new(), Vec::new(), Vec::new());
    for (ci, _, _) in rows {
        if *ci == ncamp {
            ib.extend_from_slice(b"organic");
            chb.extend_from_slice(b"organic");
        } else {
            ib.extend_from_slice(&camps[*ci].0);
            chb.extend_from_slice(&camps[*ci].1);
        }
        ie.push(ib.len() as u32);
        che.push(chb.len() as u32);
    }
    let q = |k: usize| rows.iter().map(|(_, _, r)| r[k]).collect::<Vec<i64>>();
    let cols = [
        pq::Col::Str { name: "campaign_id", bytes: ib, ends: ie, defs: None },
        pq::Col::Str { name: "channel", bytes: chb, ends: che, defs: None },
        pq::Col::I32 { name: "day", date: true, v: rows.iter().map(|(_, d, _)| (day0 + *d as i64) as i32).collect() },
        pq::Col::I64 { name: "sessions", v: q(0) },
        pq::Col::I64 { name: "purchasing_sessions", v: q(1) },
        pq::Col::I64 { name: "revenue_cents", v: q(2) },
        pq::Col::I64 { name: "assisted_sessions", v: q(3) },
    ];
    pq::write(path, rows.len(), &cols).unwrap();
}

fn write_campaign(path: &Path, rows: &[(usize, usize, [i64; 4])], camps: &[(Vec<u8>, Vec<u8>)], day0: i64) {
    let schema = Arc::new(
        parse_message_type(
            "message campaign_daily { REQUIRED BYTE_ARRAY campaign_id (UTF8); REQUIRED BYTE_ARRAY channel (UTF8); REQUIRED INT32 day (DATE); REQUIRED INT64 sessions; REQUIRED INT64 purchasing_sessions; REQUIRED INT64 revenue_cents; REQUIRED INT64 assisted_sessions; }",
        )
        .unwrap(),
    );
    let f = std::io::BufWriter::with_capacity(8 << 20, File::create(path).unwrap());
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

#[cfg(test)]
mod tests {
    use super::*;
    fn collect<const N: usize>(d: &[u8], fast: bool) -> Vec<Vec<Vec<u8>>> {
        let mut out = vec![];
        if fast {
            scan_lines::<N>(d, |f| out.push(f.iter().map(|x| x.to_vec()).collect()));
        } else {
            scan_lines_ref::<N>(d, |f| out.push(f.iter().map(|x| x.to_vec()).collect()));
        }
        out
    }
    fn parse_ts_ref(s: &[u8]) -> Option<i64> {
        let n = s.len();
        if n == 0 { return None; }
        if n >= 20 && s[4] == b'-' {
            if n == 20 { if s[19] != b'Z' { return None; } return civil_slow(s); }
            if n == 25 {
                let sign = match s[19] { b'+' => 1, b'-' => -1, _ => return None };
                if s[22] != b':' { return None; }
                let oh = dig2(&s[20..22])?; let om = dig2(&s[23..25])?;
                if oh > 23 || om > 59 { return None; }
                return Some(civil_slow(s)? - sign * (oh * 60 + om) * 60_000);
            }
            return None;
        }
        let mut v: i64 = 0; let mut over = false;
        for &c in s {
            let d = c.wrapping_sub(b'0');
            if d >= 10 { return None; }
            if !over { match v.checked_mul(10).and_then(|x| x.checked_add(d as i64)) { Some(x) => v = x, None => over = true } }
        }
        Some(if over { i64::MAX } else { v })
    }
    #[test]
    fn parse_ts_matches_reference() {
        fill_date_tab(20630, 20660);
        let mut x: u64 = 777;
        let mut rnd = || { x ^= x << 13; x ^= x >> 7; x ^= x << 17; x };
        let noise = b"0123456789:-TZ+ /a\x00\xff";
        for _ in 0..2_000_000 {
            let base = 1_783_000_000_000i64 + (rnd() % 1_500_000_000) as i64;
            let secs = base / 1000;
            let day = secs.div_euclid(86400);
            let z = day + 719_468; let era = z.div_euclid(146_097); let doe = z - era * 146_097;
            let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
            let doy = doe - (365 * yoe + yoe / 4 - yoe / 100); let mp = (5 * doy + 2) / 153;
            let d = doy - (153 * mp + 2) / 5 + 1; let m = if mp < 10 { mp + 3 } else { mp - 9 };
            let y = yoe + era * 400 + (m <= 2) as i64;
            let tod = secs.rem_euclid(86400);
            let mut s = match rnd() % 3 {
                0 => format!("{:04}-{:02}-{:02}T{:02}:{:02}:{:02}Z", y, m, d, tod / 3600, tod / 60 % 60, tod % 60).into_bytes(),
                1 => format!("{:04}-{:02}-{:02}T{:02}:{:02}:{:02}+0{}:30", y, m, d, tod / 3600, tod / 60 % 60, tod % 60, rnd() % 10).into_bytes(),
                _ => format!("{}", base).into_bytes(),
            };
            let k = rnd() % 4;
            for _ in 0..k {
                if s.is_empty() { break; }
                let p = (rnd() as usize) % s.len();
                match rnd() % 3 {
                    0 => s[p] = noise[(rnd() as usize) % noise.len()],
                    1 => if rnd() % 4 == 0 { s.remove(p); },
                    _ => if rnd() % 8 == 0 { s.insert(p, b'9'); },
                }
            }
            assert_eq!(parse_ts(&s), parse_ts_ref(&s), "{:?}", String::from_utf8_lossy(&s));
        }
        for t in [&b"9999999999999"[..], b"0000000000000", b"99999999999999999999", b"2026-02-30T00:00:00Z", b"2026-07-10T24:00:00Z", b"2026-07-10T23:59:60Z"] {
            assert_eq!(parse_ts(t), parse_ts_ref(t));
        }
    }
    #[test]
    fn etype_matches() {
        for (s, e) in [(&b"view"[..], 0), (b"cart", 0), (b"purchase", 1), (b"View", 2), (b"", 2), (b"purchas", 2), (b"purchasee", 2), (b"vie", 2), (b" view", 2)] {
            assert_eq!(etype(s), e);
        }
    }
    #[test]
    fn scanner_matches_reference() {
        let pieces: [&[u8]; 18] = [b"a\"b", b"\"a\"\"", b"\"\"x", b"x\"\"", b"ab", b"\"x\"", b"\"\"", b"\"", b"", b"long_field_value_that_goes_on_and_on_and_on_and_on_xx", b"\r", b" a ", b"\"a\"b\"", b"1234567890", b"\"q", b"q\"", b"z", b"\"\"\""];
        let mut x: u64 = 99;
        let mut rnd = || { x ^= x << 13; x ^= x >> 7; x ^= x << 17; x };
        for _ in 0..60000 {
            let mut d = vec![];
            let lines = rnd() % 40;
            for _ in 0..lines {
                let nf = 1 + rnd() % 5;
                for k in 0..nf {
                    if k > 0 { d.push(b','); }
                    d.extend_from_slice(pieces[(rnd() % 18) as usize]);
                }
                if rnd() % 50 != 0 { d.push(b'\n'); } else { d.extend_from_slice(b"\n\n"); }
            }
            if rnd() % 3 == 0 { d.pop(); }
            assert_eq!(collect::<2>(&d, true), collect::<2>(&d, false));
            assert_eq!(collect::<3>(&d, true), collect::<3>(&d, false));
            assert_eq!(collect::<4>(&d, true), collect::<4>(&d, false));
        }
    }
    #[test]
    fn pack_padded_matches_pack() {
        let alphabet = b"0123456789abcdefghijklmnopqrstuvwxyzAZ _\"\r,-";
        let mut x: u64 = 12345;
        for _ in 0..200_000 {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            let n = (x % 13) as usize;
            let mut buf = [b'q'; 32];
            for i in 0..n {
                x ^= x << 13;
                x ^= x >> 7;
                x ^= x << 17;
                let pool = if x % 10 == 0 { &alphabet[..] } else { &alphabet[..36] };
                buf[i] = pool[(x >> 8) as usize % pool.len()];
            }
            assert_eq!(pack_padded(&buf[..n]), pack(&buf[..n]), "{:?}", &buf[..n]);
        }
    }
}
