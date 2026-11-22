//! Linked IDs on all threads.
//!
//! 1. Parse the links files in parallel into (key, key) pairs; long IDs keep
//!    their bytes in per-thread name logs.
//! 2. Insert every ID into one shared open-addressing table with CAS; a slot's
//!    index is the node id. Pairs become (slot, slot).
//! 3. Union in parallel with CAS on a parent array, always hanging the root
//!    with the larger ID under the root with the smaller one (byte order).
//!    Keys only decrease along a path, so each set's root is its smallest ID,
//!    which is the person's name.
//! 4. Resolve every node's root (path halving), then store the root's key
//!    next to the node's key, so a lookup is one probe sequence.

use super::*;
use std::sync::atomic::{AtomicU32, AtomicU64};

pub struct PTable {
    /// interleaved [key, value]; key 0 = empty; value 0 = "the ID itself",
    /// otherwise the person's key
    e: Vec<[AtomicU64; 2]>,
    mask: usize,
}

impl PTable {
    #[inline(always)]
    fn start(&self, k: u64) -> usize {
        (mix(k) >> 20) as usize & self.mask
    }
    #[inline(always)]
    pub fn prefetch(&self, k: u64) {
        #[cfg(target_arch = "x86_64")]
        unsafe {
            use std::arch::x86_64::*;
            _mm_prefetch(self.e.as_ptr().add(self.start(k)) as *const i8, _MM_HINT_T0);
        }
    }
    /// The person's key for a linked ID other than the person itself.
    #[inline(always)]
    pub fn get(&self, k: u64) -> Option<u64> {
        let mut i = self.start(k);
        loop {
            let s = unsafe { self.e.get_unchecked(i) };
            let x = s[0].load(Ordering::Relaxed);
            if x == k {
                let v = s[1].load(Ordering::Relaxed);
                return if v == 0 { None } else { Some(v) };
            }
            if x == 0 {
                return None;
            }
            i = (i + 1) & self.mask;
        }
    }
    /// Insert (or find) `k`; returns its slot.
    fn insert(&self, k: u64) -> usize {
        let mut i = self.start(k);
        loop {
            let s = &self.e[i][0];
            let x = s.load(Ordering::Relaxed);
            if x == k {
                return i;
            }
            if x == 0 {
                match s.compare_exchange(0, k, Ordering::Relaxed, Ordering::Relaxed) {
                    Ok(_) => return i,
                    Err(y) if y == k => return i,
                    Err(_) => {}
                }
            }
            i = (i + 1) & self.mask;
        }
    }
    pub fn slots(&self) -> usize {
        self.mask + 1
    }
}

/// Packed keys compare as numbers; long keys compare by their bytes.
#[inline]
fn id_less(a: u64, b: u64, names: &AllNames) -> bool {
    if (a | b) & FALLBACK == 0 {
        return a < b;
    }
    let mut ba = [0u8; 10];
    let mut bb = [0u8; 10];
    fn view<'x>(k: u64, buf: &'x mut [u8; 10], names: &'x AllNames) -> &'x [u8] {
        if k & FALLBACK == 0 {
            let mut n = 0;
            for i in (0..10).rev() {
                let r = ((k >> (6 * i)) & 63) as u8;
                if r == 0 {
                    break;
                }
                buf[n] = if r <= 10 { b'0' + r - 1 } else { b'a' + r - 11 };
                n += 1;
            }
            &buf[..n]
        } else {
            names.get(k).unwrap_or(&[])
        }
    }
    view(a, &mut ba, names) < view(b, &mut bb, names)
}

#[inline]
fn find(par: &[AtomicU32], mut x: u32) -> u32 {
    loop {
        let p = par[x as usize].load(Ordering::Relaxed);
        if p == 0 {
            return x;
        }
        let px = p - 1;
        let g = par[px as usize].load(Ordering::Relaxed);
        if g == 0 {
            return px;
        }
        // path halving: point x at its grandparent (an ancestor either way)
        let _ = par[x as usize].compare_exchange(p, g, Ordering::Relaxed, Ordering::Relaxed);
        x = g - 1;
    }
}

/// Build the person table. Returns it with the links' long-ID name logs.
pub fn build(dir: &Path, nt: usize) -> (PTable, Vec<Names>) {
    let maps = map_files(&list_csv(dir));
    let chs = chunks(&maps);
    let bytes: usize = maps.iter().map(|m| m.len()).sum();
    // parse
    let mut parts = par_for(
        nt,
        chs.len(),
        || (Vec::<(u64, u64)>::new(), Names::new()),
        |(pairs, names), i| {
            let c = &chs[i];
            scan_lines(&maps[c.file][c.start..c.end], |f: &[&[u8]; 2]| {
                if f[0].is_empty() || f[1].is_empty() {
                    return;
                }
                let a = key_named_exact(f[0], names);
                let b = key_named_exact(f[1], names);
                if a != b {
                    pairs.push((a, b));
                }
            });
        },
    );
    let npairs: usize = parts.iter().map(|p| p.0.len()).sum();
    // names first: the union step compares long IDs by their bytes
    let mut logs: Vec<Names> = parts.iter_mut().map(|p| std::mem::replace(&mut p.1, Names::new())).collect();
    std::thread::scope(|sc| {
        for l in logs.iter_mut() {
            if !l.ents.is_empty() {
                sc.spawn(move || l.finalize());
            }
        }
    });
    let names = AllNames { links: KeyMap::default(), parts: logs };
    let _ = bytes;
    let cap = ((npairs * 2) * 2).next_power_of_two().max(1024);
    // zero-filled allocations (fresh pages from the kernel, no serial memset)
    let e: Vec<[AtomicU64; 2]> = unsafe {
        let mut v = std::mem::ManuallyDrop::new(vec![0u64; cap * 2]);
        Vec::from_raw_parts(v.as_mut_ptr() as *mut [AtomicU64; 2], cap, cap)
    };
    let tab = PTable { e, mask: cap - 1 };
    // insert all IDs; pairs become slot pairs
    let tab_ref = &tab;
    let slot_pairs: Vec<Vec<(u32, u32)>> = std::thread::scope(|sc| {
        let hs: Vec<_> = parts
            .iter()
            .map(|(p, _)| sc.spawn(move || p.iter().map(|&(a, b)| (tab_ref.insert(a) as u32, tab_ref.insert(b) as u32)).collect::<Vec<_>>()))
            .collect();
        hs.into_iter().map(|h| h.join().unwrap()).collect()
    });
    drop(parts);
    // union
    let par: Vec<AtomicU32> = unsafe {
        let mut v = std::mem::ManuallyDrop::new(vec![0u32; cap]);
        Vec::from_raw_parts(v.as_mut_ptr() as *mut AtomicU32, cap, cap)
    };
    let par_ref = &par;
    let names_ref = &names;
    let key = |s: u32| tab_ref.e[s as usize][0].load(Ordering::Relaxed);
    std::thread::scope(|sc| {
        for sp in &slot_pairs {
            sc.spawn(move || {
                for &(a, b) in sp {
                    loop {
                        let ra = find(par_ref, a);
                        let rb = find(par_ref, b);
                        if ra == rb {
                            break;
                        }
                        let (lo, hi) = if id_less(key(ra), key(rb), names_ref) { (ra, rb) } else { (rb, ra) };
                        if par_ref[hi as usize].compare_exchange(0, lo + 1, Ordering::Relaxed, Ordering::Relaxed).is_ok() {
                            break;
                        }
                    }
                }
            });
        }
    });
    drop(slot_pairs);
    // roots, then person keys next to the IDs
    let step = (cap + nt - 1) / nt;
    let roots: Vec<Vec<u32>> = std::thread::scope(|sc| {
        let hs: Vec<_> = (0..nt)
            .map(|t| {
                sc.spawn(move || {
                    let (lo, hi) = (t * step, ((t + 1) * step).min(cap));
                    (lo..hi).map(|i| if tab_ref.e[i][0].load(Ordering::Relaxed) == 0 { u32::MAX } else { find(par_ref, i as u32) }).collect::<Vec<u32>>()
                })
            })
            .collect();
        hs.into_iter().map(|h| h.join().unwrap()).collect()
    });
    std::thread::scope(|sc| {
        for (t, rs) in roots.iter().enumerate() {
            sc.spawn(move || {
                let lo = t * step;
                for (j, &r) in rs.iter().enumerate() {
                    let i = lo + j;
                    if r != u32::MAX && r as usize != i {
                        tab_ref.e[i][1].store(tab_ref.e[r as usize][0].load(Ordering::Relaxed), Ordering::Relaxed);
                    }
                }
            });
        }
    });
    eprintln!("links: {} pairs, {} slots", npairs, cap);
    (tab, names.parts)
}
