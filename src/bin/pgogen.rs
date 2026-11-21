//! Training-data generator for profile-guided optimisation (used only inside
//! the Docker build). Writes a small dataset in the track's layout with the
//! same shapes the real data has: quoted fields, three timestamp forms,
//! duplicate copies, bad rows, links and clicks. Not a copy of the official
//! generator; just enough variety to exercise every hot path.
//!
//! Usage: pgogen OUT_DIR EVENTS

use std::fs::{self, File};
use std::io::{BufWriter, Write};

struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
    fn chance(&mut self, p: f64) -> bool {
        (self.next() >> 11) as f64 / (1u64 << 53) as f64 <= p
    }
}

/// 8 base-36 digits from a scrambled value, like the real IDs (9 chars with the prefix).
fn b36(x: u64, prefix: char) -> String {
    const LO: u64 = 78_364_164_096; // 36^7
    const SPAN: u64 = 2_821_109_907_456 - LO; // 36^8 - 36^7
    let v = x.wrapping_mul(0x9E37_79B9_7F4A_7C15).rotate_left(23) % SPAN + LO;
    b36raw(v, prefix)
}

fn b36raw(mut v: u64, prefix: char) -> String {
    const D: &[u8] = b"0123456789abcdefghijklmnopqrstuvwxyz";
    let mut s = Vec::new();
    loop {
        s.push(D[(v % 36) as usize]);
        v /= 36;
        if v == 0 {
            break;
        }
    }
    s.reverse();
    format!("{}{}", prefix, String::from_utf8(s).unwrap())
}

fn civil(ms: i64) -> (i64, i64, i64, i64, i64, i64) {
    let days = ms.div_euclid(86_400_000);
    let sod = ms.rem_euclid(86_400_000) / 1000;
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = yoe + era * 400 + (m <= 2) as i64;
    (y, m, d, sod / 3600, sod / 60 % 60, sod % 60)
}

fn f1(ms: i64) -> String {
    let (y, m, d, h, mi, s) = civil(ms);
    format!("{y:04}-{m:02}-{d:02}T{h:02}:{mi:02}:{s:02}Z")
}

fn ts(r: &mut Rng, ms: i64) -> String {
    match r.below(100) {
        0..=57 => f1(ms - ms % 1000),
        58..=67 => {
            let off = (r.below(23) as i64 - 10) * 60;
            let local = ms - ms % 1000 + off * 60_000;
            let (y, m, d, h, mi, s) = civil(local);
            let sign = if off < 0 { '-' } else { '+' };
            let a = off.abs();
            format!("{y:04}-{m:02}-{d:02}T{h:02}:{mi:02}:{s:02}{sign}{:02}:{:02}", a / 60, a % 60)
        }
        _ => format!("{ms}"),
    }
}

fn q(r: &mut Rng, s: String) -> String {
    if r.chance(0.2) {
        format!("\"{s}\"")
    } else {
        s
    }
}

fn main() {
    let a: Vec<String> = std::env::args().collect();
    let out = std::path::PathBuf::from(&a[1]);
    let n: u64 = a[2].parse().unwrap();
    let mut r = Rng(0x9E37_79B9_7F4A_7C15);
    let ws: i64 = 1_798_761_600_000; // a midnight in 2027
    let we = ws + 14 * 86_400_000;
    for d in ["events", "clicks", "links"] {
        fs::create_dir_all(out.join(d)).unwrap();
    }
    fs::write(
        out.join("manifest.json"),
        format!("{{\"window_start\": \"{}\", \"window_end\": \"{}\"}}\n", f1(ws), f1(we)),
    )
    .unwrap();
    let cats = ["apparel", "books", "electronics", "garden", "home", "toys"];
    let mut p = BufWriter::new(File::create(out.join("products.csv")).unwrap());
    writeln!(p, "product_id,category").unwrap();
    for i in 1..=50_000 {
        writeln!(p, "p{i},{}", cats[i % cats.len()]).unwrap();
    }
    let mut c = BufWriter::new(File::create(out.join("campaigns.csv")).unwrap());
    writeln!(c, "campaign_id,channel").unwrap();
    for i in 1..=2000 {
        writeln!(c, "c_{i},{}", ["email", "search", "social"][i % 3]).unwrap();
    }
    let people = n / 30 + 1;
    let files = 8;
    let mut ev: Vec<BufWriter<File>> = (0..files)
        .map(|i| {
            let mut f = BufWriter::new(File::create(out.join("events").join(format!("part-{i:05}.csv"))).unwrap());
            writeln!(f, "event_id,user_id,ts,ingest_ts,event_type,product_id,price_cents").unwrap();
            f
        })
        .collect();
    let mut ck = BufWriter::new(File::create(out.join("clicks").join("part-00000.csv")).unwrap());
    writeln!(ck, "click_id,user_id,campaign_id,ts").unwrap();
    let mut lk = BufWriter::new(File::create(out.join("links").join("part-00000.csv")).unwrap());
    writeln!(lk, "user_id_a,user_id_b").unwrap();
    for pid in 0..people {
        let aliases = 1 + r.below(3);
        for k in 1..aliases {
            writeln!(lk, "{},{}", b36(pid * 512 + 1_000_003, 'u'), q(&mut r, b36(pid * 512 + k + 1_000_003, 'u'))).unwrap();
        }
    }
    // people with a few sessions each, events seconds to minutes apart
    let mut e: u64 = 0;
    while e < n {
        let pid = r.below(people);
        let sessions = 1 + r.below(6);
        for _ in 0..sessions {
            let mut t = ws + r.below((we - ws) as u64) as i64;
            if r.chance(0.6) {
                // a qualifying click shortly before the session
                let ct = t - r.below(7 * 86_400_000) as i64;
                let cts = ts(&mut r, ct);
                let a1 = q(&mut r, b36(e * 3 + 77, 'k'));
                let alias = r.below(2);
                let a2 = q(&mut r, b36(pid * 512 + alias + 1_000_003, 'u'));
                let cn = format!("c_{}", 1 + r.below(2000));
                let a3 = q(&mut r, cn);
                let a4 = q(&mut r, cts);
                writeln!(ck, "{a1},{a2},{a3},{a4}").unwrap();
            }
            let evs = 1 + r.below(10);
            for _ in 0..evs {
                t += 5_000 + r.below(300_000) as i64;
                if t >= we {
                    break;
                }
                let uid = b36(pid * 512 + r.below(2) + 1_000_003, 'u');
                let ing = t + r.below(600_000) as i64;
                let (typ, price) = match r.below(100) {
                    0..=2 => ("purchase", format!("{}", 100 + r.below(50_000))),
                    3..=30 => ("cart", String::new()),
                    _ => ("view", String::new()),
                };
                let copies = if r.chance(0.03) { 2 } else { 1 };
                for k in 0..copies {
                    let tsx = if r.chance(0.005) { "2027-02-30T10:00:00Z".to_string() } else { ts(&mut r, t) };
                    let prod = format!("p{}", 1 + r.below(50_000));
                    let fields = [b36(e * 7 + 13, 'e'), uid.clone(), tsx, f1(ing + k * 5000 - (ing % 1000)), typ.to_string(), prod, price.clone()];
                    let line = fields.into_iter().map(|x| q(&mut r, x)).collect::<Vec<_>>().join(",");
                    let f = r.below(files) as usize;
                    writeln!(ev[f], "{line}").unwrap();
                }
                e += 1;
            }
        }
    }
}
