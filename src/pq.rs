//! Minimal Parquet writer: one row group, one uncompressed PLAIN data page
//! (v1) per column, no statistics, no dictionary. Enough for flat tables of
//! INT32 / INT64 / BYTE_ARRAY columns, REQUIRED or OPTIONAL.

use std::io::Write;

pub enum Col<'a> {
    /// REQUIRED INT32; `date` marks the DATE converted type.
    I32 { name: &'a str, date: bool, v: Vec<i32> },
    I64 { name: &'a str, v: Vec<i64> },
    /// UTF8 strings: concatenated bytes plus end offsets. `defs` = Some for an
    /// OPTIONAL column (one flag per row; only present rows have an end).
    Str { name: &'a str, bytes: Vec<u8>, ends: Vec<u32>, defs: Option<Vec<bool>> },
}

// ---- thrift compact protocol
struct T {
    b: Vec<u8>,
    last: Vec<i16>,
}
const T_I32: u8 = 5;
const T_I64: u8 = 6;
const T_BIN: u8 = 8;
const T_LIST: u8 = 9;
const T_STRUCT: u8 = 12;

impl T {
    fn new() -> Self {
        T { b: Vec::with_capacity(256), last: vec![0] }
    }
    fn varint(&mut self, mut v: u64) {
        while v >= 0x80 {
            self.b.push((v as u8) | 0x80);
            v >>= 7;
        }
        self.b.push(v as u8);
    }
    fn zz(&mut self, v: i64) {
        self.varint(((v << 1) ^ (v >> 63)) as u64);
    }
    fn field(&mut self, id: i16, ty: u8) {
        let last = *self.last.last().unwrap();
        let d = id - last;
        if d > 0 && d <= 15 {
            self.b.push(((d as u8) << 4) | ty);
        } else {
            self.b.push(ty);
            self.zz(id as i64);
        }
        *self.last.last_mut().unwrap() = id;
    }
    fn i32f(&mut self, id: i16, v: i32) {
        self.field(id, T_I32);
        self.zz(v as i64);
    }
    fn i64f(&mut self, id: i16, v: i64) {
        self.field(id, T_I64);
        self.zz(v);
    }
    fn strf(&mut self, id: i16, s: &str) {
        self.field(id, T_BIN);
        self.varint(s.len() as u64);
        self.b.extend_from_slice(s.as_bytes());
    }
    fn begin(&mut self, id: i16) {
        self.field(id, T_STRUCT);
        self.last.push(0);
    }
    /// start of a struct that is a list element (no field header)
    fn begin_elem(&mut self) {
        self.last.push(0);
    }
    fn end(&mut self) {
        self.b.push(0);
        self.last.pop();
    }
    fn list(&mut self, id: i16, ety: u8, n: usize) {
        self.field(id, T_LIST);
        if n < 15 {
            self.b.push(((n as u8) << 4) | ety);
        } else {
            self.b.push(0xF0 | ety);
            self.varint(n as u64);
        }
    }
    fn elem_i32(&mut self, v: i32) {
        self.zz(v as i64);
    }
    fn elem_str(&mut self, s: &str) {
        self.varint(s.len() as u64);
        self.b.extend_from_slice(s.as_bytes());
    }
}

const TY_INT32: i32 = 1;
const TY_INT64: i32 = 2;
const TY_BYTE_ARRAY: i32 = 6;
const REQUIRED: i32 = 0;
const OPTIONAL: i32 = 1;
const CT_UTF8: i32 = 0;
const CT_DATE: i32 = 6;
const ENC_PLAIN: i32 = 0;
const ENC_RLE: i32 = 3;

impl<'a> Col<'a> {
    fn name(&self) -> &str {
        match self {
            Col::I32 { name, .. } | Col::I64 { name, .. } | Col::Str { name, .. } => name,
        }
    }
    fn ptype(&self) -> i32 {
        match self {
            Col::I32 { .. } => TY_INT32,
            Col::I64 { .. } => TY_INT64,
            Col::Str { .. } => TY_BYTE_ARRAY,
        }
    }
    fn optional(&self) -> bool {
        matches!(self, Col::Str { defs: Some(_), .. })
    }
    /// page body (levels + values) appended to `out`
    fn body(&self, out: &mut Vec<u8>) {
        match self {
            Col::I32 { v, .. } => {
                out.reserve(v.len() * 4);
                for x in v {
                    out.extend_from_slice(&x.to_le_bytes());
                }
            }
            Col::I64 { v, .. } => {
                out.reserve(v.len() * 8);
                for x in v {
                    out.extend_from_slice(&x.to_le_bytes());
                }
            }
            Col::Str { bytes, ends, defs, .. } => {
                if let Some(d) = defs {
                    // definition levels: one bit-packed run, bit width 1
                    let groups = (d.len() + 7) / 8;
                    let mut lv = Vec::with_capacity(groups + 10);
                    let mut h = ((groups as u64) << 1) | 1;
                    while h >= 0x80 {
                        lv.push((h as u8) | 0x80);
                        h >>= 7;
                    }
                    lv.push(h as u8);
                    for g in 0..groups {
                        let mut byte = 0u8;
                        for k in 0..8 {
                            let i = g * 8 + k;
                            if i < d.len() && d[i] {
                                byte |= 1 << k;
                            }
                        }
                        lv.push(byte);
                    }
                    out.extend_from_slice(&(lv.len() as u32).to_le_bytes());
                    out.extend_from_slice(&lv);
                }
                out.reserve(bytes.len() + ends.len() * 4);
                let mut st = 0usize;
                for &e in ends {
                    let e = e as usize;
                    out.extend_from_slice(&((e - st) as u32).to_le_bytes());
                    out.extend_from_slice(&bytes[st..e]);
                    st = e;
                }
            }
        }
    }
}

/// Write `cols` (all with `rows` rows) as one Parquet file.
pub fn write(path: &std::path::Path, rows: usize, cols: &[Col]) -> std::io::Result<()> {
    let mut out: Vec<u8> = Vec::new();
    out.extend_from_slice(b"PAR1");
    let mut chunks: Vec<(i64, i64)> = Vec::new(); // (page offset, total size incl header)
    let mut body = Vec::new();
    for c in cols {
        body.clear();
        c.body(&mut body);
        let mut h = T::new();
        h.i32f(1, 0); // DATA_PAGE
        h.i32f(2, body.len() as i32);
        h.i32f(3, body.len() as i32);
        h.begin(5);
        h.i32f(1, rows as i32);
        h.i32f(2, ENC_PLAIN);
        h.i32f(3, ENC_RLE);
        h.i32f(4, ENC_RLE);
        h.end();
        h.b.push(0); // end PageHeader
        let off = out.len() as i64;
        out.extend_from_slice(&h.b);
        out.extend_from_slice(&body);
        chunks.push((off, (h.b.len() + body.len()) as i64));
    }
    // FileMetaData
    let mut m = T::new();
    m.i32f(1, 1);
    m.list(2, T_STRUCT, cols.len() + 1);
    m.begin_elem();
    m.strf(4, "schema");
    m.i32f(5, cols.len() as i32);
    m.end();
    for c in cols {
        m.begin_elem();
        m.i32f(1, c.ptype());
        m.i32f(3, if c.optional() { OPTIONAL } else { REQUIRED });
        m.strf(4, c.name());
        match c {
            Col::Str { .. } => m.i32f(6, CT_UTF8),
            Col::I32 { date: true, .. } => m.i32f(6, CT_DATE),
            _ => {}
        }
        m.end();
    }
    m.i64f(3, rows as i64);
    let total: i64 = chunks.iter().map(|c| c.1).sum();
    if rows == 0 {
        m.list(4, T_STRUCT, 0);
    } else {
        m.list(4, T_STRUCT, 1);
        m.begin_elem();
        m.list(1, T_STRUCT, cols.len());
        for (c, &(off, size)) in cols.iter().zip(&chunks) {
            m.begin_elem();
            m.i64f(2, off);
            m.begin(3);
            m.i32f(1, c.ptype());
            m.list(2, T_I32, 2);
            m.elem_i32(ENC_PLAIN);
            m.elem_i32(ENC_RLE);
            m.list(3, T_BIN, 1);
            m.elem_str(c.name());
            m.i32f(4, 0); // UNCOMPRESSED
            m.i64f(5, rows as i64);
            m.i64f(6, size);
            m.i64f(7, size);
            m.i64f(9, off);
            m.end();
            m.end();
        }
        m.i64f(2, total);
        m.i64f(3, rows as i64);
        m.end();
    }
    m.strf(6, "indus-platonics");
    m.b.push(0);
    if rows == 0 {
        // no row group: drop the column pages written above
        out.truncate(4);
    }
    out.extend_from_slice(&m.b);
    out.extend_from_slice(&(m.b.len() as u32).to_le_bytes());
    out.extend_from_slice(b"PAR1");
    let mut f = std::fs::File::create(path)?;
    f.write_all(&out)
}
