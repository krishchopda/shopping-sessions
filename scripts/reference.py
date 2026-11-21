#!/usr/bin/env python3
"""Slow, literal reference for rules.md, written independently of the Rust code.

Usage: reference.py DATA_DIR OUT_DIR
Writes OUT_DIR/user_daily.csv and OUT_DIR/campaign_daily.csv in the fixture
format (day as YYYY-MM-DD, empty top_category for null), so compare.py can
diff it against a pipeline's Parquet output.
"""
import csv, glob, json, os, sys
from collections import defaultdict

DAY = 86_400_000
WEEK = 7 * DAY
GAP = 1_800_000
DIG = set(b"0123456789")


def fields(line, n):
    if line == b"":
        return None
    parts = line.split(b",")
    if len(parts) != n:
        return None
    out = []
    for f in parts:
        if b'"' in f:
            if len(f) >= 2 and f[:1] == b'"' and f[-1:] == b'"' and b'"' not in f[1:-1]:
                f = f[1:-1]
            else:
                return None
        out.append(f)
    return out


def rows(paths, n):
    for p in paths:
        data = open(p, "rb").read()
        lines = data.split(b"\n")
        if lines and lines[-1] == b"":
            lines.pop()
        for line in lines[1:]:
            f = fields(line, n)
            if f is not None:
                yield f


def days_from_civil(y, m, d):
    y -= m <= 2
    era = y // 400
    yoe = y - era * 400
    doy = (153 * ((m + 9) % 12) + 2) // 5 + d - 1
    doe = yoe * 365 + yoe // 4 - yoe // 100 + doy
    return era * 146097 + doe - 719468


def civil(s):
    if len(s) < 19 or s[4:5] != b"-" or s[7:8] != b"-" or s[10:11] != b"T" or s[13:14] != b":" or s[16:17] != b":":
        return None
    for i in (0, 1, 2, 3, 5, 6, 8, 9, 11, 12, 14, 15, 17, 18):
        if s[i] not in DIG:
            return None
    y, mo, d = int(s[0:4]), int(s[5:7]), int(s[8:10])
    h, mi, se = int(s[11:13]), int(s[14:16]), int(s[17:19])
    if not (1 <= mo <= 12) or h > 23 or mi > 59 or se > 59 or d < 1:
        return None
    leap = y % 4 == 0 and (y % 100 != 0 or y % 400 == 0)
    dim = [31, 29 if leap else 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31][mo - 1]
    if d > dim:
        return None
    return (days_from_civil(y, mo, d) * 86400 + h * 3600 + mi * 60 + se) * 1000


def ts(s):
    if len(s) == 20 and s[19:20] == b"Z":
        return civil(s)
    if len(s) == 25 and s[19:20] in (b"+", b"-") and s[22:23] == b":":
        if not all(c in DIG for c in s[20:22] + s[23:25]):
            return None
        oh, om = int(s[20:22]), int(s[23:25])
        if oh > 23 or om > 59:
            return None
        base = civil(s)
        if base is None:
            return None
        sign = 1 if s[19:20] == b"+" else -1
        return base - sign * (oh * 60 + om) * 60000
    if len(s) > 0 and all(c in DIG for c in s):
        return int(s)
    return None


def main():
    data, out = sys.argv[1], sys.argv[2]
    man = json.load(open(os.path.join(data, "manifest.json")))
    ws = ts(man["window_start"].encode())
    we = ts(man["window_end"].encode())

    cat = {}
    for f in rows([os.path.join(data, "products.csv")], 2):
        cat[f[0]] = f[1]
    chan = {}
    for f in rows([os.path.join(data, "campaigns.csv")], 2):
        chan[f[0]] = f[1]

    # links
    parent = {}

    def find(x):
        parent.setdefault(x, x)
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    for a, b in rows(sorted(glob.glob(os.path.join(data, "links", "*.csv"))), 2):
        if a == b"" or b == b"":
            continue
        ra, rb = find(a), find(b)
        if ra != rb:
            parent[ra] = rb
    name = {}
    for x in list(parent):
        r = find(x)
        if r not in name or x < name[r]:
            name[r] = x

    def person(u):
        if u in parent:
            return name[find(u)]
        return u

    # events: dedup on event_id, keep smallest ingest_ts
    kept = {}
    for f in rows(sorted(glob.glob(os.path.join(data, "events", "*.csv"))), 7):
        eid = f[0]
        if eid == b"":
            continue
        ing = ts(f[3])
        if eid not in kept or ing < kept[eid][0]:
            kept[eid] = (ing, f)
    evs = defaultdict(list)
    for eid, (ing, f) in kept.items():
        uid, t, typ, prod, price = f[1], ts(f[2]), f[4], f[5], f[6]
        if uid == b"" or t is None or not (ws <= t < we) or typ not in (b"view", b"cart", b"purchase") or prod == b"":
            continue
        p = 0
        if typ == b"purchase":
            if price == b"" or not all(c in DIG for c in price):
                continue
            p = int(price)
            if not (1 <= p <= 10_000_000):
                continue
        if ing - t > DAY:
            continue
        evs[person(uid)].append((t, p, cat.get(prod, b"unknown") if p else None))

    clicks = defaultdict(list)
    seen = set()
    for f in rows(sorted(glob.glob(os.path.join(data, "clicks", "*.csv"))), 4):
        cid, uid, camp, t = f[0], f[1], f[2], ts(f[3])
        if cid == b"" or cid in seen:
            continue
        seen.add(cid)
        if uid == b"" or t is None or not (ws - WEEK <= t < we) or camp not in chan:
            continue
        clicks[person(uid)].append((t, cid, camp))

    ud = {}
    cd = defaultdict(lambda: [0, 0, 0, 0])
    for p, lst in evs.items():
        lst.sort(key=lambda e: e[0])
        sessions = []
        for e in lst:
            if sessions and e[0] - sessions[-1][-1][0] <= GAP:
                sessions[-1].append(e)
            else:
                sessions.append([e])
        cl = clicks.get(p, [])
        for s in sessions:
            start = s[0][0]
            day = start // DAY
            rev = sum(e[1] for e in s)
            purch = any(e[1] for e in s)
            r = ud.setdefault((p, day), [0, 0, 0, defaultdict(int)])
            r[0] += 1
            r[1] += len(s)
            r[2] += rev
            for e in s:
                if e[1]:
                    r[3][e[2]] += e[1]
            q = [c for c in cl if c[0] <= start and start - c[0] <= WEEK]
            if q:
                best = min(q, key=lambda c: (-c[0], c[1]))
                camp = best[2]
            else:
                camp = b"organic"
            row = cd[(camp, day)]
            row[0] += 1
            row[1] += purch
            row[2] += rev
            for c in set(c[2] for c in q):
                cd[(c, day)][3] += 1

    def date(d):
        import datetime
        return (datetime.date(1970, 1, 1) + datetime.timedelta(days=d)).isoformat()

    os.makedirs(out, exist_ok=True)
    with open(os.path.join(out, "user_daily.csv"), "w", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(["user_id", "day", "sessions", "events", "revenue_cents", "top_category"])
        for (p, day), r in ud.items():
            top = ""
            if r[3]:
                top = min(r[3].items(), key=lambda kv: (-kv[1], kv[0]))[0].decode()
            w.writerow([p.decode(), date(day), r[0], r[1], r[2], top])
    with open(os.path.join(out, "campaign_daily.csv"), "w", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(["campaign_id", "channel", "day", "sessions", "purchasing_sessions", "revenue_cents", "assisted_sessions"])
        for (c, day), r in cd.items():
            ch = "organic" if c == b"organic" else chan[c].decode()
            w.writerow([c.decode(), ch, date(day), r[0], r[1], r[2], r[3]])


if __name__ == "__main__":
    main()
