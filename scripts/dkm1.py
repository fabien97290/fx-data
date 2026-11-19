"""Dukascopy M1 BID + ASK + volume -> Parquet mensuel (UTC). Usage: dkm1.py SYMBOL Y0 Y1. Reprise automatique (mois déjà faits sautés).
Enregistrement candle bi5 : 24 octets BE : sec (u4), open, close, low, high (u4), volume (f4)."""
import sys, lzma, time, datetime as dt, os, threading
import numpy as np, pandas as pd, requests
from concurrent.futures import ThreadPoolExecutor
SYM, Y0, Y1 = sys.argv[1].upper(), int(sys.argv[2]), int(sys.argv[3])
OUT = os.environ.get("DKOUT", "data")
FAILED = False
MINMIN = int(os.environ.get("DKMIN", "15000"))   # minutes minimum attendues par mois (0 = pas de contrôle)
NOW = dt.datetime.now(dt.timezone.utc)
_tl = threading.local()
def sess():
    if not hasattr(_tl, "s"): _tl.s = requests.Session(); _tl.s.headers["User-Agent"] = "Mozilla/5.0"
    return _tl.s
DT = np.dtype([("s", ">u4"), ("o", ">u4"), ("c", ">u4"), ("l", ">u4"), ("h", ">u4"), ("v", ">f4")])
def get(args):
    d, side = args
    url = f"https://datafeed.dukascopy.com/datafeed/{SYM}/{d.year}/{d.month-1:02d}/{d.day:02d}/{side}_candles_min_1.bi5"
    s = sess()
    for k in range(30):
        try:
            r = s.get(url, timeout=120)
            if r.status_code == 200:
                if not r.content: return d, side, None
                a = np.frombuffer(lzma.decompress(r.content), DT)
                return d, side, a
            if r.status_code == 404: return d, side, None
        except Exception: pass
        time.sleep(min(120, 5 * (k + 1)))
    raise RuntimeError("échec " + url)
for Y in range(Y0, Y1 + 1):
    for M in range(1, 13):
        os.makedirs(f"{OUT}/{SYM}", exist_ok=True); fn = f"{OUT}/{SYM}/{SYM}_{Y}-{M:02d}.parquet"
        if os.path.exists(fn): continue
        if (Y, M) >= (NOW.year, NOW.month): break                 # mois en cours ou futur : pas encore complet
        t0 = time.time()
        days = [dt.date(Y, M, 1) + dt.timedelta(k) for k in range(31)]
        days = [d for d in days if d.month == M and d.weekday() != 5]
        try:
            with ThreadPoolExecutor(int(os.environ.get('DKTHREADS', '4'))) as ex: res = list(ex.map(get, [(d, s) for d in days for s in ("BID", "ASK")]))
        except RuntimeError as e:
            print(f"{SYM} {Y}-{M:02d} : ÉCHEC, repris au prochain passage ({e})", flush=True); FAILED = True; continue
        by = {}
        for d, side, a in res:
            if a is None or len(a) == 0: continue
            t = np.datetime64(dt.datetime(d.year, d.month, d.day), "s") + a["s"].astype("timedelta64[s]")
            by.setdefault(side, []).append(pd.DataFrame({"t": t, f"{side[0].lower()}o": a["o"], f"{side[0].lower()}h": a["h"], f"{side[0].lower()}l": a["l"],
                                                         f"{side[0].lower()}c": a["c"], f"{side[0].lower()}v": a["v"]}))
        if "BID" not in by or "ASK" not in by:
            print(f"{SYM} {Y}-{M:02d} : aucune donnée (BID {'ok' if 'BID' in by else 'absent'}, ASK {'ok' if 'ASK' in by else 'absent'}), ignoré", flush=True); continue
        B = pd.concat(by["BID"]); A = pd.concat(by["ASK"])
        X = B.merge(A, on="t", how="inner").sort_values("t")
        X = X[(X.bv > 0) | (X.av > 0)]                      # retire les minutes « plates » (marché fermé)
        for c in X.columns:
            if c != "t" and not c.endswith("v"): X[c] = X[c].astype(np.int32)
        if len(X) < MINMIN:
            print(f"{SYM} {Y}-{M:02d} : seulement {len(X):,} min (< {MINMIN:,}), non enregistré, repris au prochain passage (DKMIN=0 pour forcer)", flush=True)
            FAILED = True; continue
        X.to_parquet(fn, compression="zstd", compression_level=9, index=False)
        print(f"{SYM} {Y}-{M:02d} : {len(X):,} min, {os.path.getsize(fn)/1e6:.2f} Mo, {time.time()-t0:.0f} s", flush=True)

if FAILED: sys.exit(1)
