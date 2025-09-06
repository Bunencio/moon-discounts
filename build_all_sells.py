# build_all_sells.py
import io, json, os, struct
from collections import defaultdict
from datetime import datetime, date
from datetime import datetime as dt
import pandas as pd
import requests

STALL_LIST_URL = "https://moonlight-stall-db.pirategames.online/stall/list"

# Rutas en el repo
HISTORY_PATH = "market_history.json"
RAW_PATH     = "docs/stall_list.raw"

# Salidas a servir por Pages
SALES_TODAY_CSV          = "docs/sales_all_today.csv"
SALES_TODAY_ENRICHED_CSV = "docs/sales_all_today_enriched.csv"
ITEM_ANALYSIS_TODAY_CSV  = "docs/item_analysis_today.csv"
PRESENCE_CSV             = "docs/stall_presence.csv"

ITEMS_JSON_PATHS = ["items_name.json"]

INCLUDE_TODAY_HISTORY = True
OUTLIER_MULTIPLIER    = 3.0

def _load_item_map(paths):
    for p in paths:
        if os.path.exists(p):
            with open(p, "r", encoding="utf-8") as f:
                raw = json.load(f)
            return {int(k): v for k, v in raw.items()}
    return {}

def _clean(b: bytes) -> str:
    return b.split(b"\x00", 1)[0].decode("utf-8", "ignore").strip()

def fetch(url, timeout=20):
    r = requests.get(url, timeout=timeout, stream=True)
    r.raise_for_status()
    return r.content

chunk_size = 930
stall_info_format_string = "i32s64sB32s"
stall_slot_format_string = "=HIB37s"
SLOTS_PER_STALL = 18
BASE_OFF = 138

def extract(content):
    items = {"BUY": {}, "SELL": {}}
    rows = []
    buf = io.BytesIO(content); buf.seek(8)
    slot_size = struct.calcsize(stall_slot_format_string)
    while True:
        chunk = buf.read(chunk_size)
        if len(chunk) < chunk_size: break
        num, seller_b, desc_b, stall_type, location_b = struct.unpack_from(stall_info_format_string, chunk, 0)
        stype = "SELL" if stall_type == 1 else "BUY"
        seller = _clean(seller_b)
        stall_desc = _clean(desc_b)
        location = _clean(location_b)
        stall_name = stall_desc or location
        for i in range(SLOTS_PER_STALL):
            off = BASE_OFF + i * slot_size
            item_id, item_price, quantity, _ = struct.unpack_from(stall_slot_format_string, chunk, off)
            if item_id <= 0: continue
            item_id, item_price, quantity = int(item_id), int(item_price), int(quantity)
            items.setdefault(stype, {}).setdefault(item_id, {})
            items[stype][item_id][item_price] = items[stype][item_id].get(item_price, 0) + quantity
            if stype == "SELL":
                rows.append({"item_id": item_id, "price": item_price, "quantity": quantity, "seller": seller, "stall": stall_name})
    return items, rows

def compute_sell_medians(history, include_today=True):
    from collections import defaultdict
    from datetime import date
    prices = defaultdict(list)
    today = date.today().strftime("%Y-%m-%d")
    for day, data in history.items():
        if not include_today and day == today: continue
        for iid, pm in data.get("SELL", {}).items():
            for p in pm: prices[int(iid)].append(int(p))
    return {iid: float(pd.Series(vals).median()) for iid, vals in prices.items() if vals}

def compute_sell_averages(history, include_today, medians, out_mult):
    from datetime import date
    totals = {}
    today = date.today().strftime("%Y-%m-%d")
    for day, data in history.items():
        if not include_today and day == today: continue
        for iid, pm in data.get("SELL", {}).items():
            iid = int(iid); med = medians.get(iid)
            for p, q in pm.items():
                p, q = int(p), int(q)
                if q <= 0: continue
                if med is not None and p >= med * out_mult: continue
                sum_pq, sum_q, obs = totals.get(iid, (0,0,0))
                totals[iid] = (sum_pq + p*q, sum_q + q, obs + 1)
    return {iid: (pq/q, obs) for iid, (pq, q, obs) in totals.items() if q > 0}

def merge_history(history, extracted):
    from datetime import date
    today = date.today().strftime("%Y-%m-%d")
    if today not in history:
        history[today] = extracted; return history
    for stype, imap in extracted.items():
        history[today].setdefault(stype, {})
        for iid, pm in imap.items():
            history[today][stype].setdefault(str(iid), {})
            for p, q in pm.items():
                p = str(p)
                prev = history[today][stype][str(iid)].get(p)
                history[today][stype][str(iid)][p] = max(int(q), int(prev or 0))
    return history

def relabel(d):
    try:
        D = dt.strptime(d, "%Y-%m-%d").date()
        t = date.today()
        s = (t - D).days
        if s == 0: return "hoy"
        if s == 1: return "ayer"
        if s == 2: return "antier"
        return f"hace {s} días" if s>2 else f"en {abs(s)} días"
    except: return ""

def main():
    os.makedirs("docs", exist_ok=True)
    ITEM_NAME = _load_item_map(ITEMS_JSON_PATHS)

    content = fetch(STALL_LIST_URL)
    extracted, sell_rows = extract(content)
    today_str = date.today().strftime("%Y-%m-%d")

    try:
        with open(HISTORY_PATH, "r", encoding="utf-8") as f:
            history = json.load(f)
    except FileNotFoundError:
        history = {}
    history = merge_history(history, extracted)
    with open(HISTORY_PATH, "w", encoding="utf-8") as f:
        json.dump(history, f, ensure_ascii=False, separators=(",", ":"))

    med = compute_sell_medians(history, include_today=INCLUDE_TODAY_HISTORY)
    avgs = compute_sell_averages(history, INCLUDE_TODAY_HISTORY, med, OUTLIER_MULTIPLIER)

    # 1) TODOS los SELL de hoy (crudo)
    df_today = pd.DataFrame([{
        "date": today_str,
        "item_id": r["item_id"],
        "item_name": ITEM_NAME.get(int(r["item_id"]), ""),
        "price": r["price"],
        "quantity": r["quantity"],
        "seller": r["seller"],
        "stall": r["stall"],
    } for r in sell_rows])
    df_today.to_csv(SALES_TODAY_CSV, index=False)

    # 2) Enriquecido con historia
    def enr(row):
        iid = int(row["item_id"])
        avg_obs = avgs.get(iid); medv = med.get(iid)
        avg = avg_obs[0] if avg_obs else None
        obs = int(avg_obs[1]) if avg_obs else 0
        price = float(row["price"])
        pct_vs_avg = (price/avg*100.0) if (avg and avg>0) else None
        pct_vs_med = (price/medv*100.0) if (medv and medv>0) else None
        disc = (1 - price/avg)*100.0 if (avg and avg>0) else None
        return pd.Series({
            "avg_hist": round(float(avg),2) if avg else None,
            "median_hist": round(float(medv),2) if medv else None,
            "obs_hist": obs,
            "pct_vs_avg": round(float(pct_vs_avg),2) if pct_vs_avg else None,
            "pct_vs_median": round(float(pct_vs_med),2) if pct_vs_med else None,
            "discount_pct_vs_avg": round(float(disc),2) if disc else None,
        })
    if not df_today.empty:
        enriched = df_today.join(df_today.apply(enr, axis=1))
    else:
        enriched = pd.DataFrame(columns=list(df_today.columns)+["avg_hist","median_hist","obs_hist","pct_vs_avg","pct_vs_median","discount_pct_vs_avg"])
    enriched.to_csv(SALES_TODAY_ENRICHED_CSV, index=False)

    # 3) Presencia por item (first/last/dates)
    item_days = defaultdict(set)
    for day, data in history.items():
        for iid, pm in data.get("SELL", {}).items():
            item_days[int(iid)].add(day)
    pres_rows = []
    for iid, days in item_days.items():
        ds = sorted(days)
        pres_rows.append({
            "item_id": iid,
            "item_name": ITEM_NAME.get(int(iid), ""),
            "first_seen": ds[0],
            "last_seen": ds[-1],
            "last_seen_relative": relabel(ds[-1]),
            "seen_days_count": len(ds),
            "dates": ";".join(ds),
        })
    pd.DataFrame(pres_rows).to_csv(PRESENCE_CSV, index=False)

    # 4) Guardar raw para debug
    with open(RAW_PATH, "wb") as f: f.write(content)

if __name__ == "__main__":
    main()
