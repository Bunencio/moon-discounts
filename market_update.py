# market_update.py
import requests, io, struct, json, os, pandas as pd
from datetime import datetime, date
from collections import defaultdict
from tabulate import tabulate

# --- Settings (paths relative to repo root) ---
STALL_LIST_URL = "https://moonlight-stall-db.pirategames.online/stall/list"
HISTORY_PATH   = "market_history.json"                    # cumulative history file (committed)
RESULTS_CSV    = "docs/discount_hits_ge_50pct.csv"        # output for the web page
RAW_PATH       = "docs/stall_list.raw"                    # last raw binary snapshot for debugging
ITEMS_JSON_PATHS = [
    "items_name.json",            # repo file if present
]

# Detection params
THRESHOLD_DISCOUNT_PCT = 50
MIN_OBS = 3
INCLUDE_TODAY_HISTORY = True
OUTLIER_MULTIPLIER = 3.0
EXCLUDE_NAME_KEYWORDS = ["Boots", "Gloves", "Gauntlets", "Armor", "Fairy"]
SHOW_MAX_ROWS = 30

# --- helpers ---
def _load_item_map(paths):
    for p in paths:
        if os.path.exists(p):
            with open(p, "r", encoding="utf-8") as f:
                raw_map = json.load(f)
            print(f"Loaded item name map: {p} (entries: {len(raw_map)})")
            return {int(k): v for k, v in raw_map.items()}
    print("WARNING: items_name.json not found. Names will be blank; only IDs shown.")
    return {}

ITEM_NAME = _load_item_map(ITEMS_JSON_PATHS)

def _clean(b: bytes) -> str:
    return b.split(b"\x00", 1)[0].decode("utf-8", "ignore").strip()

def _blocked_by_name(name: str) -> bool:
    nm = (name or "").lower()
    for kw in EXCLUDE_NAME_KEYWORDS:
        if kw.lower() in nm:
            return True
    return False

def download_binary_content(url, timeout=20):
    r = requests.get(url, timeout=timeout, stream=True)
    r.raise_for_status()
    content = r.content
    if not content:
        raise ValueError("Received empty binary content.")
    print(f"Fetched {len(content)} bytes from {url}")
    return content

# --- parsing formats ---
chunk_size = 930
stall_info_format_string = 'i32s64sB32s'
stall_slot_format_string = '=HIB37s'

def extract(content):
    items = {"BUY": {}, "SELL": {}}
    buffer = io.BytesIO(content)
    buffer.seek(8)
    slot_size = struct.calcsize(stall_slot_format_string)
    base_off = 138

    rows = []
    stalls = 0
    while True:
        chunk = buffer.read(chunk_size)
        if len(chunk) < chunk_size:
            break
        stalls += 1
        num, seller_b, desc_b, stall_type, location_b = struct.unpack_from(stall_info_format_string, chunk, 0)
        stall_type_str = "SELL" if stall_type == 1 else "BUY"
        seller = _clean(seller_b)
        stall_desc = _clean(desc_b)
        location = _clean(location_b)
        stall_name = stall_desc or location

        for i in range(18):
            off = base_off + i * slot_size
            item_id, item_price, quantity, _ = struct.unpack_from(stall_slot_format_string, chunk, off)
            if item_id <= 0:
                continue
            item_id, item_price, quantity = int(item_id), int(item_price), int(quantity)
            items.setdefault(stall_type_str, {}).setdefault(item_id, {})
            items[stall_type_str][item_id][item_price] = items[stall_type_str][item_id].get(item_price, 0) + quantity
            if stall_type_str == "SELL":
                rows.append({
                    "item_id": item_id,
                    "item_name": ITEM_NAME.get(item_id, ""),
                    "price": item_price,
                    "quantity": quantity,
                    "seller": seller,
                    "stall": stall_name,
                })
    print(f"Parsed {stalls} stalls.")
    return items, rows

def compute_sell_medians(history, include_today=True):
    today = date.today().strftime("%Y-%m-%d")
    prices = defaultdict(list)
    for day, data in history.items():
        if not include_today and day == today:
            continue
        for iid, pm in data.get("SELL", {}).items():
            for p in pm:
                prices[int(iid)].append(int(p))
    return {iid: float(pd.Series(vals).median()) for iid, vals in prices.items() if vals}

def compute_sell_averages(history, include_today, medians, out_mult):
    today = date.today().strftime("%Y-%m-%d")
    totals = {}
    for day, data in history.items():
        if not include_today and day == today:
            continue
        for iid, pm in data.get("SELL", {}).items():
            iid = int(iid)
            med = medians.get(iid)
            for p, q in pm.items():
                p, q = int(p), int(q)
                if q <= 0:
                    continue
                if med is not None and p >= med * out_mult:
                    continue
                sum_pq, sum_q, obs = totals.get(iid, (0, 0, 0))
                totals[iid] = (sum_pq + p * q, sum_q + q, obs + 1)
    return {iid: (pq / q, obs) for iid, (pq, q, obs) in totals.items() if q > 0}

def merge_history(history, extracted):
    today = date.today().strftime("%Y-%m-%d")
    if today not in history:
        history[today] = extracted
        return history
    for stype, imap in extracted.items():
        history[today].setdefault(stype, {})
        for iid, pm in imap.items():
            history[today][stype].setdefault(str(iid), {})
            for p, q in pm.items():
                p = str(p)
                prev = history[today][stype][str(iid)].get(p)
                history[today][stype][str(iid)][p] = max(int(q), int(prev or 0))
    return history

def ensure_dirs():
    os.makedirs("docs", exist_ok=True)

def main():
    ensure_dirs()

    # fetch & parse
    content = download_binary_content(STALL_LIST_URL)
    extracted, current_sell_rows = extract(content)

    # load history
    try:
        with open(HISTORY_PATH, "r", encoding="utf-8") as f:
            history = json.load(f)
        print(f"Loaded existing history: {HISTORY_PATH}")
    except FileNotFoundError:
        history = {}

    # stats from history
    medians = compute_sell_medians(history, include_today=INCLUDE_TODAY_HISTORY)
    print(f"Computed SELL medians for {len(medians)} items.")
    hist_avgs = compute_sell_averages(history, INCLUDE_TODAY_HISTORY, medians, OUTLIER_MULTIPLIER)

    # detect hits
    hits = []
    for r in current_sell_rows:
        iid = r["item_id"]
        price = r["price"]
        qty = r["quantity"]
        name = r["item_name"]
        if any(kw.lower() in (name or "").lower() for kw in EXCLUDE_NAME_KEYWORDS):
            continue
        if iid not in hist_avgs:
            continue
        avg, obs = hist_avgs[iid]
        if obs < MIN_OBS or avg <= 0:
            continue
        discount_pct = (1 - price / avg) * 100.0
        if discount_pct >= THRESHOLD_DISCOUNT_PCT:
            hits.append({
                "discount_pct": round(discount_pct, 2),
                "item_id": iid,
                "item_name": name,
                "price": int(price),
                "avg": round(float(avg), 2),
                "obs": int(obs),
                "qty": int(qty),
                "seller": r["seller"],
                "stall": r["stall"],
            })

    df = pd.DataFrame(hits).sort_values("discount_pct", ascending=False).reset_index(drop=True)

    if not df.empty:
        print(f"\n=== SELL items with ≥ {THRESHOLD_DISCOUNT_PCT}% discount ===")
        print(tabulate(df.head(30), headers="keys", tablefmt="github", showindex=False))
        df.to_csv(RESULTS_CSV, index=False)
    else:
        # still write an empty CSV with headers for a clean page
        df = pd.DataFrame(columns=["discount_pct","item_id","item_name","price","avg","obs","qty","seller","stall"])
        df.to_csv(RESULTS_CSV, index=False)
        print("\nNo hits; wrote empty CSV with headers.")

    # save metadata + artifacts
    history = merge_history(history, extracted)
    with open(HISTORY_PATH, "w", encoding="utf-8") as f:
        json.dump(history, f, ensure_ascii=False, separators=(",", ":"))
    with open(RAW_PATH, "wb") as f:
        f.write(content)
    with open("docs/last_run.json", "w", encoding="utf-8") as f:
        json.dump({"updated_at": datetime.utcnow().isoformat(timespec="seconds") + "Z"}, f)

    print("Done.")

if __name__ == "__main__":
    main()
