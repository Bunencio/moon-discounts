# market_update.py
# Ejecuta el scrapper, calcula descuentos y profit, guarda CSV/artefactos en docs/ y mantiene el historial.

import io, os, json, struct
from collections import defaultdict
from datetime import datetime, date

import pandas as pd
import requests
from tabulate import tabulate

# =========================
# Settings (rutas relativas al repo)
# =========================
STALL_LIST_URL = "https://moonlight-stall-db.pirategames.online/stall/list"

HISTORY_PATH         = "market_history.json"                  # historial acumulado (commitado)
DOCS_DIR             = "docs"
RESULTS_DISCOUNT_CSV = os.path.join(DOCS_DIR, "discount_hits_ge_50pct.csv")
RESULTS_PROFIT_CSV   = os.path.join(DOCS_DIR, "profit_hits_ge_20000.csv")
RAW_PATH             = os.path.join(DOCS_DIR, "stall_list.raw")
LAST_RUN_JSON        = os.path.join(DOCS_DIR, "last_run.json")
ITEMS_JSON_PATHS     = ["items_name.json"]                    # si está en el repo, pone nombres a los IDs

# Parámetros de detección
THRESHOLD_DISCOUNT_PCT = 50
MIN_OBS = 3                         # mínimo de observaciones históricas por ítem
INCLUDE_TODAY_HISTORY = True
OUTLIER_MULTIPLIER = 3.0            # filtra precios >= 3× mediana histórica (overpriced)
PROFIT_MIN_IMPS = 20000             # profit total mínimo
EXCLUDE_NAME_KEYWORDS = [
    "Boots", "Gloves", "Gauntlets", "Armor", "Fairy",
    "Shadow Gem", "Potion of Monkey", "Lustrious Gem", "Candy", "Sword",
    "Blueprint"
]
SHOW_MAX_ROWS = 30

# =========================
# Helpers
# =========================
def ensure_dirs():
    os.makedirs(DOCS_DIR, exist_ok=True)

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

def download_binary_content(url, timeout=25):
    r = requests.get(url, timeout=timeout, stream=True)
    r.raise_for_status()
    content = r.content
    if not content:
        raise ValueError("Received empty binary content.")
    print(f"Fetched {len(content)} bytes from {url}")
    return content

# =========================
# Parsing formatos
# =========================
CHUNK_SIZE = 930
STALL_INFO_FMT = "i32s64sB32s"
STALL_SLOT_FMT = "=HIB37s"
SLOTS_PER_STALL = 18
BASE_OFF = 138

def extract(content):
    """
    Returns:
      extracted = {"BUY": {iid:{price:qty}}, "SELL": {...}}
      current_sell_rows = list[dict] con filas SELL (esta corrida)
    """
    items = {"BUY": {}, "SELL": {}}
    buffer = io.BytesIO(content)
    buffer.seek(8)
    slot_size = struct.calcsize(STALL_SLOT_FMT)

    rows = []
    stalls = 0
    while True:
        chunk = buffer.read(CHUNK_SIZE)
        if len(chunk) < CHUNK_SIZE:
            break
        stalls += 1
        num, seller_b, desc_b, stall_type, location_b = struct.unpack_from(
            STALL_INFO_FMT, chunk, 0
        )
        stall_type_str = "SELL" if stall_type == 1 else "BUY"
        seller = _clean(seller_b)
        stall_desc = _clean(desc_b)
        location = _clean(location_b)
        stall_name = stall_desc or location

        for i in range(SLOTS_PER_STALL):
            off = BASE_OFF + i * slot_size
            item_id, item_price, quantity, _ = struct.unpack_from(
                STALL_SLOT_FMT, chunk, off
            )
            if item_id <= 0:
                continue
            item_id, item_price, quantity = int(item_id), int(item_price), int(quantity)
            items.setdefault(stall_type_str, {}).setdefault(item_id, {})
            items[stall_type_str][item_id][item_price] = (
                items[stall_type_str][item_id].get(item_price, 0) + quantity
            )
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

# =========================
# Historial y estadísticas
# =========================
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
    """
    Promedio ponderado por cantidad. Excluye outliers: price >= out_mult * mediana.
    Return: { item_id: (avg_price, obs_count) }
    """
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
    """
    Fusiona el snapshot de hoy (BUY+SELL) manteniendo máx cantidad por (item, price).
    """
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

# =========================
# Main
# =========================
def main():
    ensure_dirs()

    # 1) Descargar y parsear
    content = download_binary_content(STALL_LIST_URL)
    extracted, current_sell_rows = extract(content)

    # 2) Cargar historial, integrar snapshot de HOY y persistir
    try:
        with open(HISTORY_PATH, "r", encoding="utf-8") as f:
            history = json.load(f)
        print(f"Loaded existing history: {HISTORY_PATH}")
    except FileNotFoundError:
        history = {}

    history = merge_history(history, extracted)
    with open(HISTORY_PATH, "w", encoding="utf-8") as f:
        json.dump(history, f, ensure_ascii=False, separators=(",", ":"))

    # 3) Stats desde historial (incluye hoy si así se configuró)
    medians   = compute_sell_medians(history, include_today=INCLUDE_TODAY_HISTORY)
    hist_avgs = compute_sell_averages(history, INCLUDE_TODAY_HISTORY, medians, OUTLIER_MULTIPLIER)
    print(f"Computed SELL medians for {len(medians)} items.")

    # 4) Armar listas de resultados
    today_str = date.today().strftime("%Y-%m-%d")
    discount_hits = []
    profit_hits   = []

    for r in current_sell_rows:
        iid   = r["item_id"]
        price = r["price"]
        qty   = r["quantity"]
        name  = r["item_name"]

        if _blocked_by_name(name):
            continue
        if iid not in hist_avgs:
            continue

        avg, obs = hist_avgs[iid]
        if obs < MIN_OBS or avg <= 0:
            continue

        discount_pct = (1 - price / avg) * 100.0

        # TAB 1: ≥ 50% OFF
        if discount_pct >= THRESHOLD_DISCOUNT_PCT:
            discount_hits.append({
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

        # TAB 2: Profit ≥ 20,000
        profit_unit  = float(avg) - float(price)
        profit_total = profit_unit * float(qty)
        if profit_total >= PROFIT_MIN_IMPS:
            profit_hits.append({
                "date": today_str,
                "item_id": iid,
                "item_name": name,
                "seller": r["seller"],
                "stall": r["stall"],
                "price": int(price),
                "avg": round(float(avg), 2),
                "profit_unit": round(profit_unit, 2),
                "qty": int(qty),
                "profit_total": round(profit_total, 2),
                "discount_pct": round(discount_pct, 2),
                "obs": int(obs),
            })

    # 5) DataFrames y guardado de CSVs (siempre escribimos, aunque sea vacío con headers)
    # Descuentos
    if discount_hits:
        df_disc = pd.DataFrame(discount_hits).sort_values(
            ["discount_pct", "item_id"], ascending=[False, True]
        ).reset_index(drop=True)
        print("\n=== SELL items with ≥ 50% discount ===")
        print(tabulate(df_disc.head(SHOW_MAX_ROWS), headers="keys", tablefmt="github", showindex=False))
    else:
        df_disc = pd.DataFrame(columns=[
            "discount_pct","item_id","item_name","price","avg","obs","qty","seller","stall"
        ])
        print("\nNo SELL items with ≥ 50% discount.")
    df_disc.to_csv(RESULTS_DISCOUNT_CSV, index=False)

    # Profit
    if profit_hits:
        df_profit = pd.DataFrame(profit_hits).sort_values(
            ["discount_pct","profit_total","profit_unit"], ascending=[False, False, False]
        ).reset_index(drop=True)
        print("\n=== Items with Profit ≥ 20,000 ===")
        print(tabulate(df_profit.head(SHOW_MAX_ROWS), headers="keys", tablefmt="github", showindex=False))
    else:
        df_profit = pd.DataFrame(columns=[
            "date","item_id","item_name","seller","stall",
            "price","avg","profit_unit","qty","profit_total",
            "discount_pct","obs"
        ])
        print("\nNo items with profit_total ≥ 20,000.")
    df_profit.to_csv(RESULTS_PROFIT_CSV, index=False)

    # 6) Artefactos web
    with open(RAW_PATH, "wb") as f:
        f.write(content)

    with open(LAST_RUN_JSON, "w", encoding="utf-8") as f:
        json.dump({"updated_at": datetime.utcnow().isoformat(timespec="seconds") + "Z"}, f, ensure_ascii=False)

    print("\nDone.")

if __name__ == "__main__":
    main()
