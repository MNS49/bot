# ============================================
# Section5.py (UPDATED FINAL, with SLOTS/NEXTSLOTS enabled)
# Telegram Handlers
#          - Channel recommendations (Email Gate from Section 2, EN notifications)
#          - Saved Messages commands
#            (gate/off/gate open/gate close/pause/reuse/status/summary/track/track <n>/
#             sell <symbol>|sell <index>/cycl/clean terminal/close/cancel/help/risk)
#          - Blacklist commands: Add <sym> / Remove <sym> / Status List
#          - slots / slots all / nextslots / verlauf
#          - Unified numbering map for status & alerts
#          - Realized-only % for TP/SL/DRWDN
#          - Separate SL vs DRWDN lists everywhere
#          - Debug funds toggles: "debug funds on/off/<Nm>"
#          - Console echo for commands & recommendations (ENABLE_CONSOLE_ECHO)
#          - Status BUY line format: "— T N | C LABN  <buy> → now <price> / Δ <pct>"
#          - Manual Email Gate override from Saved Messages (gate open / gate close)
# ============================================

import os, re, json, time, asyncio, random
from datetime import datetime, timezone, date, timedelta
from typing import Any, Dict, List, Optional, Tuple, Set
from statistics import median

# IMPORTANT: Section 5 relies on Telethon events when split into a separate file.
# Make sure to import Telethon events here.
try:
    from telethon import events  # provided in Section 1's client
except Exception:
    events = None  # fallback to avoid import error during static checks

# ===== Telegram long message splitter =====
TELEGRAM_MSG_LIMIT = 4000  # conservative limit for Telegram bots

# ====== console_echo alias (reuse global) ======
# We reuse the global console_echo defined in Section 2. If not present,
# we fallback to a no-op. We also expose _console_echo for legacy calls.
try:
    _ = console_echo  # type: ignore[name-defined]
except Exception:
    def console_echo(msg: str) -> None:  # no-op fallback
        pass
# legacy alias
_console_echo = console_echo

# ====== Fallbacks (in case Section 2 wasn't imported yet at runtime) ======
try:
    normalize_symbol
except NameError:  # pragma: no cover
    def normalize_symbol(s: str) -> str:
        return (s or "").upper().replace('-', '').replace('/', '')

async def _send_long_message(text: str, part_title: str = None, limit: int = TELEGRAM_MSG_LIMIT):
    if text is None:
        return
    if len(text) <= limit:
        # prefer T/C-aware notifier only for short one-liners — here it's multi-line/overview so use plain
        await send_notification(text)
        _console_echo(text)
        return
    parts, chunk = [], ""
    for line in text.splitlines(True):
        if len(chunk) + len(line) > limit:
            parts.append(chunk.rstrip())
            chunk = line
        else:
            chunk += line
    if chunk:
        parts.append(chunk.rstrip())
    total = len(parts)
    title_prefix = (part_title + " — ") if part_title else ""
    for i, p in enumerate(parts, 1):
        header = f"{title_prefix}(Part {i}/{total})\n"
        msg = header + p
        await send_notification(msg)
        _console_echo(msg)

# ===== Email Gate helpers (centralized in Section 2) =====
# NOTE: the gate source of truth is Section 2:
#   - should_accept_recommendations()  → combines bot active + gate state
#   - is_email_gate_open()             → raw gate flag
async def show_gate_status():
    try:
        is_open = is_email_gate_open()
    except Exception:
        is_open = True
    label = "OPEN ✅ (accepting recommendations)" if is_open else "CLOSED⛔️ (paused; ignoring recommendations)"
    extra = "\nTrigger words (subject/body): ‘buy crypto’ → OPEN, ‘sell crypto’ → CLOSE"
    await send_notification(f"📧 Email Gate status: {label}{extra}")

# ===== Summary accumulation (PnL) =====
def accumulate_summary(profit_delta: float = 0.0, loss_delta: float = 0.0) -> None:
    try:
        data = {"total_profit": 0.0, "total_loss": 0.0, "net": 0.0}
        if os.path.exists(SUMMARY_FILE):
            try:
                with open(SUMMARY_FILE, 'r') as f:
                    loaded = json.load(f)
                data["total_profit"] = float(loaded.get("total_profit", 0.0) or 0.0)
                data["total_loss"]   = float(loaded.get("total_loss", 0.0) or 0.0)
            except Exception:
                pass
        if profit_delta and profit_delta > 0:
            data["total_profit"] += float(profit_delta)
        if loss_delta and loss_delta > 0:
            data["total_loss"] += float(loss_delta)
        data["net"] = data["total_profit"] - data["total_loss"]
        with open(SUMMARY_FILE, 'w') as f:
            json.dump(data, f, indent=2)
    except Exception as e:
        print(f"⚠️ accumulate_summary error: {e}")

async def show_trade_summary():
    summary = {"total_profit": 0.0, "total_loss": 0.0, "net": 0.0}
    try:
        if os.path.exists(SUMMARY_FILE):
            with open(SUMMARY_FILE, 'r') as f:
                loaded = json.load(f)
            summary["total_profit"] = float(loaded.get("total_profit", 0.0) or 0.0)
            summary["total_loss"]   = float(loaded.get("total_loss", 0.0) or 0.0)
        else:
            with open(SUMMARY_FILE, 'w') as f:
                json.dump(summary, f, indent=2)
    except Exception as e:
        await send_notification(f"⚠️ Summary read error: {e}")
    summary["net"] = summary["total_profit"] - summary["total_loss"]
    await send_notification(
        "📊 Profit & Loss Summary:\n"
        f"💰 Total Profit: {summary['total_profit']:.2f} USDT\n"
        f"📉 Total Loss: {summary['total_loss']:.2f} USDT\n"
        f"📊 Net profit : {summary['net']:.2f} USDT"
    )

# ===== Berlin timezone helpers =====

def _berlin_tz():
    try:
        from zoneinfo import ZoneInfo  # Python 3.9+
        return ZoneInfo("Europe/Berlin")
    except Exception:
        return timezone.utc

def _dow_short(dt_local: datetime) -> str:
    return ["Mo", "Tu", "We", "Th", "Fr", "Sa", "Su"][dt_local.weekday()]

def _fmt_berlin(ts: Optional[float]) -> str:
    if ts is None:
        return "—"
    try:
        dt = datetime.fromtimestamp(float(ts), tz=timezone.utc).astimezone(_berlin_tz())
        return f"{_dow_short(dt)} {dt.strftime('%d/%m--%H:%M')}"
    except Exception:
        return "—"

def _safe_ts_to_datestr(ts: Any) -> str:
    try:
        return datetime.fromtimestamp(float(ts), tz=timezone.utc).date().isoformat()
    except Exception:
        return ""

# ========== GLOBAL numbering map (status ↔ alerts ↔ sell <index>) ==========
_STATUS_INDEX_MAP: Dict[int, Tuple[str, str, str]] = {}
_STATUS_REV_INDEX_MAP: Dict[Tuple[str, str, str], int] = {}

def _rebuild_status_index_map():
    global _STATUS_INDEX_MAP, _STATUS_REV_INDEX_MAP
    _STATUS_INDEX_MAP = {}
    _STATUS_REV_INDEX_MAP = {}

    structure = get_trade_structure()

    open_list: List[Tuple[str,str,str,float]] = []  # (SYM, track, cycle, opened_ts)
    buy_list:  List[Tuple[str,str,str,float]] = []  # (SYM, track, cycle, start/opened_ts)

    trades: List[Dict[str, Any]] = []
    if os.path.exists(TRADES_FILE):
        try:
            with open(TRADES_FILE, 'r') as f:
                tdata = json.load(f)
            trades = tdata.get("trades", []) or []
        except Exception:
            trades = []

    def _find_latest_open_ts(sym_up: str, track_num: str, cycle_code: str) -> Optional[float]:
        latest_ts = None
        for tr in trades:
            if (tr.get("symbol") or "").upper().replace("-", "").replace("/", "") != sym_up:
                continue
            if str(tr.get("track_num")) != str(track_num):
                continue
            if str(tr.get("cycle_num")) != str(cycle_code):
                continue
            ts = tr.get("opened_at")
            if ts is None:
                continue
            tsf = float(ts)
            if (latest_ts is None) or (tsf > latest_ts):
                latest_ts = tsf
        return latest_ts

    for tkey, tdata in sorted(structure.get("tracks", {}).items(), key=lambda kv: int(kv[0])):
        for cname, cell in (tdata.get("cycles") or {}).items():
            if not cell:
                continue
            st  = (cell.get("status") or "").lower()
            sym = (cell.get("symbol") or "").upper()
            if not sym:
                continue
            cycle_code = str(cell.get("cycle_num") or cname)
            if st in ("open", "reserved"):
                ts = _find_latest_open_ts(sym, str(tkey), cycle_code) or time.time()
                open_list.append((sym, str(tkey), cycle_code, ts))
            elif st == "buy":
                st_iso = cell.get("start_time")
                ts = None
                if st_iso:
                    try:
                        dt = datetime.fromisoformat(st_iso)
                        if not dt.tzinfo: dt = dt.replace(tzinfo=timezone.utc)
                        ts = dt.timestamp()
                    except Exception:
                        ts = None
                if ts is None:
                    ts = _find_latest_open_ts(sym, str(tkey), cycle_code) or time.time()
                buy_list.append((sym, str(tkey), cycle_code, ts))

    open_list_sorted = sorted(open_list, key=lambda x: (x[0], int(x[1]), x[2]))
    buy_list_sorted  = sorted(buy_list,  key=lambda x: (x[0], int(x[1]), x[2]))

    idx = 1
    for sym, t, c, _ in open_list_sorted:
        _STATUS_INDEX_MAP[idx] = (sym, t, c)
        _STATUS_REV_INDEX_MAP[(sym, t, c)] = idx
        idx += 1
    for sym, t, c, _ in buy_list_sorted:
        _STATUS_INDEX_MAP[idx] = (sym, t, c)
        _STATUS_REV_INDEX_MAP[(sym, t, c)] = idx
        idx += 1

# ============ STATUS (بدون لوحة) ============
async def show_bot_status():
    today = date.today().isoformat()
    structure = get_trade_structure()

    trades: List[Dict[str, Any]] = []
    if os.path.exists(TRADES_FILE):
        try:
            with open(TRADES_FILE, 'r') as f:
                tdata = json.load(f)
            trades = tdata.get("trades", []) or []
        except Exception as e:
            print(f"status read error: {e}")

    total_overall = len(trades)
    overall_tp       = sum(1 for tr in trades if (tr.get("status") or "").lower() == "closed")
    overall_loss     = sum(1 for tr in trades if (tr.get("status") or "").lower() == "stopped")
    overall_failed   = sum(1 for tr in trades if (tr.get("status") or "").lower() == "failed")
    overall_drawdown = sum(1 for tr in trades if (tr.get("status") or "").lower() == "drwn")

    latest_opened_date: Dict[str, str] = {}
    for tr in trades:
        sym = (tr.get("symbol") or "").upper()
        if not sym: continue
        d = _safe_ts_to_datestr(tr.get("opened_at"))
        if d:
            prev = latest_opened_date.get(sym)
            if (not prev) or (d > prev):
                latest_opened_date[sym] = d

    today_total  = sum(1 for tr in trades if _safe_ts_to_datestr(tr.get("opened_at")) == today)

    open_cells: List[Tuple[str, str, str, float]] = []
    buy_cells:  List[Tuple[str, str, str, float]] = []
    open_syms:  List[str] = []
    buy_syms:   List[str] = []

    def _latest_open_ts_for(sym: str, tkey: str, cycle_code: str) -> Optional[float]:
        latest_ts = None
        for tr in trades:
            if (tr.get("symbol") or "").upper().replace('-', '').replace('/', '') != sym:
                continue
            if str(tr.get("track_num")) != str(tkey):
                continue
            if str(tr.get("cycle_num")) != str(cycle_code):
                continue
            ts = tr.get("opened_at")
            if ts is None:
                continue
            tsf = float(ts)
            if (latest_ts is None) or (tsf > latest_ts):
                latest_ts = tsf
        return latest_ts

    try:
        for tkey, tdata in sorted(structure.get("tracks", {}).items(), key=lambda kv: int(kv[0])):
            track_num = str(tkey)
            for cname, cell in (tdata.get("cycles") or {}).items():
                if not cell: continue
                st  = (cell.get("status") or "").lower()
                sym = (cell.get("symbol") or "").upper().replace('-', '').replace('/', '')
                if not sym: continue
                cycle_num = str(cell.get("cycle_num") or cname)
                if st in ("open", "reserved"):
                    ts_open = _latest_open_ts_for(sym, track_num, cycle_num) or time.time()
                    open_cells.append((sym, track_num, cycle_num, ts_open)); open_syms.append(sym)
                elif st == "buy":
                    st_iso = cell.get("start_time")
                    ts_buy = None
                    if st_iso:
                        try:
                            dt = datetime.fromisoformat(st_iso)
                            if not dt.tzinfo: dt = dt.replace(tzinfo=timezone.utc)
                            ts_buy = dt.timestamp()
                        except Exception:
                            ts_buy = None
                    if ts_buy is None:
                        tr_latest = _latest_open_ts_for(sym, track_num, cycle_num)
                        ts_buy = tr_latest if tr_latest is not None else time.time()
                    buy_cells.append((sym, track_num, cycle_num, ts_buy)); buy_syms.append(sym)
    except Exception as e:
        print(f"status structure scan error: {e}")

    overall_open = len(open_cells)
    overall_buy  = len(buy_cells)
    today_open   = sum(1 for sym in set(open_syms) if latest_opened_date.get(sym) == today)
    today_buy    = sum(1 for sym in set(buy_syms)  if latest_opened_date.get(sym) == today)

    tp_today = loss_today = failed_today = drawdown_today = 0
    tp_today_entries: List[str] = []
    loss_today_entries: List[str] = []
    failed_today_entries: List[str] = []
    drawdown_today_entries: List[str] = []

    def _fmt_open_close_line(tr: Dict[str, Any]) -> str:
        sym = (tr.get("symbol") or "").upper()
        track_num = str(tr.get("track_num") or "?")
        cycle_num = str(tr.get("cycle_num") or "?")
        return f"• {sym} — T {track_num} | C {cycle_num}"

    for tr in trades:
        st = (tr.get("status") or "").lower()
        closed_d = _safe_ts_to_datestr(tr.get("closed_at"))
        if closed_d != today:
            continue
        if st == "closed":
            tp_today += 1; tp_today_entries.append(_fmt_open_close_line(tr))
        elif st == "stopped":
            loss_today += 1; loss_today_entries.append(_fmt_open_close_line(tr))
        elif st == "failed":
            failed_today += 1; failed_today_entries.append(_fmt_open_close_line(tr))
        elif st == "drwn":
            drawdown_today += 1; drawdown_today_entries.append(_fmt_open_close_line(tr))

    cap = get_effective_max_open(structure)
    free_now = max(0, cap - (overall_open + overall_buy))

    def _safe_pct(num: int, den: int) -> float:
        try:
            den = int(den)
            if den <= 0: return 0.0
            return (float(num) / float(den)) * 100.0
        except Exception:
            return 0.0

    realized_total = overall_tp + overall_loss + overall_drawdown
    tp_pct        = _safe_pct(overall_tp,       realized_total)
    loss_pct      = _safe_pct(overall_loss,     realized_total)
    drawdown_pct  = _safe_pct(overall_drawdown, realized_total)

    open_sorted = sorted(open_cells, key=lambda x: (x[0], int(x[1]), x[2]))
    buy_sorted  = sorted(buy_cells,  key=lambda x: (x[0], int(x[1]), x[2]))
    global _STATUS_INDEX_MAP, _STATUS_REV_INDEX_MAP
    _STATUS_INDEX_MAP = {}
    _STATUS_REV_INDEX_MAP = {}
    idx = 1
    for sym, track_num, cycle_num, ts in open_sorted:
        _STATUS_INDEX_MAP[idx] = (sym, track_num, cycle_num)
        _STATUS_REV_INDEX_MAP[(sym, track_num, cycle_num)] = idx
        idx += 1
    for sym, track_num, cycle_num, ts in buy_sorted:
        _STATUS_INDEX_MAP[idx] = (sym, track_num, cycle_num)
        _STATUS_REV_INDEX_MAP[(sym, track_num, cycle_num)] = idx
        idx += 1

    # Gate state text uses Section 2 primitive
    try:
        gate_txt = 'OPEN ✅' if is_email_gate_open() else 'CLOSED ⛔️'
    except Exception:
        gate_txt = 'OPEN ✅'

    lines: List[str] = [
        "📊 Bot Status:",
        f"✅ Running: {'Yes' if is_bot_active() else 'No'}",
        f"📧 Email Gate: {gate_txt}",
        f"📈 Totals Today: {today_total}",
        f" — open: {today_open} |Buy: {today_buy} | 🏆 TP: {tp_today} | ❌ SL: {loss_today} | ⚠️ Failed: {failed_today} | 📉 DRWDN: {drawdown_today}",
        "",
        f"📈 Gesamt: {total_overall}",
        f" open: {overall_open} |Buy: {overall_buy} | 🏆 TP: {overall_tp} , {tp_pct:.2f} % | ❌ SL: {overall_loss} , {loss_pct:.2f} % | 📉 DRWDN: {overall_drawdown} , {drawdown_pct:.2f} %",
        f"⚠️ Failed: {overall_failed}",
        "",
        f"📌 Open/Buy now: {overall_open + overall_buy} / 🔓 Free: {free_now} (cap: {cap})",
        "",
        "📜 Open Trades:",
    ]

    i = 1
    if open_sorted:
        for sym, track_num, cycle_num, ts in open_sorted:
            ts_fmt = _fmt_berlin(ts)
            lines.append(f"• {i}. {ts_fmt} {sym} — T {track_num} | C {cycle_num}")
            i += 1
    else:
        lines.append("• (none)")

    lines.extend(["", "📜 Buy Trades :"])
    if buy_sorted:
        for sym, track_num, cycle_num, ts in buy_sorted:
            ts_fmt = _fmt_berlin(ts)
            cell = (structure.get("tracks", {}).get(str(track_num), {}).get("cycles", {}) or {}).get(cycle_num) or {}
            bp = cell.get("bought_price")
            try:
                bought_price = float(bp) if bp is not None else None
            except Exception:
                bought_price = None
            now_price = await fetch_current_price(sym)
            pct_str = "—"
            if bought_price and now_price:
                try:
                    pct = ((float(now_price) - float(bought_price)) / float(bought_price)) * 100.0
                    pct_str = f"{pct:+.2f}%"
                except Exception:
                    pct_str = "—"
            bp_str = f"{bought_price:.6f}" if bought_price else "—"
            now_str = f"{now_price:.6f}" if now_price else "N/A"
            lines.append(f"• {i}. {ts_fmt} {sym} — T {track_num} | C {cycle_num}  {bp_str} → now {now_str} / Δ {pct_str}")
            i += 1
    else:
        lines.append("• (none)")

    lines.extend(["", "✅TP Trades   :"])
    lines.extend(tp_today_entries or ["(none)"])
    lines.extend(["", "❌ SL (today):"])
    lines.extend(loss_today_entries or ["(none)"])
    lines.extend(["", "📉 DRWDN (today):"])
    lines.extend(drawdown_today_entries or ["(none)"])
    lines.extend(["", "⚠️ Failed Trades Today:"])
    lines.extend(failed_today_entries or ["(none)"])

    lines.extend(["", "🪵 Terminal Notices:"])
    if os.path.exists(TERMINAL_LOG_FILE):
        try:
            with open(TERMINAL_LOG_FILE, 'r') as f:
                notif_log = json.load(f) or {}
            if notif_log:
                items = sorted(notif_log.items(), key=lambda kv: kv[1].get("count", 0), reverse=True)
                notif_summary = "\n".join([f"• {msg} (x{info['count']})" for msg, info in items])
            else:
                notif_summary = "(none)"
        except Exception:
            notif_summary = "(none)"
    else:
        notif_summary = "(none)"
    lines.append(notif_summary)

    await _send_long_message("\n".join(lines), part_title="📊 Bot Status")

# === Helpers used by track/slots/nextslots/verlauf (unchanged basics) ===
def _extract_label(cycle_code: str) -> str:
    try:
        m = re.match(r"([A-Za-z]+)", str(cycle_code))
        return m.group(1).upper() if m else ""
    except Exception:
        return ""

def _find_latest_trade_record(trades: List[Dict[str, Any]], track_key: str, cycle_code: str, sym: str) -> Optional[Dict[str, Any]]:
    sym_norm = (sym or "").upper().replace("-", "").replace("/", "")
    latest = None; latest_ts = -1.0
    for tr in trades:
        try:
            if str(tr.get("track_num")) != str(track_key): continue
            if str(tr.get("cycle_num")) != str(cycle_code): continue
            if (tr.get("symbol") or "").upper().replace("-", "").replace("/", "") != sym_norm: continue
            ts = float(tr.get("opened_at", 0) or 0)
            if ts >= latest_ts:
                latest_ts = ts
                latest = tr
        except Exception:
            continue
    return latest

# ====== SLOTS & NEXTSLOTS COMMANDS (enabled) ======
def _fallback_cycle_labels(n: int):
    """مولّد بسيط للّوابل إذا لم تتوفر get_cycle_labels (A..Z ثم AA..AZ...)."""
    labels = []
    from string import ascii_uppercase as AZ
    # يكفي للأعداد الكبيرة بشكل معقول
    i = 0
    while len(labels) < max(1, int(n)):
        if i < 26:
            labels.append(AZ[i])
        else:
            k = i - 26
            labels.append(AZ[k // 26] + AZ[k % 26])
        i += 1
    return labels[:n]

def _labels_for_count(structure):
    cnt = int(structure.get("cycle_count", globals().get("DEFAULT_CYCLE_COUNT", 10)))
    if 'get_cycle_labels' in globals() and callable(globals()['get_cycle_labels']):
        try:
            return get_cycle_labels(cnt)
        except Exception:
            pass
    return _fallback_cycle_labels(cnt)

async def cmd_list_slots(all_cycles: bool = False):
    """
    يعرض جميع الخانات الفارغة:
      - داخل حدود cycle_count (الأفتراضي)
      - أو كل الخانات (عند all_cycles=True)
    """
    try:
        structure = get_trade_structure()
        labels_in_range = set(_labels_for_count(structure))
        tracks = structure.get("tracks", {}) or {}
        if not tracks:
            await send_notification("ℹ️ لا توجد مسارات بعد.")
            return

        empty_in = []   # ضمن حدود cycle_count
        empty_out = []  # خارج الحدود

        for tkey in sorted(tracks.keys(), key=lambda x: int(x)):
            cycles = (tracks[tkey].get("cycles") or {})
            for cname, cell in cycles.items():
                is_empty = (not cell) or (isinstance(cell, dict) and not cell.get("status"))
                if not is_empty:
                    continue
                m = re.match(r"([A-Za-z]+)", str(cname))
                lab = (m.group(1).upper() if m else "")
                line = f"• T {tkey} | C {cname}"
                if lab in labels_in_range:
                    empty_in.append(line)
                else:
                    empty_out.append(line)

        if not empty_in and (not all_cycles or not empty_out):
            await send_notification("ℹ️ لا توجد خانات فارغة حالياً ضمن حدود الدورات.")
            return

        lines = ["🧩 Empty slots:"]
        if empty_in:
            lines.append("— داخل حدود cycle_count:")
            lines.extend(sorted(empty_in))
        else:
            lines.append("— داخل حدود cycle_count: (none)")

        if all_cycles:
            lines.append("")
            lines.append("— خارج حدود cycle_count:")
            lines.extend(sorted(empty_out) if empty_out else ["(none)"])

        await _send_long_message("\n".join(lines), part_title="slots")
    except Exception as e:
        await send_notification(f"⚠️ slots error: {e}")

async def cmd_list_nextslots():
    """
    يعرض لكل Label الخانة التالية المرشّحة (أقرب خانة فارغة) اعتمادًا على
    structure['cycle_track_ptr'] إن وُجد (دون تعديل المؤشر).
    """
    try:
        structure = get_trade_structure()
        tracks = structure.get("tracks", {}) or {}
        if not tracks:
            await send_notification("ℹ️ لا توجد مسارات بعد.")
            return

        labels = _labels_for_count(structure)
        ptr = dict(structure.get("cycle_track_ptr") or {})
        for lab in labels:
            if str(lab) not in ptr:
                ptr[str(lab)] = 1

        max_track = max(int(k) for k in tracks.keys()) if tracks else 0
        if max_track <= 0:
            await send_notification("ℹ️ لا توجد مسارات مُهيّأة.")
            return

        lines = ["🔮 Next candidate slots per label:"]
        for lab in labels:
            start = int(ptr.get(str(lab), 1))
            found = None
            for step in range(max_track):
                tnum = ((start - 1 + step) % max_track) + 1
                cname = f"{lab}{tnum}"
                cell = (tracks.get(str(tnum), {}).get("cycles") or {}).get(cname)
                is_empty = (not cell) or (isinstance(cell, dict) and not cell.get("status"))
                if is_empty:
                    found = (tnum, cname)
                    break
            if found:
                lines.append(f"• {lab}:  T {found[0]} | C {found[1]}")
            else:
                lines.append(f"• {lab}:  (no free slot)")

        await _send_long_message("\n".join(lines), part_title="nextslots")
    except Exception as e:
        await send_notification(f"⚠️ nextslots error: {e}")

# --- Single track details (split SL vs DRWDN) ---
async def show_single_track_status(track_index: int):
    try:
        structure = get_trade_structure()
        tkey = str(track_index)
        tdata = structure.get("tracks", {}).get(tkey)
        if not tdata:
            await send_notification(f"⚠️ المسار {track_index} غير موجود.")
            return
        amount = float(tdata.get("amount", 0) or 0)
        cycles = tdata.get("cycles", {}) or {}

        trades: List[Dict[str, Any]] = []
        if os.path.exists(TRADES_FILE):
            try:
                with open(TRADES_FILE, 'r') as f:
                    tdata_all = json.load(f)
                trades = tdata_all.get("trades", [])
            except Exception:
                trades = []

        def _pct(a: Optional[float], b: Optional[float]) -> Optional[float]:
            try:
                if a is None or b is None: return None
                a = float(a); b = float(b)
                if a == 0.0: return None
                return ((b - a) / a) * 100.0
            except Exception:
                return None

        lines: List[str] = [f"🔎 Track {tkey} / {amount:.2f} $ — details"]
        open_entries: List[str] = []
        buy_entries:  List[str] = []
        tp_entries:   List[str] = []
        sl_entries:   List[str] = []
        drw_entries:  List[str] = []

        for cname, cell in cycles.items():
            if not cell: continue
            st = (cell.get("status") or "").lower()
            sym = (cell.get("symbol") or "").upper()
            if not sym: continue
            cycle_code = (cell.get("cycle_num") or cname)
            if st in ("open", "reserved"):
                tr = _find_latest_trade_record(trades, tkey, cycle_code, sym)
                ts_open = tr.get("opened_at") if tr else None
                open_entries.append(
                    f"{_fmt_berlin(ts_open)} {sym} — T {tkey} | C {cycle_code} / "
                    f"Entry≤{float(cell.get('entry',0) or 0):.6f} / TP1≥{float((cell.get('targets') or [0])[0]):.6f} / SL≤{float(cell.get('sl',0) or 0):.6f}"
                )
            elif st == "buy":
                st_iso = cell.get("start_time")
                ts_buy = None
                if st_iso:
                    try:
                        dt = datetime.fromisoformat(st_iso)
                        if not dt.tzinfo: dt = dt.replace(tzinfo=timezone.utc)
                        ts_buy = dt.timestamp()
                    except Exception:
                        pass
                if ts_buy is None:
                    tr_latest = _find_latest_trade_record(trades, tkey, cycle_code, sym)
                    ts_buy = tr_latest.get("opened_at") if tr_latest else None
                bought_price = float(cell.get("bought_price", 0) or 0)
                now_price: Optional[float] = await fetch_current_price(sym)
                pct = _pct(bought_price, now_price) if (now_price is not None and bought_price > 0) else None
                pct_str = (f"{pct:+.2f}%" if pct is not None else "—")
                now_str = (f"{now_price:.6f}" if now_price is not None else "N/A")
                buy_entries.append(
                    f"{_fmt_berlin(ts_buy)} {sym} — T {tkey} | C {cycle_code} / buy {bought_price:.6f} → now {now_str} / Δ {pct_str}"
                )

        for tr in trades:
            if str(tr.get("track_num")) != tkey:
                continue
            st = (tr.get("status") or "").lower()
            if st not in ("closed", "stopped", "drwn"):
                continue
            sym = (tr.get("symbol") or "").upper()
            cycle_code = str(tr.get("cycle_num") or "") or "?"
            open_ts  = tr.get("opened_at")
            close_ts = tr.get("closed_at")
            bought_exec = tr.get("bought_price"); sell_exec = tr.get("sell_price")
            pct = None
            try:
                if bought_exec is not None and sell_exec is not None and float(bought_exec) != 0.0:
                    pct = ((float(sell_exec) - float(bought_exec)) / float(bought_exec)) * 100.0
            except Exception:
                pct = None
            pct_str = (f"{pct:+.2f}%" if pct is not None else "—")
            tag = "TP" if st == "closed" else ("SL" if st == "stopped" else "DRWDN")
            linestr = f"{_fmt_berlin(close_ts)} {sym} — T {tkey} | C {cycle_code} / {tag} / Δ {pct_str}  {_fmt_berlin(open_ts)}"
            if st == "closed":
                tp_entries.append(linestr)
            elif st == "stopped":
                sl_entries.append(linestr)
            else:
                drw_entries.append(linestr)

        c_open = len(open_entries); c_buy = len(buy_entries)
        c_tp   = len(tp_entries);   c_sl  = len(sl_entries); c_drw = len(drw_entries)
        lines.append(f"open: {c_open} | Buy: {c_buy} | TP: {c_tp} | SL: {c_sl} | DRWDN: {c_drw}\n")
        if open_entries:
            lines.append("📜 Open:"); lines.extend(sorted(open_entries)); lines.append("")
        if buy_entries:
            lines.append("📜 Buy:");  lines.extend(sorted(buy_entries)); lines.append("")
        if tp_entries:
            lines.append("✅ TP (realized):"); lines.extend(tp_entries); lines.append("")
        if sl_entries:
            lines.append("🛑 SL (realized):"); lines.extend(sl_entries); lines.append("")
        if drw_entries:
            lines.append("📉 DRWDN (realized):"); lines.extend(drw_entries); lines.append("")
        msg = "\n".join(lines).rstrip()
        await _send_long_message(msg, part_title=f"Track {tkey} details")
    except Exception as e:
        await send_notification(f"⚠️ خطأ في عرض تفاصيل المسار {track_index}: {e}")

# --- All tracks overview (split SL vs DRWDN) ---
async def show_tracks_status():
    try:
        structure = get_trade_structure()
        trades = []
        if os.path.exists(TRADES_FILE):
            try:
                with open(TRADES_FILE, 'r') as f:
                    tdata = json.load(f)
                trades = tdata.get("trades", [])
            except Exception:
                trades = []
        def _format_duration(open_ts: Any, close_ts: Any) -> str:
            try:
                if open_ts is None or close_ts is None: return ""
                t1 = datetime.fromtimestamp(float(open_ts), tz=timezone.utc)
                t2 = datetime.fromtimestamp(float(close_ts), tz=timezone.utc)
                if t2 < t1: return ""
                delta = t2 - t1
                d = delta.days; h = delta.seconds // 3600; m = (delta.seconds % 3600) // 60
                return f"{d}d / {h}h / {m}m"
            except Exception:
                return ""
        lines: List[str] = []
        for tnum in sorted(structure.get("tracks", {}).keys(), key=lambda x: int(x)):
            tdata = structure["tracks"][tnum]
            amount = float(tdata.get("amount", 0) or 0)
            cycles = tdata.get("cycles", {}) or {}
            open_entries: List[str] = []; buy_entries:  List[str] = []
            tp_entries:   List[str] = []; sl_entries:   List[str] = []; drw_entries: List[str] = []
            for cname, cell in cycles.items():
                if not cell: continue
                st = (cell.get("status") or "").lower()
                sym = (cell.get("symbol") or "").upper()
                if not sym: continue
                cycle_code = (cell.get("cycle_num") or cname)
                if st in ("open", "reserved"): open_entries.append(f"{sym} — C {cycle_code} / open")
                elif st == "buy":              buy_entries.append(f"{sym} — C {cycle_code} / buy")
            for tr in trades:
                if str(tr.get("track_num")) != str(tnum): continue
                st = (tr.get("status") or "").lower()
                if st not in ("closed", "stopped", "drwn"): continue
                sym = (tr.get("symbol") or "").upper()
                cycle_code = str(tr.get("cycle_num") or "") or "?"
                dur = _format_duration(tr.get("opened_at"), tr.get("closed_at"))
                if st == "closed": tp_entries.append(f"{sym} — C {cycle_code} / TP / {dur}")
                elif st == "stopped": sl_entries.append(f"{sym} — C {cycle_code} / SL / {dur}")
                else: drw_entries.append(f"{sym} — C {cycle_code} / DRWDN / {dur}")
            c_open = len(open_entries); c_buy  = len(buy_entries)
            c_tp   = sum(1 for tr in trades if str(tr.get("track_num")) == str(tnum) and (tr.get("status") or "").lower() == "closed")
            c_sl   = sum(1 for tr in trades if str(tr.get("track_num")) == str(tnum) and (tr.get("status") or "").lower() == "stopped")
            c_drw  = sum(1 for tr in trades if str(tr.get("track_num")) == str(tnum) and (tr.get("status") or "").lower() == "drwn")
            total_cycles = c_open + c_buy + c_tp + c_sl + c_drw
            lines.append(f"Track : {tnum} / {amount:.2f} $ / {total_cycles} Cycle")
            lines.append(f"open: {c_open} | Buy: {c_buy} | TP: {c_tp} | SL: {c_sl} | DRWDN: {c_drw}")
            if open_entries: lines.extend(sorted(open_entries, key=lambda s: (s.split(' — ')[1], s.split(' — ')[0])))
            if buy_entries:  lines.extend(sorted(buy_entries,  key=lambda s: (s.split(' — ')[1], s.split(' — ')[0])))
            if tp_entries:   lines.extend(tp_entries)
            if sl_entries:   lines.extend(sl_entries)
            if drw_entries:  lines.extend(drw_entries)
            lines.append("")
        if not lines:
            await send_notification("ℹ️ لا يوجد أي مسارات حالياً.")
        else:
            await _send_long_message("\n".join(lines).rstrip(), part_title="Tracks status")
    except Exception as e:
        await send_notification(f"⚠️ خطأ في عرض حالة المسارات: {e}")

# --- Terminal notices cleaner ---
async def clean_terminal_notices():
    try:
        with open(TERMINAL_LOG_FILE, 'w') as f:
            json.dump({}, f, indent=2)
        await send_notification("🧹 Terminal notices cleared. Logs are clean now.")
    except Exception as e:
        await send_notification(f"⚠️ Failed to clear terminal notices: {e}")

# --- cycl <N> dynamic cycle count ---
async def apply_cycle_count(new_count: int):
    try:
        structure = get_trade_structure()
        old_count = int(structure.get("cycle_count", DEFAULT_CYCLE_COUNT))
        new_count = max(1, int(new_count))
        if new_count == old_count:
            await send_notification(f"ℹ️ Cycle count unchanged (still {new_count})."); return
        labels_old = get_cycle_labels(old_count)
        labels_new = get_cycle_labels(new_count)
        if new_count > old_count:
            added = labels_new[len(labels_old):]
            for tkey, tdata in (structure.get("tracks") or {}).items():
                cycles = (tdata or {}).get("cycles") or {}
                for lab in added:
                    key = f"{lab}{tkey}"
                    if key not in cycles:
                        cycles[key] = None
                tdata["cycles"] = cycles
            ptr = structure.get("cycle_track_ptr") or {}
            for lab in added:
                if lab not in ptr: ptr[lab] = 1
            structure["cycle_track_ptr"] = ptr
            structure["cycle_count"] = new_count
            save_trade_structure(structure)
            head = f"✅ Cycle count increased: {old_count} → {new_count}"
            if added:
                await send_notification(f"{head}\n➕ Added labels per track: {added[0]}..{added[-1]} (empty slots, no impact on active trades).")
            else:
                await send_notification(head)
            return
        structure["cycle_count"] = new_count
        save_trade_structure(structure)
        outside_active: List[str] = []
        allowed = set(labels_new)
        for tkey, tdata in (structure.get("tracks") or {}).items():
            cycles = (tdata or {}).get("cycles") or {}
            for cname, cell in cycles.items():
                if not cell: continue
                st = (cell.get("status") or "").lower()
                if st not in ("open", "buy", "reserved"): continue
                m = re.match(r"([A-Za-z]+)\d+", str(cname))
                lab = m.group(1).upper() if m else None
                if lab and lab not in allowed:
                    outside_active.append(str(cname))
        if outside_active:
            preview = ", ".join(outside_active[:20]) + (" …" if len(outside_active) > 20 else "")
            await send_notification(
                "⚠️ Cycle count decreased: "
                f"{old_count} → {new_count}\n"
                f"Active cycles above limit will remain visible until they close:\n"
                f"{preview}\n"
                "ℹ️ No new trades will be opened on these cycles."
            )
        else:
            await send_notification(f"✅ Cycle count decreased: {old_count} → {new_count}\n(no active cycles above the new limit).")
    except Exception as e:
        await send_notification(f"❌ cycl error: {e}")

# ====== Parser مرن لنصوص القناة ======
def _parse_signal_text(text: str):
    """
    - Symbol from '#SYMBOL' (with/without - or /)
    - BUY as 'BUY - <price>' or 'BUY: <price>'
    - TPn as 'TP1 - 0.123' or 'TP 1: 0.123' (ignores any 'TP LONG' line)
    - SL as 'SL - <price>'
    """
    t = (text or "")
    # إزالة رموز الاتجاه (RTL/LTR) والحروف الرسومية (├ │ ─ ...) لتسهيل المطابقة
    t = t.replace("\u200f", "").replace("\u200e", "")
    t = re.sub(r"[├┤└┘┐┌┴┬┼│─]", " ", t)

    def _normalize_number(val: str) -> float:
        """دعم الفواصل أو المسافات داخل الرقم."""
        v = (val or "").strip().replace(" ", "")
        # السماح بفواصل كفاصل عشري (0,734 → 0.734) وإزالة فواصل الآلاف
        if v.count(",") > 1 and "." not in v:
            v = v.replace(",", "")
        else:
            v = v.replace(",", ".")
        return float(v)

    # symbol (السطر قد يحتوي #SYMBOL أو SYMBOL/USDT دون #)
    m_sym = re.search(r"#\s*([A-Z0-9\-_\/]+)", t, re.IGNORECASE)
    if not m_sym:
        m_sym = re.search(r"\b([A-Z0-9]{2,}[/\-]?USDT)\b", t, re.IGNORECASE)
    if not m_sym:
        raise ValueError("symbol not found")
    symbol = m_sym.group(1).upper().replace("-", "").replace("/", "")

    # buy
    m_buy = re.search(r"\bBUY\b[^0-9]*([0-9][0-9\.,]*)", t, re.IGNORECASE)
    if not m_buy:
        raise ValueError("buy not found")
    entry = _normalize_number(m_buy.group(1))

    # remove TP LONG lines before scanning
    t_clean = re.sub(r"TP\s*LONG.*", "", t, flags=re.IGNORECASE)
    # TPs
    tps_pairs = re.findall(r"\bTP\s*(\d+)\s*[-:]?\s*([0-9][0-9\.,]*)", t_clean, re.IGNORECASE)
    tps_sorted = [_normalize_number(val) for _, val in sorted(((int(n), v) for n, v in tps_pairs), key=lambda x: x[0])]

    # SL
    m_sl = re.search(r"\bSL\b[^0-9]*([0-9][0-9\.,]*)", t, re.IGNORECASE)
    sl = _normalize_number(m_sl.group(1)) if m_sl else 0.0

    return symbol, entry, tps_sorted, sl

# -- Channel: recommendations listener (Email Gate + Blacklist) --
def _resolve_channel_chat():
    target = (CHANNEL_USERNAME or "").strip()
    if not target:
        return None
    if target.startswith("http") or target.startswith("tg://") or target.startswith("+"):
        try:
            return client.loop.run_until_complete(client.get_entity(target))
        except Exception as e:
            print(f"❌ Failed to resolve channel link: {e}")
            return None
    return target

def attach_channel_handler():
    if events is None:
        print("⚠️ Telethon events unavailable; recommendations listener disabled.")
        return

    chats_target = _resolve_channel_chat()
    if not chats_target:
        print("⚠️ CHANNEL_USERNAME not set or invalid; recommendations listener disabled.")
        return

    async def recommendation_handler(event):
        if not is_bot_active():
            return

        message = (event.raw_text or "").strip()

        # parse first to know the symbol (even if gate is closed)
        try:
            symbol, entry_price, targets, sl_price = _parse_signal_text(message)
        except Exception:
            return  # not a recognizable signal

        _console_echo(f"[REC] {symbol} | BUY {entry_price} | TPs={targets} | SL={sl_price}")

        # Email Gate check
        try:
            gate_ok = should_accept_recommendations()
        except Exception:
            gate_ok = True  # fail-open

        if not gate_ok:
            try:
                await send_notification_tc("⛔️ Recommendation ignored: Email gate is CLOSED.", symbol=symbol)
            except Exception:
                _console_echo("[GATE] CLOSED — ignored recommendation")
            return

        # Blacklist check
        try:
            if _is_blocked_symbol(symbol):
                await send_notification_tc("🚫 Ignored: symbol is in blacklist.", symbol=symbol)
                return
        except Exception:
            pass

        if not targets:
            await send_notification_tc("⚠️ No TP targets found.", symbol=symbol)
            return

        exec_fn = globals().get("EXECUTE_TRADE_FN") or globals().get("execute_trade")
        if not callable(exec_fn):
            await send_notification_tc("❌ Internal error: execute_trade not available (handler).", symbol=symbol)
            return
        await exec_fn(symbol, entry_price, sl_price, targets)

    client.add_event_handler(recommendation_handler, events.NewMessage(chats=chats_target))

# ---------- SELL helpers ----------

def _find_active_cells_by_symbol(symbol_norm: str):
    structure = get_trade_structure()
    out = []
    try:
        for tnum, tdata in structure.get("tracks", {}).items():
            for cname, cell in (tdata.get("cycles") or {}).items():
                if not cell: continue
                st = (cell.get("status") or "").lower()
                sym = (cell.get("symbol") or "").upper().replace("-", "").replace("/", "")
                if sym == symbol_norm and st in ("open", "buy", "reserved"):
                    out.append((str(tnum), cname, cell))
    except Exception as e:
        print(f"_find_active_cells_by_symbol error: {e}")
    return structure, out

# ====== VERLAUF: Full timeline of all trades ======

def _fmt_dt(ts: Optional[float]) -> str:
    if ts is None: return "—"
    try:
        dt = datetime.fromtimestamp(float(ts), tz=timezone.utc).astimezone(_berlin_tz())
        return dt.strftime("%d.%m %H:%M:%S")
    except Exception:
        return "—"

async def show_verlauf():
    if not os.path.exists(TRADES_FILE):
        await send_notification("ℹ️ لا توجد صفقات بعد."); return
    try:
        with open(TRADES_FILE, 'r') as f:
            data = json.load(f) or {}
        trades = data.get("trades", []) or []
    except Exception as e:
        await send_notification(f"⚠️ قراءة TRADES_FILE فشلت: {e}"); return

    trades = sorted(trades, key=lambda tr: float(tr.get("opened_at", 0) or 0))

    lines: List[str] = ["📜 Verlauf — سجلّ الحركات الكاملة"]
    for tr in trades:
        try:
            sym = (tr.get("symbol") or "").upper()
            tnum = int(str(tr.get("track_num") or 0) or 0)
            cycle = str(tr.get("cycle_num") or "")
            lab = _extract_label(cycle) or ""
            opened_at = tr.get("opened_at"); bought_at = tr.get("bought_at"); sold_at = tr.get("sold_at")
            amount = float(tr.get("amount", 0) or 0)
            bought_price = tr.get("bought_price"); sell_price = tr.get("sell_price")
            qty = tr.get("sell_qty")
            status = (tr.get("status") or "").lower()

            lines.append(f"\n— {sym}")
            lines.append(f"📥 Signal @ {_fmt_dt(opened_at)} → will open at T{tnum}, C {cycle} | Amount {amount:.2f} USDT")

            if bought_price is not None:
                buy_ts_show = bought_at if bought_at is not None else opened_at
                usd_spent = (float(bought_price) * float(qty)) if (qty and bought_price) else amount
                qty_show = f"{float(qty):.6f}" if qty is not None else "—"
                lines.append(f"✅ Buy   @ {_fmt_dt(buy_ts_show)} → price {float(bought_price):.6f} | qty {qty_show} | ~USDT {usd_spent:.4f} | T{tnum}, C {cycle}")

            if status in ("closed", "stopped"):
                pnl_str = "—"
                if bought_price is not None and sell_price is not None and qty is not None:
                    pnl = (float(sell_price) - float(bought_price)) * float(qty)
                    sign = "+" if pnl >= 0 else "-"
                    pnl_str = f"{sign}{abs(pnl):.4f} USDT"
                ts_sell = sold_at if sold_at is not None else tr.get("closed_at")

                if status == "closed":
                    next_track = tnum + 1
                    next_cycle = f"{lab}{next_track}" if lab else f"{cycle}"
                    lines.append(
                        f"🏆 TP/TL @ {_fmt_dt(ts_sell)} → sell {float(sell_price) if sell_price is not None else 0.0:.6f} | PnL {pnl_str} | "
                        f"T{tnum}, C {cycle} → next T{next_track}, C {next_cycle}"
                    )
                elif status == "stopped":
                    back_track = max(1, tnum - 6)
                    back_cycle = f"{lab}{back_track}" if lab else f"{cycle}"
                    lines.append(
                        f"🛑 SL    @ {_fmt_dt(ts_sell)} → sell {float(sell_price) if sell_price is not None else 0.0:.6f} | PnL {pnl_str} | "
                        f"from T{tnum}, C {cycle} → back to T{back_track}, C {back_cycle}"
                    )
            elif status == "failed":
                lines.append(f"🚫 Canceled/Failed (no execution) | T{tnum}, C {cycle}")
            elif status == "drwn":
                pnl_str = "—"
                if bought_price is not None and sell_price is not None and qty is not None:
                    pnl = (float(sell_price) - float(bought_price)) * float(qty)
                    sign = "+" if pnl >= 0 else "-"
                    pnl_str = f"{sign}{abs(pnl):.4f} USDT"
                ts_sell = tr.get("sold_at") or tr.get("closed_at")
                lines.append(
                    f"📉 DRWDN @ {_fmt_dt(ts_sell)} → sell {float(sell_price) if sell_price is not None else 0.0:.6f} | PnL {pnl_str} | "
                    f"T{tnum}, C {cycle}"
                )

        except Exception as e:
            lines.append(f"(parse error on one trade: {e})")

    await _send_long_message("\n".join(lines), part_title="verlauf")

# ===== Commands on control account (instead of Saved Messages) =====
_pending_close_request = {"waiting": False}

COMMAND_CHAT = _resolve_command_chat()

@client.on(events.NewMessage(chats=COMMAND_CHAT))
async def command_handler(event):
    text = event.raw_text.strip()
    cmd = text.lower()

    _console_echo(f"[CMD] {text}")

    # ===== Blacklist commands (Add / Remove / Status List) =====
    if cmd.startswith("add "):
        sym = normalize_symbol(text.split(maxsplit=1)[1])
        try:
            added = add_to_blacklist(sym)
            if added:
                await send_notification(f"✅ Added {sym} to blacklist. Future signals will be ignored.")
            else:
                await send_notification(f"ℹ️ {sym} is already in the blacklist.")
        except Exception as e:
            await send_notification(f"❌ Failed to add {sym} to blacklist: {e}")
        return

    if cmd.startswith("remove "):
        sym = normalize_symbol(text.split(maxsplit=1)[1])
        try:
            removed = remove_from_blacklist(sym)
            if removed:
                await send_notification(f"✅ Removed {sym} from blacklist.")
            else:
                await send_notification(f"ℹ️ {sym} was not in the blacklist.")
        except Exception as e:
            await send_notification(f"❌ Failed to remove {sym} from blacklist: {e}")
        return

    if cmd == "status list":
        try:
            bl = list_blacklist()
            if bl:
                await send_notification("🚫 Blacklist symbols:\n" + "\n".join(f"• {s}" for s in bl))
            else:
                await send_notification("🚫 Blacklist is empty.")
        except Exception as e:
            await send_notification(f"❌ Failed to read blacklist: {e}")
        return

    # ===== Email Gate: status + manual control from Saved Messages =====
    if cmd in ("off", "gate"):
        await show_gate_status()
        return

    if cmd in ("gate close", "gate off"):
        try:
            # استخدم الدالة المركزية من Section 2 إن توفرت
            if 'set_email_gate' in globals() and callable(globals()['set_email_gate']):
                globals()['set_email_gate'](False)
            # fallback على الراپر في Section 7 إن كان موجودًا
            elif 'set_email_trade_gate' in globals() and callable(globals()['set_email_trade_gate']):
                globals()['set_email_trade_gate'](False)
            else:
                raise RuntimeError("set_email_gate is not available")
            await send_notification("📧 Email gate changed → CLOSED ⛔️ (blocking new recommendations)")
        except Exception as e:
            await send_notification(f"❌ Failed to close Email gate: {e}")
        return

    if cmd in ("gate open", "gate on"):
        try:
            if 'set_email_gate' in globals() and callable(globals()['set_email_gate']):
                globals()['set_email_gate'](True)
            elif 'set_email_trade_gate' in globals() and callable(globals()['set_email_trade_gate']):
                globals()['set_email_trade_gate'](True)
            else:
                raise RuntimeError("set_email_gate is not available")
            await send_notification("📧 Email gate changed → OPEN ✅ (accepting channel recommendations)")
        except Exception as e:
            await send_notification(f"❌ Failed to open Email gate: {e}")
        return

    # ===== Simulation toggle =====
    if cmd in ("sim on", "simulation on"):
        set_simulation_mode(True)
        await send_notification("🧪 Simulation mode is now ON (no real orders).")
        return

    if cmd in ("sim off", "simulation off"):
        set_simulation_mode(False)
        await send_notification("⚙️ Simulation mode is now OFF (live trading).")
        return

    # ===== Debug funds toggles =====
    if cmd.startswith("debug funds"):
        parts = cmd.split()
        try:
            if len(parts) == 3 and parts[2] == "on":
                enable_debug_funds(0)
                await send_notification("🟢 DEBUG_FUNDS enabled (no expiry).")
                return
            if len(parts) == 3 and parts[2] == "off":
                disable_debug_funds()
                await send_notification("🔴 DEBUG_FUNDS disabled.")
                return
            if len(parts) == 3 and parts[2].endswith("m"):
                n = int(parts[2][:-1])
                enable_debug_funds(n)
                await send_notification(f"🟢 DEBUG_FUNDS enabled for {n} minute(s).")
                return
            if len(parts) == 3 and parts[2].isdigit():
                n = int(parts[2])
                enable_debug_funds(n)
                await send_notification(f"🟢 DEBUG_FUNDS enabled for {n} minute(s).")
                return
            await send_notification("ℹ️ Usage: debug funds on | debug funds off | debug funds <N>m")
        except Exception as e:
            await send_notification(f"⚠️ debug funds error: {e}")
        return

    # ===== Slots commands (now enabled) =====
    if cmd == "slots":
        fn = globals().get("cmd_list_slots")
        if callable(fn):
            await fn(all_cycles=False)
        else:
            await send_notification("⚠️ أمر slots غير مُفعّل حاليًا.")
        return

    if cmd == "slots all":
        fn = globals().get("cmd_list_slots")
        if callable(fn):
            await fn(all_cycles=True)
        else:
            await send_notification("⚠️ أمر slots all غير مُفعّل حاليًا.")
        return

    if cmd == "nextslots":
        fn = globals().get("cmd_list_nextslots")
        if callable(fn):
            await fn()
        else:
            await send_notification("⚠️ أمر nextslots غير مُفعّل حاليًا.")
        return

    if cmd == "verlauf":
        await show_verlauf(); return

    # ===== Manual close flow =====
    if cmd == "close":
        _pending_close_request["waiting"] = True
        await send_notification(
            "🧩 Send details in this template (copy & edit):\n\n"
            "Close 📥 New signal:\n"
            "📌 Symbol: COTIUSDT\n"
            "🎯 Entry ≤ 0.05621, TP1 ≥ 0.0573342, SL ≤ 0.050589\n"
            "📈 Track: 1 | Cycle: A10\n"
            "💵 Amount: 50.0 USDT\n"
            "ℹ️ I will SELL at TP touch, or after a 1h candle closes ≤ SL."
        )
        return

    if cmd == "cancel":
        _pending_close_request["waiting"] = False
        await send_notification("🛑 Manual-close request canceled.")
        return

    if _pending_close_request.get("waiting"):
        details = text
        try:
            details_norm = details.replace('،', ',')
            sym_guess = None
            m_sym_guess = re.search(r"(?:Symbol:\s*|📌\s*)([A-Z0-9\-\_/]+)", details_norm, re.IGNORECASE)
            if m_sym_guess:
                sym_guess = m_sym_guess.group(1).upper().replace('-', '').replace('/', '')

            m_sym_re = re.search(r"(?:Symbol:\s*|📌\s*)([A-Z0-9\-\_/]+)", details_norm, re.IGNORECASE)
            nums_re = re.search(
                r"Entry\s*(?:≤|<=)\s*([0-9]*\.?[0-9]+)\s*,\s*TP1\s*(?:≥|>=)\s*([0-9]*\.?[0-9]+)\s*,\s*SL\s*(?:≤|<=)\s*([0-9]*\.?[0-9]+)",
                details_norm, re.IGNORECASE
            )
            pos_re  = re.search(r"Track:\s*(\d+)\s*\|\s*Cycle:\s*([A-Za-z]+\d+)", details_norm, re.IGNORECASE)
            amt_re  = re.search(r"Amount:\s*([0-9]*\.?[0-9]+)\s*USDT", details_norm, re.IGNORECASE)

            if not (m_sym_re and nums_re and pos_re):
                if sym_guess:
                    await send_notification(f"⚠️ Could not parse manual-close details for {sym_guess}. Please re-check and resend.")
                else:
                    await send_notification("⚠️ Could not parse manual-close details. Please re-check and resend.")
                return

            symbol = m_sym_re.group(1).upper().replace('-', '').replace('/', '')
            entry_price = float(nums_re.group(1))
            tp1 = float(nums_re.group(2))
            sl_price = float(nums_re.group(3))
            track_num = pos_re.group(1)
            cycle_num = pos_re.group(2).upper()

            if amt_re:
                amount = float(amt_re.group(1))
            else:
                structure = get_trade_structure()
                cell = structure["tracks"].get(str(track_num), {}).get("cycles", {}).get(cycle_num)
                amount = float((cell or {}).get("amount", 0) or 0)

            if amount <= 0:
                await send_notification(f"⚠️ Amount missing or invalid for {symbol}. Please include 'Amount: ... USDT'.")
                return

            _pending_close_request["waiting"] = False

            asyncio.create_task(
                manual_close_monitor(symbol, entry_price, sl_price, tp1, amount, track_num, cycle_num)
            )
            await send_notification_tc(
                (
                    f"🟠 Manual close armed\n"
                    f"🎯 TP: {tp1} | 🛑 SL: {sl_price}\n"
                    f"💵 Amount: {amount} USDT"
                ),
                symbol=symbol, track_num=track_num, cycle_num=cycle_num
            )
        except Exception as e:
            _pending_close_request["waiting"] = False
            if 'symbol' in locals() and symbol:
                await send_notification_tc(f"⚠️ Manual close parse error: {e}", symbol=symbol, track_num=track_num if 'track_num' in locals() else None, cycle_num=cycle_num if 'cycle_num' in locals() else None)
            elif 'sym_guess' in locals() and sym_guess:
                await send_notification(f"⚠️ Manual close parse error for {sym_guess}: {e}")
            else:
                await send_notification(f"⚠️ Manual close parse error: {e}")
        return

    # ===== Risk command (with guard) =====
    if cmd.startswith("risk"):
        fn = globals().get("handle_risk_command")
        if callable(fn):
            await fn(text)
        else:
            await send_notification("⚠️ أمر risk غير مُفعّل حاليًا.")
        return

    # ===== New command: sell <index>  or  sell <symbol> =====
    if cmd.startswith("sell"):
        parts = text.split(maxsplit=1)
        if len(parts) < 2:
            await send_notification("⚠️ Usage: sell <index>  or  sell <symbol>\nExample: sell 6  or  sell ALGO")
            return

        arg = parts[1].strip()
        is_index = arg.isdigit()
        map_dirty = False

        # --- Lookup by index ---
        if is_index:
            idx = int(arg)
            if idx not in _STATUS_INDEX_MAP:
                try:
                    _rebuild_status_index_map()
                except Exception:
                    pass
                if idx not in _STATUS_INDEX_MAP:
                    await send_notification(f"⚠️ sell {idx}: index not found in the current list.")
                    return
            sym_norm, track_num, cycle_num = _STATUS_INDEX_MAP[idx]
            symbol_in = sym_norm
            structure = get_trade_structure()
            cell = (structure.get("tracks", {}).get(track_num, {}).get("cycles", {}) or {}).get(cycle_num)
            if not cell:
                await send_notification_tc("ℹ️ No active trade on this slot.", symbol=symbol_in, track_num=track_num, cycle_num=cycle_num)
                return
            st = (cell.get("status") or "").lower()
            if st in ("open", "reserved"):
                await send_notification_tc("🚫 Cancelled pending buy.", symbol=symbol_in, track_num=track_num, cycle_num=cycle_num)
                await update_trade_status(symbol_in, 'failed', track_num=track_num, cycle_num=cycle_num)
                structure["tracks"][track_num]["cycles"][cycle_num] = None
                save_trade_structure(structure)
                map_dirty = True
                try:
                    _rebuild_status_index_map()
                except Exception:
                    pass
                return

            if st == "buy":
                try:
                    pair = format_symbol(symbol_in)
                    meta = get_symbol_meta(pair)
                    if not meta:
                        await send_notification_tc("❌ Sell meta fetch failed.", symbol=symbol_in, track_num=track_num, cycle_num=cycle_num)
                        return
                    qty = float(cell.get("filled_qty", 0) or 0)
                    bought_price = float(cell.get("bought_price", 0) or 0)
                    base_inc = meta['baseIncrement']; min_base = meta['baseMinSize']
                    if qty <= 0 or bought_price <= 0:
                        await send_notification_tc("⚠️ Sell aborted: missing execution data (qty/price).", symbol=symbol_in, track_num=track_num, cycle_num=cycle_num)
                        return
                    adj_qty = quantize_down(qty * 0.9998, base_inc)
                    if adj_qty < min_base or adj_qty == 0.0:
                        await send_notification_tc("⚠️ Sell aborted: adjusted qty < min size.", symbol=symbol_in, track_num=track_num, cycle_num=cycle_num)
                        return

                    order = place_market_order(pair, 'sell', size=str(adj_qty), symbol_hint=symbol_in, sim_override=bool(cell.get("simulated", False)))
                    order_id = (order or {}).get("orderId")
                    if not order_id:
                        await send_notification_tc("❌ Sell error: no orderId returned.", symbol=symbol_in, track_num=track_num, cycle_num=cycle_num)
                        return
                    await asyncio.sleep(1)
                    filled_qty, deal_funds = await get_order_deal_size(order_id, symbol=symbol_in, sim_override=bool(cell.get("simulated", False)))
                    if filled_qty <= 0.0:
                        await send_notification_tc(f"❌ Sell issue: order executed but filled size = 0.\n🆔 orderId: {order_id}", symbol=symbol_in, track_num=track_num, cycle_num=cycle_num)
                        return
                    sell_price = deal_funds / filled_qty
                    pnl = (sell_price - bought_price) * filled_qty
                    pct = ((sell_price - bought_price) / max(bought_price, 1e-12)) * 100.0

                    _update_trade_exec_fields(symbol_in, track_num, cycle_num,
                                              bought_price=bought_price, sell_price=sell_price, sell_qty=filled_qty)

                    duration_str = ""
                    try:
                        st_iso = cell.get("start_time")
                        if st_iso:
                            dt = datetime.fromisoformat(st_iso)
                            if not dt.tzinfo: dt = dt.replace(tzinfo=timezone.utc)
                            delta = datetime.now(timezone.utc) - dt
                            duration_str = f"{delta.days}d / {delta.seconds // 3600}h / {(delta.seconds % 3600)//60}m"
                    except Exception:
                        pass

                    if pnl >= 0:
                        try: accumulate_summary(profit_delta=max(0.0, float(pnl)))
                        except Exception: pass
                        await send_notification_tc(
                            (
                                "🧾 Manual SELL — TP\n"
                                f"💰 Buy: {bought_price:.6f} → Sell: {sell_price:.6f}\n"
                                f"📦 Qty: {filled_qty:.6f}\n"
                                f"💵 PnL: {pnl:.4f} USDT  ({pct:+.2f}%)\n"
                                f"{('⏱️ ' + duration_str) if duration_str else ''}"
                            ),
                            symbol=symbol_in, track_num=track_num, cycle_num=cycle_num
                        )
                        await update_trade_status(symbol_in, 'closed', track_num=track_num, cycle_num=cycle_num)
                        await update_active_trades((track_num, cycle_num), {"symbol": symbol_in}, final_status="closed")
                    else:
                        try: accumulate_summary(loss_delta=max(0.0, float(-pnl)))
                        except Exception: pass
                        await send_notification_tc(
                            (
                                "🧾 Manual SELL — LOSS (drawdown)\n"
                                f"💰 Buy: {bought_price:.6f} → Sell: {sell_price:.6f}\n"
                                f"📦 Qty: {filled_qty:.6f}\n"
                                f"💵 PnL: {pnl:.4f} USDT  ({pct:+.2f}%)\n"
                                f"{('⏱️ ' + duration_str) if duration_str else ''}\n"
                                "🔁 Drawdown: slot released. Waiting for a new recommendation to reuse this slot."
                            ),
                            symbol=symbol_in, track_num=track_num, cycle_num=cycle_num
                        )
                        await update_trade_status(symbol_in, 'drwn', track_num=track_num, cycle_num=cycle_num)  # unified
                        await update_active_trades((track_num, cycle_num), {"symbol": symbol_in}, final_status="drwn")

                except Exception as e:
                    await send_notification_tc(f"❌ Sell error: {e}", symbol=symbol_in, track_num=track_num, cycle_num=cycle_num)
                map_dirty = True
            try:
                _rebuild_status_index_map()
            except Exception:
                pass
            return

        # --- Fallback: sell <symbol> ---
        symbol_in = arg.strip()
        symbol_norm = symbol_in.upper().replace('-', '').replace('/', '')
        structure, active_cells = _find_active_cells_by_symbol(symbol_norm)
        if not active_cells:
            await send_notification(f"ℹ️ لا توجد صفقات فعّالة للرمز {symbol_norm}.")
            return

        for track_num, cycle_num, cell in active_cells:
            st = (cell.get("status") or "").lower()
            if st in ("open", "reserved"):
                await send_notification_tc("🚫 Cancelled pending buy.", symbol=symbol_norm, track_num=track_num, cycle_num=cycle_num)
                await update_trade_status(symbol_norm, 'failed', track_num=track_num, cycle_num=cycle_num)
                structure["tracks"][track_num]["cycles"][cycle_num] = None
                save_trade_structure(structure)
                continue
            if st == "buy":
                try:
                    pair = format_symbol(symbol_norm)
                    meta = get_symbol_meta(pair)
                    if not meta:
                        await send_notification_tc("❌ Sell meta fetch failed.", symbol=symbol_norm, track_num=track_num, cycle_num=cycle_num)
                        continue
                    qty = float(cell.get("filled_qty", 0) or 0)
                    bought_price = float(cell.get("bought_price", 0) or 0)
                    base_inc = meta['baseIncrement']; min_base = meta['baseMinSize']
                    if qty <= 0 or bought_price <= 0:
                        await send_notification_tc("⚠️ Sell aborted: missing execution data (qty/price).", symbol=symbol_norm, track_num=track_num, cycle_num=cycle_num)
                        continue
                    adj_qty = quantize_down(qty * 0.9998, base_inc)
                    if adj_qty < min_base or adj_qty == 0.0:
                        await send_notification_tc("⚠️ Sell aborted: adjusted qty < min size.", symbol=symbol_norm, track_num=track_num, cycle_num=cycle_num)
                        continue
                    order = place_market_order(pair, 'sell', size=str(adj_qty), symbol_hint=symbol_norm, sim_override=bool(cell.get("simulated", False)))
                    order_id = (order or {}).get("orderId")
                    if not order_id:
                        await send_notification_tc("❌ Sell error: no orderId returned.", symbol=symbol_norm, track_num=track_num, cycle_num=cycle_num)
                        continue
                    await asyncio.sleep(1)
                    filled_qty, deal_funds = await get_order_deal_size(order_id, symbol=symbol_norm, sim_override=bool(cell.get("simulated", False)))
                    if filled_qty <= 0.0:
                        await send_notification_tc(f"❌ Sell issue: order executed but filled size = 0.\n🆔 orderId: {order_id}", symbol=symbol_norm, track_num=track_num, cycle_num=cycle_num)
                        continue
                    sell_price = deal_funds / filled_qty
                    pnl = (sell_price - bought_price) * filled_qty
                    pct = ((sell_price - bought_price) / max(bought_price, 1e-12)) * 100.0

                    _update_trade_exec_fields(symbol_norm, track_num, cycle_num,
                                              bought_price=bought_price, sell_price=sell_price, sell_qty=filled_qty)

                    duration_str = ""
                    try:
                        st_iso = cell.get("start_time")
                        if st_iso:
                            dt = datetime.fromisoformat(st_iso)
                            if not dt.tzinfo: dt = dt.replace(tzinfo=timezone.utc)
                            delta = datetime.now(timezone.utc) - dt
                            duration_str = f"{delta.days}d / {delta.seconds // 3600}h / {(delta.seconds % 3600)//60}m"
                    except Exception:
                        pass

                    if pnl >= 0:
                        try: accumulate_summary(profit_delta=max(0.0, float(pnl)))
                        except Exception: pass
                        await send_notification_tc(
                            (
                                f"🧾 Manual SELL — TP\n"
                                f"💰 Buy: {bought_price:.6f} → Sell: {sell_price:.6f}\n"
                                f"📦 Qty: {filled_qty:.6f}\n"
                                f"💵 PnL: {pnl:.4f} USDT  ({pct:+.2f}%)\n"
                                f"{('⏱️ ' + duration_str) if duration_str else ''}"
                            ),
                            symbol=symbol_norm, track_num=track_num, cycle_num=cycle_num
                        )
                        await update_trade_status(symbol_norm, 'closed', track_num=track_num, cycle_num=cycle_num)
                        await update_active_trades((track_num, cycle_num), {"symbol": symbol_norm}, final_status="closed")
                    else:
                        try: accumulate_summary(loss_delta=max(0.0, float(-pnl)))
                        except Exception: pass
                        await send_notification_tc(
                            (
                                f"🧾 Manual SELL — LOSS (drawdown)\n"
                                f"💰 Buy: {bought_price:.6f} → Sell: {sell_price:.6f}\n"
                                f"📦 Qty: {filled_qty:.6f}\n"
                                f"💵 PnL: {pnl:.4f} USDT  ({pct:+.2f}%)\n"
                                f"{('⏱️ ' + duration_str) if duration_str else ''}\n"
                                "🔁 Drawdown: slot released. Waiting for a new recommendation to reuse this slot."
                            ),
                            symbol=symbol_norm, track_num=track_num, cycle_num=cycle_num
                        )
                        await update_trade_status(symbol_norm, 'drwn', track_num=track_num, cycle_num=cycle_num)  # unified
                        await update_active_trades((track_num, cycle_num), {"symbol": symbol_norm}, final_status="drwn")
                except Exception as e:
                    await send_notification_tc(f"❌ Sell error: {e}", symbol=symbol_norm, track_num=track_num, cycle_num=cycle_num)
                map_dirty = True
        if map_dirty:
            try:
                _rebuild_status_index_map()
            except Exception:
                pass
        return

    # ===== Other commands =====
    if cmd.startswith("track "):
        parts = text.split()
        if len(parts) >= 2:
            try:
                tn = int(parts[1]); await show_single_track_status(tn)
            except Exception:
                await send_notification("⚠️ Usage: track <n>  (example: track 1)")
        else:
            await send_notification("⚠️ Usage: track <n>  (example: track 1)")
        return

    if cmd == "pause":
        set_bot_active(False)
        await send_notification("⏸️ Bot paused (will ignore new recommendations).")

    elif cmd == "reuse":
        set_bot_active(True)
        await send_notification("▶️ Bot resumed.")

    elif cmd == "status":
        await show_bot_status()

    elif cmd == "summary":
        await show_trade_summary()

    elif cmd == "track":
        await show_tracks_status()

    elif cmd == "clean terminal":
        await clean_terminal_notices()

    elif cmd.startswith("cycl"):
        parts = text.split()
        if len(parts) == 1:
            structure = get_trade_structure()
            await send_notification(
                f"ℹ️ Current cycle count = {int(structure.get('cycle_count', DEFAULT_CYCLE_COUNT))}\n"
                "Usage: cycl <N> (e.g., cycl 20)"
            )
        else:
            try:
                n = int(parts[1]); await apply_cycle_count(n)
            except Exception:
                await send_notification("⚠️ Usage: cycl <N>  (example: cycl 20)")
        return

    elif cmd == "help":
        await send_notification(
            "🆘 Commands:\n"
            "• gate (or off) – Show Email Gate status (open/closed via email)\n"
            "• gate open / gate close – Manually OPEN/CLOSE Email Gate now\n"
            "• pause – Pause recommendations\n"
            "• reuse – Resume recommendations\n"
            "• status – Show bot status (with numbering & timestamps; BUY shows price/Δ)\n"
            "• summary – Profit/Loss summary\n"
            "• track – Show tracks status (all)\n"
            "• track <n> – Show only track n with %\n"
            "• sell <index> – Exit/cancel by index from status (e.g., sell 6)\n"
            "• sell <symbol> – Market-exit or cancel pending (e.g., sell ALGO)\n"
            "• cycl <N> – Change cycle count (e.g., cycl 20)\n"
            "• clean terminal – Clear Terminal Notices\n"
            "• close – Manual close (sends a template)\n"
            "• cancel – Cancel manual-close request\n"
            "• risk – Market quality report\n"
            "• slots – List empty slots (within cycle limit)\n"
            "• slots all – List empty slots including out-of-range\n"
            "• nextslots – Predict all next candidate slots\n"
            "• verlauf – Full timeline of all trades\n"
            "• debug funds on/off/<N>m – Toggle detailed balance logging\n"
            "• Add <symbol> / Remove <symbol> / Status List – manage blacklist"
        )
    else:
        pass
