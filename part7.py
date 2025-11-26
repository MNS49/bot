# ============================================
# Section 7) NTP Check & Main (final, revised)
#  - Single source of truth for IMAP settings (reads from Section 1 globals)
#  - Uses unified console_echo from Section 2 (with safe alias)
#  - Unified Email Gate (set_email_gate / is_email_gate_open from Section 2)
#  - Hourly 4% drawdown aggregation + NTP skew notifier
#  - (#3): Trusted senders filter for IMAP commands
#  - (#4): Persist & restore last_seen_uid in EMAIL_STATE_FILE
#  - FIX: لا تنبيهات دروداون ولا استئناف مراقبة لصفقات نهائيّة (closed/stopped/drwn/failed)
#         بالاعتماد على TRADES_FILE كمرجع للحالة النهائية.
# ============================================

import time
import asyncio
import imaplib
import email
import re
import os
import json
from datetime import datetime, timezone
from typing import Optional, List, Dict, Any, Tuple
from email.utils import parseaddr  # for trusted senders check

# ---------- Console echo (use Section 2 global) ----------
try:
    console_echo  # provided by Section 2
except NameError:  # safe no-op fallback
    def console_echo(msg: str) -> None:
        try:
            if bool(globals().get("ENABLE_CONSOLE_ECHO", False)):
                print(msg)
        except Exception:
            pass
# keep backward compatibility
_console_echo = console_echo

# ---------- Pull required globals from Section 1 ----------
EMAIL_STATE_FILE = globals().get("EMAIL_STATE_FILE", "email_gate_state.json")
ENABLE_CONSOLE_ECHO = bool(globals().get("ENABLE_CONSOLE_ECHO", True))
client = globals().get("client")

# ---------- IMAP settings (single source of truth = Section 1) ----------
IMAP_HOST = globals().get("IMAP_HOST", os.getenv("IMAP_HOST", ""))
IMAP_USER = globals().get("IMAP_USER", os.getenv("IMAP_USER", ""))
IMAP_APP_PASSWORD = (
    globals().get("IMAP_APP_PASSWORD")
    or globals().get("IMAP_PASSWORD")
    or os.getenv("IMAP_PASSWORD", "")
)
IMAP_FOLDER = globals().get("IMAP_FOLDER", os.getenv("IMAP_FOLDER", "INBOX"))
IMAP_POLL_SEC = int(globals().get("EMAIL_GATE_POLL_SEC", os.getenv("IMAP_POLL_SEC", "30")))

# ---------- Trusted senders (from Section 1) ----------
EMAIL_TRUSTED_SENDERS = set(
    s.lower().strip() for s in globals().get("EMAIL_TRUSTED_SENDERS", set()) if s
)

# ---------- NTP (time sync) ----------
NTP_MAX_DIFF_SEC = 2.0      # KuCoin غالبًا يرفض > 2 ثواني فرق توقيت
NTP_ALERT_COOLDOWN = 3600   # تنبيه واحد كل ساعة
_last_ntp_alert_ts = 0.0

def check_system_time(max_allowed_diff_sec: float = NTP_MAX_DIFF_SEC) -> float:
    """
    قياس انحراف الوقت (ثواني). يرجّع -1 عند الفشل.
    يطبع للترمينال فقط (آمن قبل start()).
    """
    try:
        try:
            import ntplib
        except ImportError:
            print("ℹ️ ntplib غير مُثبت؛ نفّذ: pip install ntplib")
            return -1.0

        client_ntp = ntplib.NTPClient()
        diffs = []
        for _ in range(3):
            try:
                resp = client_ntp.request('pool.ntp.org', version=3, timeout=2)
                diffs.append(abs(time.time() - resp.tx_time))
            except Exception:
                pass

        if not diffs:
            print("⚠️ Unable to reach NTP.")
            return -1.0

        best = min(diffs)
        if best > max_allowed_diff_sec:
            print(f"⚠️ Large time skew: ~{best:.2f}s — may cause KuCoin signature errors.")
        else:
            print(f"✅ Time in sync (~{best:.2f}s).")
        return best

    except Exception as e:
        print(f"⚠️ NTP check failed: {e}")
        return -1.0

async def _maybe_warn_ntp_diff():
    """
    تُشغَّل بعد بدء تلغرام. ترسل تنبيهات إذا الانحراف كبير.
    """
    global _last_ntp_alert_ts
    diff = check_system_time(NTP_MAX_DIFF_SEC)
    now = time.time()

    if diff == -1.0:
        if now - _last_ntp_alert_ts > NTP_ALERT_COOLDOWN:
            _last_ntp_alert_ts = now
            await send_notification("ℹ️ NTP skew not measured (ntplib missing or no network).")
        return

    if diff > NTP_MAX_DIFF_SEC and (now - _last_ntp_alert_ts > NTP_ALERT_COOLDOWN):
        _last_ntp_alert_ts = now
        await send_notification(
            f"⚠️ System time skew is ~{diff:.2f}s. KuCoin may reject requests.\n"
            f"🔧 Use chrony (preferred) or ntpdate to sync."
        )

# ---------- Email Gate helpers (wrappers over Section 2) ----------
def set_email_trade_gate(value: bool) -> None:
    """
    Public helper used by various parts of the app when we want to change the gate.
    It persists via set_email_gate(...) from Section 2 and issues a notification.
    """
    try:
        # prefer centralized setter if available
        if 'set_email_gate' in globals() and callable(globals()['set_email_gate']):
            globals()['set_email_gate'](bool(value))
        else:
            # fallback: write directly to EMAIL_STATE_FILE (best-effort)
            try:
                s: Dict[str, Any] = {}
                if os.path.exists(EMAIL_STATE_FILE):
                    with open(EMAIL_STATE_FILE, "r") as f:
                        s = json.load(f) or {}
                s["gate_open"] = bool(value)
                with open(EMAIL_STATE_FILE, "w") as f:
                    json.dump(s, f, indent=2)
            except Exception as e:
                _console_echo(f"[GATE] failed to persist gate state: {e}")

        state = "OPEN ✅ (accepting channel recommendations)" if value else "CLOSED ⛔️ (blocking new recommendations)"
        try:
            asyncio.create_task(send_notification(f"📧 Email gate changed → {state}"))
        except Exception:
            _console_echo(f"[GATE] Email gate changed → {state}")
    except Exception as e:
        _console_echo(f"[GATE] set_email_trade_gate error: {e}")

def is_email_trade_gate_open() -> bool:
    """
    Wrapper to check gate state. Uses is_email_gate_open() from Section 2 if available.
    """
    try:
        if 'is_email_gate_open' in globals() and callable(globals()['is_email_gate_open']):
            return bool(globals()['is_email_gate_open']())
        # fallback: read EMAIL_STATE_FILE directly
        try:
            if os.path.exists(EMAIL_STATE_FILE):
                with open(EMAIL_STATE_FILE, "r") as f:
                    d = json.load(f) or {}
                return bool(d.get("gate_open", True))
        except Exception:
            pass
        return True
    except Exception:
        return True

# ---------- TRADES_FILE helpers (مرجع الحالة النهائية) ----------
_FINAL_STATES = {"closed", "stopped", "drwn", "failed"}

def _load_trades_cache() -> List[Dict[str, Any]]:
    if not os.path.exists(TRADES_FILE):
        return []
    try:
        with open(TRADES_FILE, 'r') as f:
            tdata = json.load(f) or {}
        return tdata.get("trades", []) or []
    except Exception:
        return []

def _latest_trade_for(trades: List[Dict[str, Any]], sym_up: str, track_num: str, cycle_code: str) -> Optional[Dict[str, Any]]:
    latest = None; latest_ts = -1.0
    for tr in trades:
        try:
            if (tr.get("symbol") or "").upper().replace('-', '').replace('/', '') != sym_up: continue
            if str(tr.get("track_num")) != str(track_num): continue
            if str(tr.get("cycle_num")) != str(cycle_code): continue
            ts = float(tr.get("opened_at", 0) or 0)
            if ts >= latest_ts:
                latest_ts = ts
                latest = tr
        except Exception:
            continue
    return latest

def _latest_state_for(trades: List[Dict[str, Any]], sym_up: str, track_num: str, cycle_code: str) -> Optional[str]:
    tr = _latest_trade_for(trades, sym_up, track_num, cycle_code)
    return (tr.get("status") or "").lower() if tr else None

def _is_final_in_trades(trades: List[Dict[str, Any]], sym_up: str, track_num: str, cycle_code: str) -> bool:
    st = _latest_state_for(trades, sym_up, track_num, cycle_code)
    return (st in _FINAL_STATES) if st else False

# ---------- IMAP parsing helpers ----------
def _imap_email_text_from_msg(msg: email.message.Message) -> str:
    """
    استخراج النص/العنوان من الرسالة.
    """
    subject = email.header.make_header(email.header.decode_header(msg.get('Subject', '') or ''))
    subject_str = str(subject)
    body_parts: List[str] = []
    if msg.is_multipart():
        for part in msg.walk():
            ctype = (part.get_content_type() or "").lower()
            cdisp = (part.get("Content-Disposition") or "").lower()
            if ctype in ("text/plain", "text/html") and "attachment" not in cdisp:
                try:
                    body_parts.append(
                        part.get_payload(decode=True).decode(
                            part.get_content_charset() or "utf-8", errors="ignore"
                        )
                    )
                except Exception:
                    try:
                        body_parts.append(part.get_payload(decode=True).decode("utf-8", errors="ignore"))
                    except Exception:
                        pass
    else:
        try:
            body_parts.append(msg.get_payload(decode=True).decode(msg.get_content_charset() or "utf-8", errors="ignore"))
        except Exception:
            try:
                body_parts.append(msg.get_payload(decode=True).decode("utf-8", errors="ignore"))
            except Exception:
                pass
    body_joined = "\n".join(body_parts)
    full_text = f"{subject_str}\n\n{body_joined}"
    return full_text

def _email_says_enable(text: str) -> bool:
    """يفتح البوابة عند وجود صيغة 'buy crypto' (غير حساسة لحالة الأحرف)."""
    t = (text or "").lower()
    return bool(re.search(r"\bbuy\s+crypto\b", t))

def _email_says_disable(text: str) -> bool:
    """يغلق البوابة عند وجود صيغة 'sell crypto' (مع دعم الخطأ الإملائي الشائع فقط)."""
    t = (text or "").lower()
    return bool(re.search(r"\bsell\s+crypto\b", t)) or bool(re.search(r"\bsell\s+cryrpto\b", t))

async def _imap_connect_and_select() -> Optional[imaplib.IMAP4_SSL]:
    """
    إنشاء اتصال IMAP وتحديد المجلد.
    """
    if not IMAP_HOST or not IMAP_USER or not IMAP_APP_PASSWORD:
        _console_echo("[IMAP] Missing IMAP configs; watcher disabled.")
        return None
    try:
        M = imaplib.IMAP4_SSL(IMAP_HOST)
        M.login(IMAP_USER, IMAP_APP_PASSWORD)
        typ, _ = M.select(IMAP_FOLDER, readonly=True)
        if typ != "OK":
            try:
                M.logout()
            except Exception:
                pass
            _console_echo(f"[IMAP] Failed to select folder: {IMAP_FOLDER}")
            return None
        return M
    except Exception as e:
        _console_echo(f"[IMAP] connect/select error: {e}")
        return None

# ---------- IMAP watcher (with #3 trusted senders + #4 persist last_uid) ----------
async def imap_control_watcher():
    """
    يراقب الإيميل دوريًا:
      - إذا وصلت رسالة تحتوي "buy crypto" → فتح البوابة (يستقبل التوصيات).
      - إذا وصلت رسالة تحتوي "sell crypto" → غلق البوابة (يمنع فتح صفقات جديدة).
    لا يؤثر على إدارة الصفقات المفتوحة (تُدار مستقلًا).
    """
    _console_echo("[IMAP] watcher starting…")

    # (#4) Restore last_seen_uid from EMAIL_STATE_FILE for persistence
    last_seen_uid: Optional[int] = None
    try:
        if os.path.exists(EMAIL_STATE_FILE):
            with open(EMAIL_STATE_FILE, "r") as f:
                s = json.load(f) or {}
            lu = s.get("last_uid")
            if lu is not None:
                last_seen_uid = int(lu)
    except Exception as e:
        _console_echo(f"[IMAP] could not restore last_uid: {e}")
        last_seen_uid = None

    while True:
        M = await _imap_connect_and_select()
        if M is None:
            await asyncio.sleep(max(60, IMAP_POLL_SEC))
            continue
        try:
            typ, data = M.search(None, "ALL")
            if typ != "OK" or not data or not data[0]:
                await asyncio.sleep(IMAP_POLL_SEC)
                try: M.logout()
                except Exception: pass
                continue
            ids = data[0].split()
            recent_ids = ids[-50:] if len(ids) > 50 else ids  # scan up to last 50

            updated_uid = last_seen_uid
            for msg_id in reversed(recent_ids):  # الأحدث أولًا
                # fetch UID
                typ, uid_data = M.fetch(msg_id, "(UID)")
                if typ != "OK" or not uid_data:
                    continue
                uid_line = uid_data[0][0].decode(errors="ignore") if isinstance(uid_data[0], tuple) else str(uid_data[0])
                m_uid = re.search(r"UID\s+(\d+)", uid_line)
                uid_val = int(m_uid.group(1)) if m_uid else None
                if last_seen_uid is not None and uid_val is not None and uid_val <= last_seen_uid:
                    continue  # قديم

                # fetch full message
                typ, msg_data = M.fetch(msg_id, "(RFC822)")
                if typ != "OK" or not msg_data or not isinstance(msg_data[0], tuple):
                    continue
                try:
                    raw = msg_data[0][1]
                    em = email.message_from_bytes(raw)
                    full_text = _imap_email_text_from_msg(em)
                except Exception:
                    continue

                # (#3) Trusted senders filter
                try:
                    sender = (em.get('From') or '')
                    _, sender_email = parseaddr(sender)
                    if EMAIL_TRUSTED_SENDERS and (sender_email or "").lower() not in EMAIL_TRUSTED_SENDERS:
                        _console_echo(f"[IMAP] ignoring untrusted sender: {sender_email}")
                        if uid_val is not None:
                            updated_uid = max(updated_uid or 0, uid_val)
                        continue
                except Exception as e:
                    _console_echo(f"[IMAP] trusted-sender check error: {e}")

                try:
                    if _email_says_enable(full_text):
                        if 'set_email_gate' in globals() and callable(globals()['set_email_gate']):
                            globals()['set_email_gate'](True)
                        else:
                            set_email_trade_gate(True)
                        _console_echo("[IMAP] set gate OPEN (via email)")
                    elif _email_says_disable(full_text):
                        if 'set_email_gate' in globals() and callable(globals()['set_email_gate']):
                            globals()['set_email_gate'](False)
                        else:
                            set_email_trade_gate(False)
                        _console_echo("[IMAP] set gate CLOSED (via email)")
                except Exception as e:
                    _console_echo(f"[IMAP] command parse error: {e}")

                if uid_val is not None:
                    updated_uid = max(updated_uid or 0, uid_val)

            # Persist last_seen_uid + current gate state (#4)
            try:
                state_obj = {
                    "last_uid": int(updated_uid or 0),
                    "gate_open": bool(is_email_trade_gate_open()),
                }
                with open(EMAIL_STATE_FILE, "w") as f:
                    json.dump(state_obj, f, indent=2)
                last_seen_uid = updated_uid
            except Exception as e:
                _console_echo(f"[IMAP] persist state error: {e}")

            await asyncio.sleep(IMAP_POLL_SEC)
        except Exception as e:
            _console_echo(f"[IMAP] loop error: {e}")
            await asyncio.sleep(max(30, IMAP_POLL_SEC))
        finally:
            try:
                M.logout()
            except Exception:
                pass

# ---------- Hourly 4% drawdown aggregation ----------
async def _hourly_drawdown_check_and_notify():
    """
    كل ساعة: يفحص جميع الخلايا بحالة Buy ويجمع العملات التي هبطت >= 4% منذ سعر الشراء الفعلي.
    يرسل رسالة واحدة تحتوي جميع العملات المتأثرة للحسابين.
    (FIX) يتأكد من أن الصفقة ليست منتهية نهائيًا في TRADES_FILE قبل إصدار تنبيه.
    """
    try:
        # أعِد بناء خريطة الترقيم كي يكون الرقم مطابقًا لآخر ترتيب status (إن وُجدت الدالة)
        try:
            if '_rebuild_status_index_map' in globals() and callable(globals()['_rebuild_status_index_map']):
                globals()['_rebuild_status_index_map']()
        except Exception:
            pass

        structure = get_trade_structure()
        trades = _load_trades_cache()
        affected_lines: List[str] = []

        for tnum, tdata in (structure.get("tracks") or {}).items():
            for cname, cell in (tdata.get("cycles") or {}).items():
                if not cell:
                    continue
                if (cell.get("status") or "").lower() != "buy":
                    continue

                sym = (cell.get("symbol") or "").upper().replace('-', '').replace('/', '')
                bought_price = float(cell.get("bought_price", 0) or 0)
                if not sym or bought_price <= 0:
                    continue

                # FIX: لا تنبيه إذا كانت الحالة النهائية في TRADES_FILE
                if _is_final_in_trades(trades, sym, str(tnum), str(cname)):
                    continue

                price = await fetch_current_price(sym)
                if price is None or price <= 0:
                    continue

                drop_pct = ((bought_price - price) / max(bought_price, 1e-12)) * 100.0
                if drop_pct >= 4.0:
                    try:
                        idx = globals().get('_STATUS_REV_INDEX_MAP', {}).get((sym, str(tnum), str(cname)))
                    except Exception:
                        idx = None
                    idx_prefix = (f"{idx} " if idx is not None else "")
                    affected_lines.append(
                        f"•  {idx_prefix}{sym} — Track {tnum} | Cycle {cname} | "
                        f"Buy {bought_price:.6f} → Now {price:.6f}  (−{drop_pct:.2f}%)"
                    )

        if affected_lines:
            msg = "📉 Hourly drawdown alert (≥ 4%):\n" + "\n".join(sorted(affected_lines))
            # أرسل للحسابين (مع تحمّل أي فشل في المسار الثاني)
            if 'send_notification_both' in globals():
                await send_notification_both(msg)
            else:
                await send_notification(msg, to_telegram=True)
                if 'send_to_second_account' in globals():
                    try:
                        await send_to_second_account(msg)
                    except Exception:
                        pass

    except Exception as e:
        print(f"⚠️ hourly drawdown aggregation error: {e}")

async def status_notifier():
    """
    مُنبّه كل ساعة:
      - فحص NTP.
      - تجميع تنبيه الهبوط 4%+ لكل المراكز المشتراة برسالة واحدة.
    """
    while True:
        try:
            await _maybe_warn_ntp_diff()
            await _hourly_drawdown_check_and_notify()
            await asyncio.sleep(3600)
        except Exception as e:
            print(f"⚠️ status_notifier error: {e}")
            await asyncio.sleep(300)

# ---------- Resume open trades on startup ----------
async def resume_open_trades():
    """
    عند تشغيل البوت:
      - الخلايا بحالة open → يعيد تشغيل monitor_and_execute (ينتظر الشراء ثم البيع).
      - الخلايا بحالة buy  → يراقب فقط TP/SL عبر manual_close_monitor.
    كما يرسل تلخيصًا بعد الانتهاء بعدد المهام التي تم استئنافها.
    (FIX) لا يستأنف إذا كانت الصفقة نهائية في TRADES_FILE، ويُنظّف الخانة إذا كانت ما زالت معلّمة open/buy.
    """
    open_resumed = 0
    buy_resumed = 0
    cleaned_slots: List[Tuple[str, str, str]] = []  # (sym, track, cycle)

    structure = get_trade_structure()
    trades = _load_trades_cache()

    # سنُجري أي تنظيف ضروري ثم نحفظ مرّة واحدة في النهاية
    dirty = False

    for tnum, tdata in structure["tracks"].items():
        for cname, cell in (tdata.get("cycles") or {}).items():
            if not cell:
                continue
            try:
                symbol = (cell.get("symbol") or "").upper()
                entry = float(cell.get("entry", 0) or 0)
                sl = float(cell.get("sl", 0) or 0)
                targets = cell.get("targets") or []
                amount = float(cell.get("amount", 0) or 0)
                status = (cell.get("status") or "").lower()
                sym_norm = symbol.upper().replace('-', '').replace('/', '')

                # FIX: تخطّي واستبعاد أي خانة نهائية حسب TRADES_FILE
                if _is_final_in_trades(trades, sym_norm, str(tnum), str(cname)):
                    if status in ("open", "buy", "reserved"):
                        # حرّر الخانة لأن صفقتها أصبحت نهائية
                        structure["tracks"][tnum]["cycles"][cname] = None
                        dirty = True
                        cleaned_slots.append((sym_norm, str(tnum), str(cname)))
                    continue

                if status == "open" and symbol and targets:
                    asyncio.create_task(
                        monitor_and_execute(symbol, entry, sl, targets, amount, str(tnum), cname)
                    )
                    open_resumed += 1
                elif status == "buy" and symbol and targets:
                    asyncio.create_task(
                        manual_close_monitor(symbol, entry, sl, targets[0], amount, str(tnum), cname)
                    )
                    buy_resumed += 1
            except Exception as e:
                sym_dbg = cell.get("symbol") if isinstance(cell, dict) else None
                if sym_dbg:
                    print(f"resume error on {tnum}-{cname} for {sym_dbg}: {e}")
                else:
                    print(f"resume error on {tnum}-{cname}: {e}")

    if dirty:
        try:
            save_trade_structure(structure)
        except Exception as e:
            print(f"⚠️ resume cleanup save error: {e}")

    # تلخيص الاستئناف (إشعار واحد)
    if open_resumed or buy_resumed or cleaned_slots:
        lines = [
            "🔄 Resume summary:",
            f"• Open monitors restarted: {open_resumed}",
            f"• Buy monitors restarted: {buy_resumed}",
        ]
        if cleaned_slots:
            preview = "\n".join(f"   - {s} — T {t} | C {c}" for s,t,c in cleaned_slots[:12])
            more = " …" if len(cleaned_slots) > 12 else ""
            lines.append("• Cleaned finalized slots (freed):")
            lines.append(preview + more)
        await send_notification("\n".join(lines))

# ---------- Entrypoint ----------
async def main():
    # اربط مستمع القناة قبل البدء
    try:
        if 'attach_channel_handler' in globals() and callable(globals()['attach_channel_handler']):
            globals()['attach_channel_handler']()
    except Exception:
        _console_echo("[MAIN] attach_channel_handler failed or missing; continuing.")

    # ابدأ تلغرام أولاً (لتجنّب فشل الإشعارات)
    if client is None:
        raise RuntimeError("Telegram client (client) is not initialized. Ensure Section 1 is loaded first.")
    await client.start()

    # وسم وضع التشغيل (محاكاة/حقيقي)
    try:
        mode_label = "Simulation" if (is_simulation() if 'is_simulation' in globals() else False) else "Live"
    except Exception:
        mode_label = "Live"

    # رسالة ترحيب تُرسل إلى الحسابين + حالة بوابة الإيميل
    gate_state = "OPEN ✅ (accepting channel recommendations)" if is_email_trade_gate_open() else "CLOSED ⛔️ (blocking new recommendations)"
    start_msg = f"✅ Bot started! ({mode_label})\n📡 Waiting for recommendations…\n📧 Email gate: {gate_state}"
    if 'send_notification_both' in globals():
        await send_notification_both(start_msg)
    else:
        await send_notification(start_msg, to_telegram=True)

    # فحص NTP الأول بعد تشغيل تلغرام
    await _maybe_warn_ntp_diff()

    # منبّه الحالة (NTP + تجميع هبوط 4% كل ساعة)
    asyncio.create_task(status_notifier())

    # مراقِب الإيميل (فتح/غلق بوابة استقبال توصيات القناة)
    asyncio.create_task(imap_control_watcher())

    # استئناف الصفقات غير المُقفلة (مع تلخيص + تنظيف الخانات النهائية)
    await resume_open_trades()

    # تشغيل حتى الانفصال
    await client.run_until_disconnected()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("🛑 Bot stopped manually.")
