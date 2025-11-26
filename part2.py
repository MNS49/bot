# ============================================
# Section 2) Utilities: formatting, rounding,
#              notifications, KuCoin helpers
#   (Updated with unified Email Gate checker
#    + dynamic blacklist + _update_trade_exec_fields)
# ============================================

import os
import re
import json
import time
import asyncio
from datetime import datetime, timezone
from typing import Optional, Tuple, Dict, Any, List
from decimal import Decimal

# ==== Imports/vars expected from Section 1 ====
# - client (Telethon TelegramClient)
# - kucoin (_KucoinAdapter)
# - files: TERMINAL_LOG_FILE, TRADES_FILE, TRACK_FILE, SUMMARY_FILE, STATE_FILE, EMAIL_STATE_FILE, BLACKLIST_FILE
# - SIMULATION_MODE, ENABLE_CONSOLE_ECHO
# - KUCOIN_* config
# - helpers from Section 1: get_trade_structure(), create_new_track(), track_base_amount(), etc.

# ---- Unified Email Gate interface (new) ----
def _email_gate_allows() -> bool:
    """
    واجهة موحّدة للاستعلام عن سماح بوابة البريد باستقبال توصيات جديدة.
    تعتمد على:
      - حالة البوت العامة (is_bot_active)
      - حالة بوابة الايميل المخزنة في EMAIL_STATE_FILE (is_email_gate_open)
    تُعيد True إذا يجب قبول توصية جديدة الآن.
    FAIL-SAFE: في حال حدث خطأ أثناء الفحص، تُعيد True (حتى لا تحجب العملية عن غير عمد).
    إذا رغبت بسلوك fail-closed غيّر النهاية إلى `return False`.
    """
    try:
        # إن وُجدت should_accept_recommendations استخدمها (تضم is_bot_active + is_email_gate_open عادة)
        if 'should_accept_recommendations' in globals() and callable(should_accept_recommendations):
            return bool(should_accept_recommendations())

        bot = True
        gate = True
        if 'is_bot_active' in globals() and callable(is_bot_active):
            bot = bool(is_bot_active())
        if 'is_email_gate_open' in globals() and callable(is_email_gate_open):
            gate = bool(is_email_gate_open())
        return bool(bot and gate)
    except Exception:
        # Fail-safe permissive (change to False to be conservative)
        return True

# ---- Rounding helpers (Decimal-safe) ----
DEFAULT_TRUNCATE_STEPS: List[float] = [
    1, 0.1, 0.01, 0.001, 0.0001,
    0.00001, 0.000001, 0.0000001, 0.00000001
]

def quantize_down(value: float, step: float) -> float:
    """
    قصّ القيمة للأسفل حسب خطوة محددة باستخدام Decimal (أدق من float %).
    """
    try:
        v = Decimal(str(value))
        s = Decimal(str(step))
        return float((v // s) * s)
    except Exception:
        return 0.0

def truncate_to_step(value: float, step: float) -> float:
    """توافقًا مع الكود القديم؛ تستخدم quantize_down داخليًا."""
    return quantize_down(value, step)

def smart_truncate(value: float, steps_list: Optional[List[float]] = None) -> float:
    """جرّب عدّة خطوات لغاية الحصول على قيمة موجبة بعد القص."""
    steps = steps_list or DEFAULT_TRUNCATE_STEPS
    for step in steps:
        try:
            truncated = truncate_to_step(value, step)
            if truncated > 0:
                return truncated
        except Exception:
            continue
    return 0.0

# ---- Symbol helpers ----
def normalize_symbol(sym: str) -> str:
    """ALGO-USDT / ALGOUSDT → ALGOUSDT (uppercase, no separators)."""
    return (sym or "").upper().replace("-", "").replace("/", "")

def format_symbol(symbol: str) -> str:
    """
    تحويل ALGOUSDT -> ALGO-USDT حسب متطلبات KuCoin REST.
    """
    s = normalize_symbol(symbol)
    return re.sub(r'(USDT|BTC|ETH|BUSD)$', r'-\1', s)

# =====================================================================
# Dynamic Blacklist (persisted) + static hard-blocked symbols (legacy)
# =====================================================================

# الثوابت القديمة (يُطبَّق فوقها البلاك ليست الديناميكية)
BLOCKED_SYMBOLS = {"MLN", "FARM"}

def _ensure_blacklist_file() -> None:
    """تهيئة ملف البلاك ليست إن كان مفقودًا."""
    try:
        if 'BLACKLIST_FILE' not in globals():
            # احتياط: في حال لم يعرّف بالقسم 1
            globals()['BLACKLIST_FILE'] = "blacklist.json"
        if not os.path.exists(BLACKLIST_FILE):
            with open(BLACKLIST_FILE, "w") as f:
                json.dump({"symbols": []}, f, indent=2)
    except Exception:
        pass

def _read_blacklist_raw() -> Dict[str, Any]:
    """قراءة الملف كما هو (قاموسي)."""
    _ensure_blacklist_file()
    try:
        with open(BLACKLIST_FILE, "r") as f:
            data = json.load(f) or {}
        if not isinstance(data, dict):
            return {"symbols": []}
        return data
    except Exception:
        return {"symbols": []}

def read_blacklist() -> List[str]:
    """قائمة الرموز (Upper/no-sep) من الملف."""
    data = _read_blacklist_raw()
    out: List[str] = []
    for s in (data.get("symbols") or []):
        try:
            out.append(normalize_symbol(str(s)))
        except Exception:
            continue
    # أعد فريدة
    dedup = sorted(set(out))
    return dedup

def write_blacklist(symbols: List[str]) -> None:
    """كتابة قائمة الرموز (تُخزّن كما هي Upper/no-sep)."""
    try:
        uniq = sorted(set(normalize_symbol(s) for s in symbols if s))
        with open(BLACKLIST_FILE, "w") as f:
            json.dump({"symbols": uniq}, f, indent=2)
    except Exception as e:
        print(f"⚠️ write_blacklist error: {e}")

def add_to_blacklist(symbol: str) -> bool:
    """إضافة رمز. ترجع True إذا تمت الإضافة، False إذا كان موجودًا مسبقًا."""
    try:
        sym = normalize_symbol(symbol)
        bl = set(read_blacklist())
        if sym in bl:
            return False
        bl.add(sym)
        write_blacklist(sorted(bl))
        return True
    except Exception as e:
        print(f"⚠️ add_to_blacklist error: {e}")
        return False

def remove_from_blacklist(symbol: str) -> bool:
    """إزالة رمز. ترجع True إذا تمت الإزالة، False إذا لم يكن موجودًا."""
    try:
        sym = normalize_symbol(symbol)
        bl = set(read_blacklist())
        if sym not in bl:
            return False
        bl.remove(sym)
        write_blacklist(sorted(bl))
        return True
    except Exception as e:
        print(f"⚠️ remove_from_blacklist error: {e}")
        return False

def list_blacklist() -> List[str]:
    """إرجاع قائمة الرموز المحجوبة حاليًا (مصنّفة تصاعديًا)."""
    try:
        return read_blacklist()
    except Exception:
        return []

def _is_blocked_symbol(sym: str) -> bool:
    """
    يعيد True إذا كان الرمز من ضمن القائمة غير المدعومة (ثابتة) أو موجود في البلاك ليست الديناميكية.
    """
    n = normalize_symbol(sym)
    try:
        if n in BLOCKED_SYMBOLS:
            return True
        return n in set(read_blacklist())
    except Exception:
        # على أي خطأ، اكتفِ بالثوابت القديمة
        return n in BLOCKED_SYMBOLS

# ==========================
# Simulation helpers (global)
# ==========================
def is_simulation() -> bool:
    """
    يرجّع حالة المحاكاة من المتغيّر العام SIMULATION_MODE (من القسم الأول).
    """
    try:
        return bool(SIMULATION_MODE)
    except NameError:
        return False

# مخزن أوامر وهمي عند التفعيل
_SIM_ORDERS: Dict[str, Dict[str, Any]] = {}  # orderId -> info

def _pair_to_symbol_no_sep(pair: str) -> str:
    # 'ALGO-USDT' -> 'ALGOUSDT'
    return normalize_symbol(pair)

def _now_ms() -> int:
    return int(time.time() * 1000)

def _effective_simulation(sim_override: Optional[bool] = None) -> bool:
    """
    يحدّد نمط التنفيذ الفعّال:
      - إذا تم تمرير sim_override → يُستخدم مباشرة.
      - غير ذلك → نعتمد is_simulation() العامة.
    """
    try:
        return bool(sim_override) if sim_override is not None else bool(is_simulation())
    except Exception:
        return False

def place_market_order(
    pair: str,
    side: str,
    *,
    funds: Optional[str] = None,
    size: Optional[str] = None,
    symbol_hint: Optional[str] = None,
    sim_override: Optional[bool] = None
) -> Dict[str, Any]:
    """
    واجهة موحّدة لوضع أمر ماركت.
    - في الوضع الحقيقي: يستدعي kucoin.create_market_order.
    - في وضع المحاكاة: يُنشئ orderId وهمي ويُسجّل البيانات في _SIM_ORDERS (يتم احتساب التعبئة في get_order_deal_size).
    - sim_override: لو مُمرّر، يفرض نمط التنفيذ (SIM/LIVE) بغضّ النظر عن SIMULATION_MODE العام.
    """
    if not _effective_simulation(sim_override):
        # استدعاء حقيقي
        return kucoin.create_market_order(
            pair, side, **({k: v for k, v in {"funds": funds, "size": size}.items() if v is not None})
        )
    # وضع المحاكاة
    oid = f"SIM-{_now_ms()}-{side.upper()}"
    _SIM_ORDERS[oid] = {
        "orderId": oid,
        "pair": pair,
        "symbol": (symbol_hint or _pair_to_symbol_no_sep(pair)),
        "side": side.lower(),
        "funds": float(funds) if funds is not None else None,
        "size": float(size) if size is not None else None,
        "created_at": time.time(),
        # سيتم ملؤها عند الاستعلام
        "filled": False,
        "dealFunds": None,
        "dealSize": None,
    }
    return {"orderId": oid}

def get_trade_balance_usdt(sim_override: Optional[bool] = None) -> float:
    """
    ارجع رصيد USDT في حساب التداول:
      - محاكاة: قيمة كبيرة ثابتة للسماح بالتنفيذ (يمكن تعديلها حسب الحاجة).
      - حقيقي: يقرأ من kucoin.get_accounts().
    sim_override: لو مُمرّر، يفرض نمط التنفيذ (SIM/LIVE).
    """
    if _effective_simulation(sim_override):
        return 1_000_000.0
    try:
        accts = kucoin.get_accounts()
        bal = 0.0
        for a in accts:
            if a.get('currency') == 'USDT' and a.get('type', '').lower() == 'trade':
                bal += float(a.get('available', 0) or 0)
        return bal
    except Exception as e:
        print(f"balance fetch error: {e}")
        return 0.0

# ==========================
# Email Gate (IMAP toggle) helpers
# ==========================
def _read_email_gate_state() -> Dict[str, Any]:
    """
    الشكل المتوقع للملف:
      {
        "last_uid": <int>,
        "last_msgid": "<str>",
        "gate_open": <bool>   # True = استقبل التوصيات / False = إيقاف استقبال
      }
    عدم وجود المفتاح gate_open يعني True افتراضيًا (السماح).
    """
    try:
        if os.path.exists(EMAIL_STATE_FILE):
            with open(EMAIL_STATE_FILE, "r") as f:
                data = json.load(f) or {}
        else:
            data = {}
    except Exception:
        data = {}
    if "gate_open" not in data:
        data["gate_open"] = True
    return data

def _write_email_gate_state(data: Dict[str, Any]) -> None:
    try:
        with open(EMAIL_STATE_FILE, "w") as f:
            json.dump(data, f, indent=2)
    except Exception as e:
        print(f"email gate write error: {e}")

def is_email_gate_open() -> bool:
    """True => استقبال التوصيات من القناة مسموح؛ False => موقوف."""
    try:
        return bool(_read_email_gate_state().get("gate_open", True))
    except Exception:
        return True

def set_email_gate(open_flag: bool) -> None:
    """تبديل حالة البوابة (يُستخدم من مراقِب الإيميل في Section 7)."""
    data = _read_email_gate_state()
    data["gate_open"] = bool(open_flag)
    _write_email_gate_state(data)

def should_accept_recommendations() -> bool:
    """
    يعتمد على:
      - حالة تشغيل البوت العامة (STATE_FILE → is_bot_active).
      - حالة بوابة الإيميل (Email Gate).
    """
    try:
        return is_bot_active() and is_email_gate_open()
    except Exception:
        return True

# ⚠️ توافق مع الأقسام القديمة التي كانت تعتمد "نافذة OFF" (تم إلغاؤها):
# نجعلها دائمًا False (لا يوجد OFF)، حتى لا تُفعّل محاكاة بسببها.
def _is_off_window_now() -> bool:
    return False

# ---- Notifications (Telegram + terminal aggregation) ----
def console_echo(msg: str) -> None:
    """Mirror to terminal when ENABLE_CONSOLE_ECHO=True (set in Section 1)."""
    try:
        if 'ENABLE_CONSOLE_ECHO' in globals() and ENABLE_CONSOLE_ECHO:
            try:
                print(msg)
            except Exception:
                pass
    except Exception:
        pass

# إبقاء الاسم القديم متاحًا للتوافق مع بقية الأقسام
_console_echo = console_echo

def tc_tag(track_num: Optional[str], cycle_num: Optional[str], style: str = "short") -> str:
    """
    صيغة موحّدة لعرض المسار/الدورة في الإشعارات:
      style="short" → 'T 1 | C AK1'
      style="long"  → 'Track 1 | Cycle AK1'
    """
    if not track_num and not cycle_num:
        return ""
    t = (str(track_num).strip() if track_num is not None else "?")
    c = (str(cycle_num).strip() if cycle_num is not None else "?")
    if style == "long":
        return f"Track {t} | Cycle {c}"
    return f"T {t} | C {c}"

def compose_msg(
    base: str,
    *,
    symbol: Optional[str] = None,
    track_num: Optional[str] = None,
    cycle_num: Optional[str] = None,
    style: str = "short",
    prefix: Optional[str] = None,
    suffix: Optional[str] = None
) -> str:
    """
    يبني رسالة موحّدة تتضمّن الرمز + T/C tag لتحسين الوضوح.
    مثال ناتج:
      "❌ manual_close trailing sell failed for ZECUSDT: ... — T 6 | C A6"
    """
    parts = []
    if prefix:
        parts.append(prefix.strip())
    core = base.strip()
    if symbol:
        sym = normalize_symbol(symbol)
        # إن لم يذكر النص الرمز صراحة، نُلحقه بصيغة ثابتة
        if sym not in core.upper():
            core = f"{core} for {sym}"
    parts.append(core)
    tc = tc_tag(track_num, cycle_num, style=style)
    if tc:
        parts.append(f"— {tc}")
    if suffix:
        parts.append(suffix.strip())
    return " ".join(p for p in parts if p)

async def send_notification(message: str, to_telegram: bool = True, tag: Optional[str] = None):
    """
    - لما to_telegram=True: إرسال إلى 'Saved Messages'.
    - لما False: تجميع برسائل التيرمينال مع عدّاد.
    ملاحظة: إذا تم الاستدعاء قبل client.start() رح تفشل Telethon؛ استخدم to_telegram=False قبل البدء.
    """
    if not to_telegram:
        log_terminal_notification(message, tag)
        console_echo(message)
        return
    try:
        await client.send_message('me', message)
        console_echo(message)
    except Exception as e:
        # تجنّب التعطّل بسبب توقيت الاتصال
        print(f"❌ Notification error: {e}")
        # سجّل داخليًا
        log_terminal_notification(f"notif_send_error: {message}", tag or "notif_send_error")

async def send_notification_tc(
    base_message: str,
    *,
    symbol: Optional[str] = None,
    track_num: Optional[str] = None,
    cycle_num: Optional[str] = None,
    style: str = "short",
    to_telegram: bool = True,
    tag: Optional[str] = None,
    prefix: Optional[str] = None,
    suffix: Optional[str] = None
):
    """
    واجهة مريحة لإرسال إشعار مع تضمين الرمز + T/C تلقائيًا.
    """
    msg = compose_msg(
        base_message,
        symbol=symbol,
        track_num=track_num,
        cycle_num=cycle_num,
        style=style,
        prefix=prefix,
        suffix=suffix,
    )
    await send_notification(msg, to_telegram=to_telegram, tag=tag)

def log_terminal_notification(message: str, tag: Optional[str] = None):
    """
    تجميع رسائل التيرمينال مع عدّاد.
    """
    tag = tag or message
    try:
        with open(TERMINAL_LOG_FILE, 'r') as f:
            log_data = json.load(f)
    except Exception:
        log_data = {}

    if tag not in log_data:
        log_data[tag] = {"count": 1, "last": datetime.now(timezone.utc).isoformat()}
    else:
        log_data[tag]["count"] += 1
        log_data[tag]["last"] = datetime.now(timezone.utc).isoformat()

    with open(TERMINAL_LOG_FILE, 'w') as f:
        json.dump(log_data, f, indent=2)

# ---- Send to second Telegram account (by username or ID) ----
async def send_to_second_account(message: str):
    """
    يرسل نفس التنبيه إلى الحساب الثاني.
    - إذا SECOND_TELEGRAM_ID > 0 سيُستخدم الـID مباشرة.
    - غير ذلك سنستخدم SECOND_TELEGRAM_USERNAME (نضيف @ عند الحاجة).
    في حال الفشل، يتم تسجيل الخطأ في terminal log دون رفع استثناء.
    """
    try:
        if isinstance(SECOND_TELEGRAM_ID, int) and SECOND_TELEGRAM_ID > 0:
            target = SECOND_TELEGRAM_ID
        else:
            uname = (SECOND_TELEGRAM_USERNAME or "").strip()
            if not uname:
                log_terminal_notification("second_account_not_configured", "second_account_not_configured")
                return
            if not uname.startswith("@"):
                uname = "@" + uname
            target = uname

        await client.send_message(target, message)
        console_echo(f"[2nd] {message}")
    except Exception as e:
        print(f"❌ Second account notification error: {e}")
        log_terminal_notification(f"second_notif_error: {message}", tag="second_notif_error")

# ---- Convenience: notify me + second account ----
async def send_notification_both(message: str):
    """
    يرسل الإشعار إلى الرسائل المحفوظة، ثم يحاول إرساله للحساب الثاني.
    أي فشل في أحد المسارين لا يوقف الآخر.
    """
    try:
        await send_notification(message, to_telegram=True)
    finally:
        try:
            await send_to_second_account(message)
        except Exception:
            pass

# ---- Debug funds toggles (verbose balance logging) ----
_DEBUG_FUNDS_UNTIL = 0.0  # 0 = off, inf = no expiry, timestamp = expiry

def enable_debug_funds(minutes: int = 0) -> None:
    """
    فعّل وضع debug funds.
    minutes=0 → بدون انتهاء (يبقى شغال لحد ما تطفّيه).
    minutes>0 → ينتهي تلقائيًا بعد N دقيقة.
    """
    global _DEBUG_FUNDS_UNTIL
    if minutes and minutes > 0:
        _DEBUG_FUNDS_UNTIL = time.time() + (minutes * 60.0)
    else:
        _DEBUG_FUNDS_UNTIL = float("inf")

def disable_debug_funds() -> None:
    """إيقاف وضع debug funds."""
    global _DEBUG_FUNDS_UNTIL
    _DEBUG_FUNDS_UNTIL = 0.0

def is_debug_funds() -> bool:
    """هل وضع debug funds مفعّل ولم ينتهِ؟"""
    return bool(_DEBUG_FUNDS_UNTIL) and (
        _DEBUG_FUNDS_UNTIL == float("inf") or time.time() <= _DEBUG_FUNDS_UNTIL
    )

# ---- Price cache (خفض الضغط على API) ----
_PRICE_CACHE: Dict[str, Tuple[float, float]] = {}  # symbol -> (price, ts)
_PRICE_TTL_SEC = 5.0

async def fetch_current_price(symbol: str) -> Optional[float]:
    """
    ترجع آخر سعر من KuCoin مع تحسينات:
      - كاش محلي لمدة قصيرة لتخفيف الاستدعاءات (TTL=5s).
      - إعادة محاولات مع backoff عند الفشل.
      - مسارات بديلة (fallback) عند فشل get_ticker:
          1) get_24hr_stats (حقل 'last')
          2) آخر شمعة مُغلَقة 1min (get_latest_candle)
    تُعيد None عند الفشل الكامل، مع تسجيل تنبيه في ملف التيرمينال.
    """
    try:
        sym = normalize_symbol(symbol)
        pair = format_symbol(sym)

        # 1) الكاش (إن وجد وحديث)
        now = time.time()
        cached = _PRICE_CACHE.get(sym)
        if cached:
            price_cached, ts_cached = cached
            if (now - ts_cached) <= _PRICE_TTL_SEC and price_cached > 0:
                return float(price_cached)

        last_err = None

        # 2) المحاولة الأساسية: ticker()
        max_retries = 3
        for attempt in range(max_retries):
            try:
                tk = kucoin.get_ticker(pair)  # {'price': '0.1234', ...}
                p = float(tk.get('price', 0) or 0)
                if p > 0:
                    _PRICE_CACHE[sym] = (p, now)
                    return p
                last_err = f"empty/zero price from ticker ({tk})"
            except Exception as e:
                last_err = str(e)
            # backoff بسيط
            await asyncio.sleep(0.5 * (2 ** attempt))

        # 3) fallback #1: 24h stats ('last')
        try:
            stats = kucoin.get_24hr_stats(pair)  # {'last': '0.1234', ...}
            p = float(stats.get('last', 0) or 0)
            if p > 0:
                _PRICE_CACHE[sym] = (p, time.time())
                return p
        except Exception as e:
            last_err = f"24h stats failed: {e}"

        # 4) fallback #2: آخر شمعة مُغلَقة 1min
        try:
            candle = get_latest_candle(sym, interval='1min')
            if candle and float(candle.get('close', 0) or 0) > 0:
                p = float(candle['close'])
                _PRICE_CACHE[sym] = (p, time.time())
                return p
        except Exception as e:
            last_err = f"kline fallback failed: {e}"

        # فشل كامل — سجّل تنبيه واحد للتيرمينال (بدون تلغرام)
        await send_notification(
            f"❌ Price fetch failed for {sym} ({pair}) — {last_err}",
            to_telegram=False,
            tag=f"price_fetch_fail_{sym}"
        )
        return None

    except Exception as e:
        # أي استثناء غير متوقّع
        await send_notification(
            f"❌ Price fetch unexpected error for {symbol}: {e}",
            to_telegram=False,
            tag=f"price_fetch_fail_{(symbol or '?').upper()}"
        )
        return None

# ---- KuCoin REST helpers ----
def get_symbol_meta(pair: str, retries: int = 3) -> Optional[Dict[str, float]]:
    """
    ترجع baseMinSize/baseIncrement/quoteIncrement للزوج؛ None عند الفشل.
    """
    for attempt in range(retries):
        try:
            symbols = kucoin.get_symbols()
            for item in symbols:
                if item['symbol'] == pair:
                    return {
                        'baseMinSize': float(item['baseMinSize']),
                        'baseIncrement': float(item['baseIncrement']),
                        'quoteIncrement': float(item.get('quoteIncrement', 0.01))
                    }
        except Exception as e:
            print(f"[Attempt {attempt+1}] Error fetching symbol info for {pair}: {e}")
            time.sleep(1)  # الدالة متزامنة
    return None

# FIX: تأكيد أن الشمعة المرجعة مُغلَقة (آخر شمعة مكتملة) وليس الشمعة الجارية
def _interval_to_ms(interval: str) -> int:
    """تحويل فاصل KuCoin البسيط إلى ميلي ثانية."""
    mapping = {
        '1min': 60_000,
        '5min': 300_000,
        '15min': 900_000,
        '30min': 1_800_000,
        '1hour': 3_600_000,
        '4hour': 14_400_000,
        '1day': 86_400_000,
    }
    return mapping.get(interval, 3_600_000)

def get_latest_candle(symbol: str, interval: str = '1hour') -> Optional[Dict[str, float]]:
    """
    تُرجع **آخر شمعة مُغلَقة** للفاصل المحدد: timestamp (ms, بداية الشمعة), open, high, low, close, volume.
    """
    try:
        formatted = format_symbol(symbol)
        klines = kucoin.get_kline_data(formatted, interval)  # نافذة حديثة
        if not klines:
            return None

        now_ms = time.time() * 1000.0
        interval_ms = _interval_to_ms(interval)

        # اختر أحدث شمعة مكتملة: start + interval <= الآن
        candidates = []
        for k in klines:
            start = float(k[0])
            start_ms = start if start >= 10**12 else start * 1000.0
            end_ms = start_ms + interval_ms
            if end_ms <= now_ms:
                open_p  = float(k[1])
                close_p = float(k[2])
                high_p  = float(k[3])
                low_p   = float(k[4])
                vol     = float(k[5])
                candidates.append((start_ms, open_p, high_p, low_p, close_p, vol))

        if not candidates:
            return None

        start_ms, open_p, high_p, low_p, close_p, vol = max(candidates, key=lambda x: x[0])
        return {
            "timestamp": start_ms,
            "open": open_p,
            "high": high_p,
            "low": low_p,
            "close": close_p,
            "volume": vol
        }
    except Exception as e:
        print(f"❌ Error fetching candle for {symbol}: {e}")
    return None

# ---- Orders: deal-size (simulation-aware) ----
async def get_order_deal_size(
    order_id: str,
    symbol: Optional[str] = None,
    sim_override: Optional[bool] = None
) -> Tuple[float, float]:
    """
    ترجع (filled_qty, deal_funds) لأمر KuCoin أو أمر محاكاة.
    - في وضع المحاكاة: تُحتسب الكميات/المبالغ بالسعر الحالي من fetch_current_price.
    - sim_override: لو مُمرّر، يفرض نمط التنفيذ (SIM/LIVE).
    """
    try:
        if _effective_simulation(sim_override):
            od = _SIM_ORDERS.get(order_id)
            if not od:
                return 0.0, 0.0

            sym = od.get("symbol") or (symbol or "")
            price = await fetch_current_price(sym)
            if price is None or price <= 0:
                return 0.0, 0.0

            side = (od.get("side") or "").lower()
            if side == "buy":
                funds = float(od.get("funds") or 0)
                qty = quantize_down(funds / max(price, 1e-12), 0.00000001)  # التقريب النهائي يعتمد لاحقًا على baseInc
                deal_funds = funds
            elif side == "sell":
                qty = float(od.get("size") or 0)
                deal_funds = qty * price
            else:
                qty = 0.0
                deal_funds = 0.0

            od["dealSize"] = qty
            od["dealFunds"] = deal_funds
            od["filled"] = True
            _SIM_ORDERS[order_id] = od
            return float(qty), float(deal_funds)

        # الوضع الحقيقي
        order = kucoin.get_order(order_id)
        filled_qty = float(order.get('dealSize', 0))
        filled_funds = float(order.get('dealFunds', 0))
        return filled_qty, filled_funds
    except Exception as e:
        ref = symbol if symbol else f"order {order_id}"
        await send_notification(f"❌ Failed to fetch order details for {ref}: {e}",
                                to_telegram=False, tag=f"order_fetch_fail_{normalize_symbol(ref)}")
        return 0.0, 0.0

# ---- Amount math helper ----
def calculate_new_amount(previous_amount: float, direction: str = "up") -> float:
    """
    تطبيق +/- TRADE_INCREMENT_PERCENT% على مبلغ سابق.
    direction='up' للزيادة؛ 'down' للتخفيض.
    """
    factor = 1 + (TRADE_INCREMENT_PERCENT / 100)
    return round(previous_amount * factor, 2) if direction == "up" else round(previous_amount / factor, 2)

# =====================================================================
# Trade file helpers: update executed fields for a specific trade
# =====================================================================
def _update_trade_exec_fields(
    symbol: str,
    track_num: str,
    cycle_num: str,
    *,
    bought_price: Optional[float] = None,
    sell_price: Optional[float] = None,
    sell_qty: Optional[float] = None,
    bought_at: Optional[float] = None,
    sold_at: Optional[float] = None
) -> None:
    """
    يحدّث حقول التنفيذ داخل TRADES_FILE للصفقة المطابقة (بالرمز + المسار + الدورة):
      - لا يغيّر status (تقوم به الدوال العليا).
      - يضيف/يعدّل: bought_price, sell_price, sell_qty.
      - يضيف طوابع وقت اختيارية: bought_at/sold_at (إن لم تُمرّر، تُستخدم now() عند تحديد السعر المعني).
    منطق اختيار الصفقة:
      1) مطابقة دقيقة لآخر صفقة تحمل symbol/track_num/cycle_num.
      2) إن لم يوجد، أحدث صفقة بالرمز نفسه.
    """
    try:
        if not os.path.exists(TRADES_FILE):
            return
        with open(TRADES_FILE, "r") as f:
            data = json.load(f) or {}
        trades = data.get("trades", [])
        if not isinstance(trades, list) or not trades:
            return

        sym_up = normalize_symbol(symbol)
        target_idx = None
        latest_opened = -1.0

        # محاولة مطابقة دقيقة بالمسار/الدورة
        for idx, tr in enumerate(trades):
            try:
                if normalize_symbol(tr.get("symbol", "")) != sym_up:
                    continue
                if str(tr.get("track_num")) != str(track_num):
                    continue
                if str(tr.get("cycle_num")) != str(cycle_num):
                    continue
                ts = float(tr.get("opened_at", 0) or 0.0)
                if ts >= latest_opened:
                    latest_opened = ts
                    target_idx = idx
            except Exception:
                continue

        # fallback: أحدث صفقة للرمز
        if target_idx is None:
            for idx, tr in enumerate(trades):
                try:
                    if normalize_symbol(tr.get("symbol", "")) != sym_up:
                        continue
                    ts = float(tr.get("opened_at", 0) or 0.0)
                    if ts >= latest_opened:
                        latest_opened = ts
                        target_idx = idx
                except Exception:
                    continue

        if target_idx is None:
            return

        tr = trades[target_idx]

        # شراء
        if bought_price is not None:
            tr["bought_price"] = float(bought_price)
            tr["bought_at"] = float(bought_at) if bought_at is not None else float(datetime.now(timezone.utc).timestamp())
        # بيع
        if sell_price is not None:
            tr["sell_price"] = float(sell_price)
            tr["sold_at"] = float(sold_at) if sold_at is not None else float(datetime.now(timezone.utc).timestamp())
        # كمية البيع (إن توفّرت)
        if sell_qty is not None:
            tr["sell_qty"] = float(sell_qty)

        trades[target_idx] = tr
        data["trades"] = trades
        with open(TRADES_FILE, "w") as f:
            json.dump(data, f, indent=2)

    except Exception as e:
        print(f"⚠️ _update_trade_exec_fields error for {symbol} T{track_num} C{cycle_num}: {e}")
