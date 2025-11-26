# ============================================
# Section 1) Imports & Config (final)
# (KuCoin V2 + robust adapter + Telethon session path from ENV)
# Replaced OFF-window with IMAP "Email Gate" toggling (buy/sell crypto)
# ============================================

import os
import re
import json
import time
import asyncio
import inspect
from datetime import datetime, timezone, date
from typing import Optional, Tuple, Dict, Any, List
from decimal import Decimal, getcontext

# ---- NEW: IMAP (Email Gate) imports ----
import imaplib
import email
import ssl

from telethon import TelegramClient, events

# ---- دقة العمليات العشرية ----
getcontext().prec = 28

# -------- KuCoin API --------
KUCOIN_API_KEY = ''          # <-- ضع KuCoin API Key
KUCOIN_API_SECRET = ''       # <-- ضع KuCoin API Secret
KUCOIN_API_PASSPHRASE = ''   # <-- ضع KuCoin API Passphrase (نصيّة كما هي في V2)

# إصدار مفاتيح KuCoin: V2
KUCOIN_API_KEY_VERSION = 2
# (اختياري) مفاتيح Partner/Broker من البيئة
KUCOIN_PARTNER = os.getenv("KUCOIN_PARTNER", "")
KUCOIN_PARTNER_KEY = os.getenv("KUCOIN_PARTNER_KEY", "")
KUCOIN_PARTNER_SECRET = os.getenv("KUCOIN_PARTNER_SECRET", "")

# اختيار بيئة KuCoin
KUCOIN_SANDBOX = os.getenv("KUCOIN_SANDBOX", "false").lower() == "true"

# -------- Telegram API --------
TG_API_ID = ""            # Telegram API ID (int بصيغة نص)
TG_API_HASH = ""        # Telegram API HASH
YOUR_TELEGRAM_ID = ""  # Telegram user ID (int بصيغة نص) إن احتجته
CHANNEL_USERNAME = ""  # مصدر التوصيات '@YourChannel' أو ID عددي

# مسار جلسة Telethon من ENV لتفادي قفل sqlite
SESSION_PATH = os.environ.get(
    "TELEGRAM_SESSION",
    os.path.expanduser("~/.config/cryptobot/bot.session")
)
try:
    os.makedirs(os.path.dirname(SESSION_PATH), exist_ok=True)
except Exception:
    pass

# --- حساب ثاني للتنبيهات (هبوط 4%) ---
SECOND_TELEGRAM_USERNAME = os.getenv("SECOND_TELEGRAM_USERNAME", "0")
SECOND_TELEGRAM_ID = int(os.getenv("SECOND_TELEGRAM_ID", "0"))  # اتركه 0 لاستخدام الـ username

# -------- إعدادات التداول --------
INITIAL_TRADE_AMOUNT = float(os.getenv("INITIAL_TRADE_AMOUNT", "50.0"))          # المبلغ الأساسي للمسار 1
TRADE_INCREMENT_PERCENT = float(os.getenv("TRADE_INCREMENT_PERCENT", "2.0"))     # نسبة الزيادة بين المسارات
MAX_OPEN_TRADES = int(os.getenv("MAX_OPEN_TRADES", "10"))                        # للمرجعية فقط (السعة الفعلية من cycle_count)

# -------- وضع المحاكاة --------
# False = تنفيذ فعلي؛ يمكن تبديله يدويًا فقط (تم إلغاء الحظر المجدول واستبداله ببريد IMAP).
SIMULATION_MODE = os.getenv("SIMULATION_MODE", "false").lower() == "true"

# --- خيارات طباعة/ديبغ ---
ENABLE_CONSOLE_ECHO = os.getenv("ENABLE_CONSOLE_ECHO", "true").lower() == "true"  # طباعة إشعارات التلغرام أيضاً في التيرمنال
DEBUG_FUNDS = os.getenv("DEBUG_FUNDS", "true").lower() == "true"                  # تتبّع أسباب فشل الشراء/الرصيد

# -------- ملفات البيانات --------
TRADES_FILE = os.getenv("TRADES_FILE", 'trades_data.json')                  # تاريخ + حالة الصفقات
TRACK_FILE = os.getenv("TRACK_FILE", 'trade_counter.json')                  # حالة المسارات/الدورات (حي)
STATE_FILE = os.getenv("STATE_FILE", 'bot_state.json')                      # حالة تشغيل/إيقاف البوت
TERMINAL_LOG_FILE = os.getenv("TERMINAL_LOG_FILE", 'terminal_notifications.json')  # تجميع إشعارات التيرمنال
SUMMARY_FILE = os.getenv("SUMMARY_FILE", 'summary.json')                    # ملخّص PnL

# -------- NEW: ملفات وحالة Email Gate --------
EMAIL_STATE_FILE = os.getenv("EMAIL_STATE_FILE", "email_gate_state.json")   # حفظ حالة بوابة الايميل + آخر UID/Message-Id

# -------- NEW: مسار ملف البلاك ليست (ديناميكي) --------
# يُستخدم لتجاهل رموز معينة (Add/Remove/Status List)
BLACKLIST_FILE = os.getenv("BLACKLIST_FILE", "blacklist.json")
# تهيئة الملف إن كان مفقودًا
if not os.path.exists(BLACKLIST_FILE):
    try:
        with open(BLACKLIST_FILE, "w") as f:
            json.dump({"symbols": []}, f, indent=2)
    except Exception:
        pass

# -------- تسميات الدورات (A..Z, AA, AB, ...) --------
DEFAULT_CYCLE_COUNT = int(os.getenv("DEFAULT_CYCLE_COUNT", "10"))  # العدد الابتدائي للدورات (قابل للتغيير بأمر cycl)

def excel_col_label(n: int) -> str:
    """
    1-indexed → 'A', 'B', ..., 'Z', 'AA', 'AB', ...
    """
    n = int(n)
    if n <= 0:
        return "A"
    label = ""
    while n > 0:
        n, rem = divmod(n - 1, 26)
        label = chr(65 + rem) + label
    return label

def get_cycle_labels(count: int) -> List[str]:
    """
    يبني تسميات الدورات حسب العدد المطلوب (A..Z, AA..).
    """
    try:
        c = max(1, int(count))
    except Exception:
        c = DEFAULT_CYCLE_COUNT
    return [excel_col_label(i) for i in range(1, c + 1)]

# -------- هيكل المسارات (دورات ديناميكية) --------
def create_trade_counter() -> Dict[str, Any]:
    """
    هيكل ابتدائي:
      - 10 مسارات (1..10) — يُنشأ المزيد تلقائيًا عند الحاجة.
      - دورات حسب DEFAULT_CYCLE_COUNT بصيغة <Label><Track> مثل: 'A1', 'B1', ...
      - تخزين cycle_count لدعم cycl لاحقًا.
    """
    tracks: Dict[str, Any] = {}
    labels = get_cycle_labels(DEFAULT_CYCLE_COUNT)
    for t in range(1, 11):
        cycles = {f"{lab}{t}": None for lab in labels}
        base = round(INITIAL_TRADE_AMOUNT * ((1 + TRADE_INCREMENT_PERCENT / 100) ** (t - 1)), 2)
        tracks[str(t)] = {"cycles": cycles, "amount": base}

    return {
        "tracks": tracks,
        "cycle_count": DEFAULT_CYCLE_COUNT,
        "total_trades": 0,
        "total_successful_trades": 0,
        "total_failed_trades": 0,
        "total_lost_trades": 0,
        "daily_successful_trades": {},
        "current_path": 1
    }

def _migrate_track_structure(structure: Dict[str, Any]) -> Dict[str, Any]:
    """
    ترقية آمنة لملف قديم:
      - يضمن وجود cycle_count.
      - يضيف مفاتيح الدورات الناقصة لكل مسار (لا يحذف شيئًا).
    """
    if "cycle_count" not in structure:
        structure["cycle_count"] = DEFAULT_CYCLE_COUNT

    try:
        labels = get_cycle_labels(int(structure.get("cycle_count", DEFAULT_CYCLE_COUNT)))
    except Exception:
        labels = get_cycle_labels(DEFAULT_CYCLE_COUNT)

    tracks = structure.get("tracks") or {}
    for tkey, tdata in tracks.items():
        cycles = (tdata or {}).get("cycles") or {}
        for lab in labels:
            key = f"{lab}{tkey}"
            if key not in cycles:
                cycles[key] = None
        tdata["cycles"] = cycles
    structure["tracks"] = tracks
    return structure

# تهيئة TRACK_FILE إن كان مفقودًا
if not os.path.exists(TRACK_FILE):
    with open(TRACK_FILE, 'w') as f:
        json.dump(create_trade_counter(), f, indent=2)

def get_trade_structure() -> Dict[str, Any]:
    """
    قراءة هيكل المسارات/الدورات، مع ترقية آمنة عند الحاجة.
    """
    if os.path.exists(TRACK_FILE):
        try:
            with open(TRACK_FILE, 'r') as f:
                data = json.load(f)
            return _migrate_track_structure(data)
        except Exception:
            return create_trade_counter()
    return create_trade_counter()

# -------- السعة الفعلية (متزامنة مع cycle_count) --------
def get_effective_max_open(structure: Optional[Dict[str, Any]] = None) -> int:
    """
    السعة القصوى المتزامنة = عدد الدورات الحالي (cycle_count).
    """
    try:
        s = structure or get_trade_structure()
        return int(s.get("cycle_count", DEFAULT_CYCLE_COUNT))
    except Exception:
        return DEFAULT_CYCLE_COUNT

# -------- حالة تشغيل/إيقاف --------
def is_bot_active() -> bool:
    if not os.path.exists(STATE_FILE):
        return True
    try:
        with open(STATE_FILE, 'r') as f:
            return bool(json.load(f).get("active", True))
    except Exception:
        return True

def set_bot_active(active: bool):
    with open(STATE_FILE, 'w') as f:
        json.dump({"active": bool(active)}, f, indent=2)

# -------- Telegram Client (باستخدام ملف جلسة من ENV) --------
client = TelegramClient(SESSION_PATH, TG_API_ID, TG_API_HASH)

# -------- KuCoin Client (يدعم المكتبتين) --------
KUCOIN_BASE_URL = 'https://openapi-sandbox.kucoin.com' if KUCOIN_SANDBOX else 'https://api.kucoin.com'

class _KucoinAdapter:
    """
    يوحّد الواجهة بين:
      1) حزمة kucoin-python (تقدّم Client موحّد)
      2) حزمة python-kucoin (تقسيم Market/Trade/Account|User)
    ويوفّر الدوال المستعملة: get_accounts, get_symbols, get_ticker,
    get_24hr_stats, get_kline_data, create_market_order, get_order
    """
    def __init__(self):
        self.mode = None
        self._client = None
        self._market = None
        self._trade  = None
        self._user   = None  # قد تكون User أو Account

        # حاول أولاً Client (kucoin-python)
        try:
            from kucoin.client import Client as _Client
            self.mode = 'client'
            self._client = self._build_client(_Client)
        except Exception:
            # split-mode (python-kucoin): Market/Trade + (User أو Account)
            self.mode = 'split'
            self._market, self._trade, self._user = self._build_split()

    # --- إنشاء Client (إن توفّر) ---
    def _build_client(self, _ClientCls):
        sig = inspect.signature(_ClientCls.__init__)
        allowed = set(sig.parameters.keys())
        kwargs: Dict[str, Any] = {}

        if 'sandbox' in allowed:
            kwargs['sandbox'] = KUCOIN_SANDBOX
        if 'api_key_version' in allowed:
            kwargs['api_key_version'] = KUCOIN_API_KEY_VERSION

        # مفاتيح الشريك (إن كانت مدعومة)
        if KUCOIN_PARTNER and 'partner' in allowed:
            kwargs['partner'] = KUCOIN_PARTNER
        if KUCOIN_PARTNER_KEY and 'partner_key' in allowed:
            kwargs['partner_key'] = KUCOIN_PARTNER_KEY
        if KUCOIN_PARTNER_SECRET and 'partner_secret' in allowed:
            kwargs['partner_secret'] = KUCOIN_PARTNER_SECRET

        cli = _ClientCls(KUCOIN_API_KEY, KUCOIN_API_SECRET, KUCOIN_API_PASSPHRASE, **kwargs)

        # ضبط ترويسة KC-API-KEY-VERSION إن لم يدعم المُنشئ هذا الخيار
        try:
            if 'api_key_version' not in allowed:
                sess = getattr(cli, 'session', None) or getattr(cli, '_session', None)
                if sess and hasattr(sess, 'headers'):
                    sess.headers['KC-API-KEY-VERSION'] = str(KUCOIN_API_KEY_VERSION)
        except Exception:
            pass
        return cli

    # --- split-mode: محاولة استيراد Market/Trade و User|Account بمرونة ---
    def _build_split(self):
        _market = _trade = _user = None

        # Market & Trade من kucoin.client
        Market = Trade = None
        try:
            from kucoin.client import Market as _M, Trade as _T
            Market, Trade = _M, _T
        except Exception:
            pass

        # User أو Account بعدة مسارات محتملة
        UserCls = None
        import_errs = []

        # 1) kucoin.client.User
        if UserCls is None:
            try:
                from kucoin.client import User as _U
                UserCls = _U
            except Exception as e:
                import_errs.append(f"client.User: {e}")

        # 2) kucoin.client.Account
        if UserCls is None:
            try:
                from kucoin.client import Account as _A
                UserCls = _A
            except Exception as e:
                import_errs.append(f"client.Account: {e}")

        # 3) kucoin.user.user.User
        if UserCls is None:
            try:
                from kucoin.user.user import User as _U2
                UserCls = _U2
            except Exception as e:
                import_errs.append(f"user.user.User: {e}")

        # 4) kucoin.account.Account
        if UserCls is None:
            try:
                from kucoin.account import Account as _A2
                UserCls = _A2
            except Exception as e:
                import_errs.append(f"account.Account: {e}")

        # بناء الكائنات مع ضبط الـ URL / Sandbox / api_key_version إن توفّرت
        if Market is not None:
            try:
                market_sig = inspect.signature(Market.__init__)
                mk_kwargs: Dict[str, Any] = {}
                if 'url' in market_sig.parameters:
                    mk_kwargs['url'] = KUCOIN_BASE_URL
                _market = Market(**mk_kwargs)
            except Exception:
                _market = None

        if Trade is not None:
            try:
                trade_sig = inspect.signature(Trade.__init__)
                tr_kwargs: Dict[str, Any] = {}
                if 'is_sandbox' in trade_sig.parameters:
                    tr_kwargs['is_sandbox'] = KUCOIN_SANDBOX
                if 'url' in trade_sig.parameters:
                    tr_kwargs['url'] = KUCOIN_BASE_URL
                if 'api_key_version' in trade_sig.parameters:
                    tr_kwargs['api_key_version'] = KUCOIN_API_KEY_VERSION
                _trade = Trade(KUCOIN_API_KEY, KUCOIN_API_SECRET, KUCOIN_API_PASSPHRASE, **tr_kwargs)

                # إجبار ترويسة V3 إن أمكن
                try:
                    sess = getattr(_trade, 'session', None) or getattr(_trade, '_session', None)
                    if sess and hasattr(sess, 'headers'):
                        sess.headers['KC-API-KEY-VERSION'] = str(KUCOIN_API_KEY_VERSION)
                except Exception:
                    pass
            except Exception:
                _trade = None

        if UserCls is not None:
            try:
                user_sig = inspect.signature(UserCls.__init__)
                us_kwargs: Dict[str, Any] = {}
                if 'is_sandbox' in user_sig.parameters:
                    us_kwargs['is_sandbox'] = KUCOIN_SANDBOX
                if 'url' in user_sig.parameters:
                    us_kwargs['url'] = KUCOIN_BASE_URL
                if 'api_key_version' in user_sig.parameters:
                    us_kwargs['api_key_version'] = KUCOIN_API_KEY_VERSION
                _user = UserCls(KUCOIN_API_KEY, KUCOIN_API_SECRET, KUCOIN_API_PASSPHRASE, **us_kwargs)

                # إجبار ترويسة V3 إن أمكن
                try:
                    sess = getattr(_user, 'session', None) or getattr(_user, '_session', None)
                    if sess and hasattr(sess, 'headers'):
                        sess.headers['KC-API-KEY-VERSION'] = str(KUCOIN_API_KEY_VERSION)
                except Exception:
                    pass
            except Exception:
                _user = None

        # طباعة تلميح في حال فشل كامل (يسمح للمحاكاة بالعمل)
        if _market is None or _trade is None:
            print("⚠️ python-kucoin: تعذّر تهيئة Market/Trade بالكامل. بعض الوظائف قد لا تعمل في الوضع الحقيقي.")

        if _user is None:
            print("⚠️ python-kucoin: لم يتم إيجاد User/Account. سيُحاول المحوّل قراءة الرصيد عبر بدائل، وإلا 0.")

        return _market, _trade, _user

    # --- واجهة موحّدة يستخدمها باقي الكود ---
    def get_accounts(self):
        try:
            if self.mode == 'client':
                return self._client.get_accounts()
            # split: جرّب user.get_account_list / get_accounts
            for obj in (self._user, self._trade):
                if obj is None:
                    continue
                for name in ('get_account_list', 'get_accounts', 'getAccounts'):
                    fn = getattr(obj, name, None)
                    if callable(fn):
                        return fn()
        except Exception as e:
            print(f"get_accounts error: {e}")
        return []

    def get_symbols(self):
        try:
            if self.mode == 'client':
                return self._client.get_symbols()
            for name in ('get_symbol_list', 'get_symbols'):
                fn = getattr(self._market, name, None) if self._market else None
                if callable(fn):
                    return fn()
        except Exception as e:
            print(f"get_symbols error: {e}")
        return []

    def get_ticker(self, symbol: str):
        try:
            if self.mode == 'client':
                return self._client.get_ticker(symbol)
            fn = getattr(self._market, 'get_ticker', None) if self._market else None
            if callable(fn):
                return fn(symbol)
        except Exception as e:
            print(f"get_ticker error: {e}")
        return {}

    def get_24hr_stats(self, symbol: str):
        try:
            if self.mode == 'client':
                fn = getattr(self._client, 'get_24hr_stats', None) or getattr(self._client, 'get_24h_stats', None)
                if callable(fn):
                    return fn(symbol)
            else:
                fn = None
                if self._market:
                    fn = getattr(self._market, 'get_24h_stats', None) or getattr(self._market, 'get_24hr_stats', None)
                if callable(fn):
                    return fn(symbol)
        except Exception as e:
            print(f"get_24hr_stats error: {e}")
        return {}

    def get_kline_data(self, symbol: str, interval: str):
        """
        يعيد شكل قريب من python-kucoin: [ [ts, open, close, high, low, vol, ...], ... ]
        """
        try:
            if self.mode == 'client':
                fn = getattr(self._client, 'get_kline_data', None) or getattr(self._client, 'get_kline', None)
            else:
                fn = getattr(self._market, 'get_kline', None) if self._market else None
            if callable(fn):
                return fn(symbol, interval)
        except Exception as e:
            print(f"get_kline_data error: {e}")
        return []

    def create_market_order(self, symbol: str, side: str, **kwargs):
        """
        kwargs: funds/size
        """
        if self.mode == 'client':
            return self._client.create_market_order(symbol, side, **kwargs)
        # split:
        if self._trade is None:
            raise RuntimeError("Trade client not available")
        return self._trade.create_market_order(symbol, side, **kwargs)

    def get_order(self, order_id: str):
        try:
            if self.mode == 'client':
                return self._client.get_order(order_id)
            # split:
            fn = None
            if self._trade:
                fn = getattr(self._trade, 'get_order_details', None) or getattr(self._trade, 'get_order', None)
            if callable(fn):
                return fn(order_id)
        except Exception as e:
            print(f"get_order error: {e}")
        return {}

# أنشئ محوّل موحّد
kucoin = _KucoinAdapter()

# -------- NEW: إعدادات Email Gate (IMAP) --------
# الهدف: مراقبة بريد IMAP لرسائل تحوي "buy crypto" لتمكين استقبال التوصيات
# و"sell crypto" (وتحمّل الخطأ الإملائي "cryrpto") لتعطيله.
IMAP_HOST = os.getenv("IMAP_HOST", "imap.gmail.com")
IMAP_PORT = int(os.getenv("IMAP_PORT", "993"))
IMAP_USER = os.getenv("IMAP_USER", "")                 # مثال: "youremail@gmail.com"
IMAP_APP_PASSWORD = os.getenv("IMAP_PASSWORD", "")  # App Password لتطبيق البريد
IMAP_FOLDER = os.getenv("IMAP_FOLDER", "INBOX")        # المجلد المرصود

# تمكين/تعطيل بوابة الإيميل + فاصل الفحص بالثواني
EMAIL_GATE_ENABLED = os.getenv("EMAIL_GATE_ENABLED", "true").lower() == "true"
EMAIL_GATE_POLL_SEC = int(os.getenv("EMAIL_GATE_POLL_SEC", "30"))

# كلمات/أنماط التفعيل والإيقاف (غير حسّاسة لحالة الأحرف)
EMAIL_BUY_PATTERNS = [
    r"\bbuy\s*crypto\b",
    r"\bbuy\s*crypt[o0]\b",   # تحمّل استبدال الحرف o بـ 0
]
EMAIL_SELL_PATTERNS = [
    r"\bsell\s*crypto\b",
    r"\bsell\s*cryrpto\b",    # الخطأ الإملائي المذكور
    r"\bsell\s*crypt[o0]\b",
]

# (اختياري) حصر المرسلين الموثوقين: فارغ = الجميع
# مثال: "boss@example.com,me@domain.com"
EMAIL_TRUSTED_SENDERS = set(
    [s.strip().lower() for s in os.getenv("EMAIL_TRUSTED_SENDERS", "").split(",") if s.strip()]
)

# تهيئة ملف حالة بوابة الإيميل إن كان مفقودًا
if not os.path.exists(EMAIL_STATE_FILE):
    try:
        with open(EMAIL_STATE_FILE, "w") as f:
            json.dump({"last_uid": 0, "last_msgid": "", "gate_open": True}, f, indent=2)
    except Exception:
        pass
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
# ============================================
# Section 3) Tracks & Slots Management
# (updated: add 'drwn' as terminal status with no pointer move,
#           and count it via total_drawdown_trades)
# ============================================

def save_trade_structure(structure: Dict[str, Any]):
    """حفظ هيكل المسارات/الدورات في الملف."""
    try:
        with open(TRACK_FILE, 'w') as f:
            json.dump(structure, f, indent=2)
    except Exception as e:
        print(f"⚠️ save_trade_structure error: {e}")

# ---------- مساعد: جلب لابل الدورات الحالية من الهيكل ----------
def _get_labels_from_structure(structure: Dict[str, Any]) -> List[str]:
    """
    يعتمد على cycle_count المخزن في trade_counter.json
    ويُولّد تسميات Excel (A..Z, AA..).
    """
    count = int(structure.get("cycle_count", DEFAULT_CYCLE_COUNT))
    return get_cycle_labels(count)

# ---------- مؤشّر مسار لكل "تسمية دورة" (A, B, ..., AA, ...) ----------
def _ensure_cycle_track_ptr(structure: Dict[str, Any]) -> Dict[str, int]:
    """
    يضمن وجود mapping يحدد المسار الحالي لكل "تسمية دورة" (Excel-like).
    مثال: {'A': 1, 'B': 1, ..., 'J': 1} أو {'A':1 .. 'T':1} عند 20 دورة.
    - لا يحذف مفاتيح قديمة (للأمان).
    - يضيف المفاتيح الناقصة فقط ويعيّنها للمسار 1.
    """
    labels = _get_labels_from_structure(structure)
    ptr = structure.get("cycle_track_ptr")
    if not isinstance(ptr, dict):
        ptr = {}
    # أضف الجديدة فقط
    for lab in labels:
        if lab not in ptr:
            ptr[lab] = 1
    structure["cycle_track_ptr"] = ptr
    save_trade_structure(structure)
    return structure["cycle_track_ptr"]

def _ensure_track_exists(structure: Dict[str, Any], track_idx: int):
    """ينشئ مسارًا جديدًا عند الحاجة مع دورات ديناميكية حسب cycle_count الحالية."""
    tkey = str(track_idx)
    if tkey not in structure["tracks"]:
        structure["tracks"][tkey] = create_new_track(track_idx, track_base_amount(track_idx))
        save_trade_structure(structure)

def update_slot_status(structure: Dict[str, Any], track_num: str, cycle_num: str, cell: Dict[str, Any]):
    """تحديث حالة الخانة في هيكل المسارات/الدورات."""
    try:
        if track_num in structure["tracks"] and cycle_num in structure["tracks"][track_num]["cycles"]:
            structure["tracks"][track_num]["cycles"][cycle_num] = cell
    except Exception as e:
        print(f"⚠️ update_slot_status error for Track {track_num} Cycle {cycle_num}: {e}")

def find_available_slot(structure: Dict[str, Any]) -> Tuple[Optional[str], Optional[str], Optional[float]]:
    """
    اختيار خانة متاحة وفق المؤشر لكل تسمية دورة:
      - لكل label (A, B, ..., AA, ...) مؤشر مسار حالي (cycle_track_ptr[label]).
      - نجرّب الخانة (label + target_track) داخل ذلك المسار.
      - إذا الخانة مشغولة ننتقل للّابل التالي (لا نغيّر المؤشر).
    يعيد: (track_num:str, cycle_key:str مثل 'A5', amount:float)
    *يحترم الحد الحالي لعدد الدورات (cycle_count) — يعني التقليص يمنع الفتح فوق الحد.
    """
    try:
        cycle_ptr = _ensure_cycle_track_ptr(structure)
        labels = _get_labels_from_structure(structure)

        # مرّ على اللابلز بالترتيب المحدد
        for lab in labels:
            target_track_idx = int(cycle_ptr.get(lab, 1))

            # تأكد من وجود المسار
            _ensure_track_exists(structure, target_track_idx)
            tkey = str(target_track_idx)

            # اسم الخانة لتلك التسمية داخل المسار الهدف
            cell_key = f"{lab}{tkey}"

            tdata = structure["tracks"].get(tkey, {})
            cell = (tdata.get("cycles") or {}).get(cell_key)

            if cell is None:
                amount = tdata.get("amount", track_base_amount(target_track_idx))
                return tkey, cell_key, amount

        # لا خانة متاحة الآن
        return None, None, None

    except Exception as e:
        print(f"⚠️ find_available_slot error: {e}")
        return None, None, None

def track_base_amount(track_num: int) -> float:
    """حساب حجم الصفقة للمسار، بدءًا من INITIAL_TRADE_AMOUNT وزيادة TRADE_INCREMENT_PERCENT% تراكمية."""
    return round(INITIAL_TRADE_AMOUNT * ((1 + TRADE_INCREMENT_PERCENT / 100) ** (track_num - 1)), 2)

def create_new_track(track_num: int, base_amount: float) -> Dict[str, Any]:
    """إنشاء مسار جديد مع دورات ديناميكية وفق cycle_count الحالي."""
    try:
        structure = get_trade_structure()  # للاستفادة من cycle_count
        labels = _get_labels_from_structure(structure)
    except Exception:
        labels = get_cycle_labels(DEFAULT_CYCLE_COUNT)

    cycles = {f"{lab}{track_num}": None for lab in labels}
    return {"cycles": cycles, "amount": base_amount}

async def update_active_trades(slot_pos: Tuple[str, str], cell_data: Dict[str, Any], final_status: str):
    """
    تحديث عدادات الصفقات وحالة الخانة بعد الإغلاق + تحريك مؤشر المسار للتسمية:
      - closed  → المؤشر = المسار الحالي + 1 (نفس التسمية).
      - stopped → المؤشر = max(1, المسار الحالي - 6) (نفس التسمية).
      - failed  → لا تغيير على المؤشر.
      - drwn    → حالة نهائية مثل الإغلاق اليدوي بالخسارة **بدون** تحريك المؤشر (لا رجوع 6 مسارات).
    *إشعار إضافي إذا أغلقت الصفقة على تسمية دورة خارج الحد الحالي (لن يُحجز مكان جديد لهذه التسمية) فقط لـ closed/stopped.
    *تحديث العدّادات:
        - total_trades يزيد دائمًا بصفقة واحدة.
        - closed  → total_successful_trades += 1 + daily_successful_trades[اليوم] += 1
        - stopped → total_lost_trades += 1
        - failed  → total_failed_trades += 1
        - drwn    → total_drawdown_trades += 1  (حقل جديد آمن حتى لو لم يكن مبدّلاً في Section 1)
    """
    track_num, cycle_num = slot_pos
    structure = get_trade_structure()

    # إخلاء الخانة (لكن نحافظ على cell_data في التاريخ عبر TRADES_FILE سابقًا)
    try:
        cell = structure["tracks"][track_num]["cycles"].get(cycle_num) or {}
        cell.update(cell_data or {})
        cell["status"] = None
        structure["tracks"][track_num]["cycles"][cycle_num] = None
    except Exception as e:
        sym_dbg = (cell_data or {}).get("symbol", "?")
        print(f"⚠️ update_active_trades clear-slot error for {sym_dbg} at {track_num}-{cycle_num}: {e}")

    # تحديث العدادات الإجمالية/اليومية
    structure["total_trades"] = structure.get("total_trades", 0) + 1
    today_str = date.today().isoformat()

    if final_status == "closed":
        structure["total_successful_trades"] = structure.get("total_successful_trades", 0) + 1
        daily = structure.get("daily_successful_trades", {})
        daily[today_str] = daily.get(today_str, 0) + 1
        structure["daily_successful_trades"] = daily
        log_terminal_notification(f"Trade closed in profit ({cell_data.get('symbol', '?')})", tag="trade_closed")

    elif final_status == "stopped":
        structure["total_lost_trades"] = structure.get("total_lost_trades", 0) + 1
        log_terminal_notification(f"Trade stopped at SL ({cell_data.get('symbol', '?')})", tag="trade_stopped")

    elif final_status == "failed":
        structure["total_failed_trades"] = structure.get("total_failed_trades", 0) + 1
        log_terminal_notification(f"Trade failed ({cell_data.get('symbol', '?')})", tag="trade_failed")

    elif final_status == "drwn":
        # جديد: عدّاد خاص للإغلاقات اليدوية بالخسارة بدون رجوع 6 مسارات
        structure["total_drawdown_trades"] = structure.get("total_drawdown_trades", 0) + 1
        log_terminal_notification(f"Trade closed manually in loss ({cell_data.get('symbol', '?')})",
                                  tag="trade_drawdown")

    # تحريك مؤشر المسار للتسمية المعنية
    try:
        # استخرج حروف التسمية من cycle_num (مثلاً من 'AA5' → 'AA')
        m = re.match(r"([A-Za-z]+)", str(cycle_num))
        cycle_label = m.group(1).upper() if m else "A"

        current_track_idx = int(track_num)
        cycle_ptr = _ensure_cycle_track_ptr(structure)

        new_track_idx = cycle_ptr.get(cycle_label, current_track_idx)

        if final_status == "closed":
            new_track_idx = current_track_idx + 1
        elif final_status == "stopped":
            new_track_idx = max(1, current_track_idx - 6)
        elif final_status == "failed":
            # لا تغيير
            new_track_idx = cycle_ptr.get(cycle_label, current_track_idx)
        elif final_status == "drwn":
            # لا تغيير على الإطلاق (لا رجوع ستة مسارات)
            new_track_idx = cycle_ptr.get(cycle_label, current_track_idx)

        cycle_ptr[cycle_label] = int(new_track_idx)
        _ensure_track_exists(structure, int(new_track_idx))  # أنشئ المسار لو غير موجود

        # إشعار خارج الحد الحالي فقط لـ closed/stopped (لا داعي لـ drwn/failed)
        labels_now = _get_labels_from_structure(structure)
        if final_status in ("closed", "stopped") and cycle_label not in labels_now:
            try:
                sym_dbg = (cell_data or {}).get("symbol", "?")
                await send_notification(
                    f"ℹ️ Closed {sym_dbg} at {cycle_num} without reserving a new slot "
                    f"(outside current cycle limit = {len(labels_now)})."
                )
            except Exception:
                pass

    except Exception as e:
        sym_dbg = (cell_data or {}).get("symbol", "?")
        print(f"⚠️ cycle pointer update error for {sym_dbg} at {cycle_num}: {e}")

    save_trade_structure(structure)

async def update_trade_status(symbol: str, status: str, track_num: str = None, cycle_num: str = None):
    """
    تعديل حالة الصفقة في TRADES_FILE.
    - دعم المطابقة الدقيقة: إذا تم تمرير track_num/cycle_num يتم تحديث الصفقة المطابقة تمامًا.
    - إن لم يُمرَّرا: يرجع للسلوك القديم (آخر صفقة بالرمز).
    - يمنع الكتابة فوق الحالات النهائية نهائيًا: 'closed' أو 'stopped' إذا كانت أحدث صفقة منتهية بالفعل.
    - عند التحويل من failed → stopped/closed: يُعدّل عدّادات TRACK_FILE (إجمالي الفاشلة/الخسارة/الناجحة + اليومي).
    """
    try:
        if not os.path.exists(TRADES_FILE):
            return

        with open(TRADES_FILE, 'r') as f:
            data = json.load(f)

        trades = data.get("trades", [])
        target_idx = None
        latest_opened = -1.0

        symbol_up = (symbol or "").upper().replace('-', '').replace('/', '')

        # 1) مطابقة دقيقة بالمسار/الدورة إن توفرت
        if track_num is not None and cycle_num is not None:
            for idx, tr in enumerate(trades):
                try:
                    if (tr.get("symbol") or "").upper().replace('-', '').replace('/', '') != symbol_up:
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

        # 2) fallback: أحدث صفقة لهذا الرمز غير النهائية
        if target_idx is None:
            latest_opened = -1.0
            for idx, tr in enumerate(trades):
                if (tr.get("symbol") or "").upper().replace('-', '').replace('/', '') != symbol_up:
                    continue
                prev_status = (tr.get("status") or "").lower()
                if prev_status in ("closed", "stopped", "drwn"):
                    continue  # نهائية
                ts = float(tr.get("opened_at", 0) or 0.0)
                if ts >= latest_opened:
                    latest_opened = ts
                    target_idx = idx

        # 3) إن لم نجد غير نهائية: أحدث صفقة لهذا الرمز كيفما كانت
        if target_idx is None:
            latest_opened = -1.0
            for idx, tr in enumerate(trades):
                if (tr.get("symbol") or "").upper().replace('-', '').replace('/', '') != symbol_up:
                    continue
                ts = float(tr.get("opened_at", 0) or 0.0)
                if ts >= latest_opened:
                    latest_opened = ts
                    target_idx = idx

        if target_idx is not None:
            prev_status = (trades[target_idx].get("status") or "").lower()
            new_status = status.lower()

            # لا نسمح بالكتابة فوق 'closed'/'stopped'/'drwn' بحالة مختلفة (نهائية بالفعل)
            if prev_status in ("closed", "stopped", "drwn") and new_status not in (prev_status,):
                return

            # تحديث الحالة والطابع الزمني
            trades[target_idx]["status"] = status
            trades[target_idx]["closed_at"] = datetime.now(timezone.utc).timestamp()

            # --- تعديل عدّادات TRACK_FILE عند التحويل من failed → stopped/closed ---
            try:
                if prev_status == "failed" and new_status in ("stopped", "closed"):
                    structure = get_trade_structure()

                    # أنقِص الفاشلة بمقدار 1 (حد أدنى 0)
                    structure["total_failed_trades"] = max(0, int(structure.get("total_failed_trades", 0)) - 1)

                    if new_status == "stopped":
                        structure["total_lost_trades"] = int(structure.get("total_lost_trades", 0)) + 1
                        log_terminal_notification(f"Failed→Stopped fix ({symbol_up})", tag="fix_failed_to_stopped")
                    else:  # closed
                        structure["total_successful_trades"] = int(structure.get("total_successful_trades", 0)) + 1
                        today_str = date.today().isoformat()
                        daily = structure.get("daily_successful_trades", {})
                        daily[today_str] = int(daily.get(today_str, 0)) + 1
                        structure["daily_successful_trades"] = daily
                        log_terminal_notification(f"Failed→Closed fix ({symbol_up})", tag="fix_failed_to_closed")

                    save_trade_structure(structure)
            except Exception as e:
                print(f"⚠️ counters fix error for {symbol_up}: {e}")

            # اكتب ملف الصفقات
            with open(TRADES_FILE, 'w') as f:
                json.dump(data, f, indent=2)

    except Exception as e:
        print(f"⚠️ Failed to update trade status for {symbol}: {e}")

# ---------- NEW: فحص وتجميع الفتحات الشاغرة ----------
def get_empty_slots(structure: Optional[Dict[str, Any]] = None, include_out_of_range: bool = False) -> Dict[str, List[str]]:
    """
    يرجع dict: track_key -> قائمة أكواد الخلايا الفارغة في هذا المسار.
    - بشكل افتراضي، يحصر على تسميات الدورات ضمن الحد الحالي (cycle_count).
    - لو include_out_of_range=True يعرض أيضًا الخلايا الفارغة خارج الحد (إن وجدت).
    """
    structure = structure or get_trade_structure()
    allowed_labels = set(_get_labels_from_structure(structure))
    out: Dict[str, List[str]] = {}
    try:
        for tkey, tdata in (structure.get("tracks") or {}).items():
            cycles = (tdata or {}).get("cycles") or {}
            empty = []
            for cname, cell in cycles.items():
                if cell is not None:
                    continue
                m = re.match(r"([A-Za-z]+)\d+", str(cname))
                lab = m.group(1).upper() if m else None
                if lab is None:
                    continue
                if (lab in allowed_labels) or include_out_of_range:
                    empty.append(cname)
            if empty:
                out[str(tkey)] = sorted(empty, key=lambda s: (len(re.match(r'([A-Za-z]+)', s).group(1)), s))
    except Exception as e:
        print(f"get_empty_slots error: {e}")
    return out

def predict_next_slot(structure: Optional[Dict[str, Any]] = None) -> Tuple[Optional[str], Optional[str], Optional[float]]:
    """
    يرجّع الخانة المتوقعة التالية حسب مؤشرات كل تسمية (كما سيحجزها البوت الآن).
    """
    return find_available_slot(structure or get_trade_structure())
# ============================================
# Section 4) Execution & Monitoring (UPDATED)
#      - TP ladder (no sell on touch; promote to next)
#      - Trailing-1% AFTER TP1 TOUCH (profit protection):
#          • أرضية = آخر TP مُلامس (≥ TP1 دائماً)
#          • بيع فوري عند كسر الأرضية (floor breach)
#          • أو بيع عند هبوط ≥1% من القمّة مع بقاء السعر فوق الأرضية
#      - Never sell below the last TP touched
#      - 1h-candle SL → إشعار فقط (لا بيع؛ المتابعة نحو الأهداف)
#      - Email Gate replaces OFF window (OFF always False)
#      - All notifications include SYMBOL + T/C tag via send_notification_tc()
#      - Early-Exit Guards: stop monitors if slot/trade already closed
#      - Polling: 60s قبل التفعيل، 10s بعد تفعيل التريلينغ
# ============================================

from datetime import datetime, timezone, date
from typing import List, Dict, Any, Optional, Tuple
import asyncio
import os
import re
import time

RETRACE_PERCENT = 1.0  # نسبة الارتداد للبيع (1%)
EPS = 1e-9             # هامش عددي صغير للتحاشي من مساواة دقيقة

async def execute_trade(symbol: str, entry_price: float, sl_price: float, targets: List[float]):
    # ===== New: Guard - enforce Email Gate (prevent opening new trades when gate closed) =====
    try:
        if not _email_gate_allows():
            try:
                _console_echo(f"[GATE] Email Gate CLOSED — ignoring execute_trade({symbol})")
            except Exception:
                pass
            try:
                await send_notification_tc("⛔️ Recommendation ignored — Email gate is CLOSED.", symbol=symbol)
            except Exception:
                pass
            return
    except Exception:
        # If gate check fails for some reason, be permissive and continue (fail-safe)
        pass

    # --- Skip unsupported symbols (notify only, no entry) ---
    try:
        if _is_blocked_symbol(symbol):
            sym_clean = normalize_symbol(symbol)
            await send_notification_tc("⏭️ تم تجاوز التوصية: الصفقة غير مدعومة حاليًا.",
                                       symbol=sym_clean)
            return
    except Exception:
        pass  # لا توقف التنفيذ إن حصل خطأ في التحقق

    structure = get_trade_structure()

    # السعة الفعلية = عدد الدورات الحالي (متزامنة مع cycl N)
    cap = get_effective_max_open(structure)

    open_count = sum(
        1
        for t in structure["tracks"].values()
        for c in t["cycles"].values()
        if c and c.get("status") in ("open", "buy", "reserved")
    )
    if open_count >= cap:
        await send_notification_tc(
            f"⚠️ Cannot open new trade. Capacity reached {open_count}/{cap} (synced to cycle_count).",
            symbol=symbol
        )
        return

    track_num, cycle_num, amount = find_available_slot(structure)
    if not track_num:
        await send_notification_tc("⚠️ No available slot at the moment.",
                                   symbol=symbol)
        return

    if not targets:
        await send_notification_tc("⚠️ No targets provided. Cancel trade.",
                                   symbol=symbol, track_num=track_num, cycle_num=cycle_num)
        return

    # احفظ الخانة كـ open (ننتظر الوصول لسعر الدخول)
    cell = structure["tracks"][track_num]["cycles"].get(cycle_num) or {}
    cell.update({
        "symbol": normalize_symbol(symbol),
        "entry": entry_price,
        "sl": sl_price,
        "targets": targets,
        "status": "open",
        "amount": amount,
        "track_num": track_num,
        "cycle_num": cycle_num,
        "start_time": None,
        # محاكاة الخانة تعتمد على الوضع العام لحظة استلام التوصية
        "simulated": bool(is_simulation()),
    })
    update_slot_status(structure, track_num, cycle_num, cell)
    save_trade_structure(structure)

    # سجّل في TRADES_FILE
    try:
        if os.path.exists(TRADES_FILE):
            with open(TRADES_FILE, 'r') as f:
                tdata = json.load(f)
        else:
            tdata = {"trades": []}
        tdata["trades"].append({
            "symbol": normalize_symbol(symbol),
            "entry": entry_price,
            "sl": sl_price,
            "targets": targets,
            "track_num": track_num,
            "cycle_num": cycle_num,
            "amount": amount,
            "status": "open",
            "opened_at": datetime.now(timezone.utc).timestamp(),
        })
        with open(TRADES_FILE, 'w') as f:
            json.dump(tdata, f, indent=2)
    except Exception as e:
        print(f"⚠️ failed to append trade for {symbol}: {e}")

    # إشعار واضح (يحوي T/C + القيم)
    await send_notification_tc(
        (
            "📥 New recommendation:\n"
            f"🎯 Entry ≤ {entry_price:.6f}, TP1 ≥ {targets[0]:.6f}, SL ≤ {sl_price:.6f}\n"
            f"💵 Amount: {amount:.2f} USDT"
        ),
        symbol=symbol, track_num=track_num, cycle_num=cycle_num, style="short"
    )

    asyncio.create_task(
        monitor_and_execute(symbol, entry_price, sl_price, targets, amount, track_num, cycle_num)
    )


async def monitor_and_execute(
    symbol: str,
    entry_price: float,
    sl_price: float,
    targets: List[float],
    amount: float,
    track_num: str,
    cycle_num: str
):
    """
    منطق التنفيذ والمراقبة:
    • شراء Market عند وصول السعر ≤ entry (مع احترام حدود KuCoin).
    • لا بيع مباشر عند أي هدف — نتقدّم إلى الهدف التالي (TP ladder).
    • Trailing (بعد لمس TP1 مباشرة):
        - أرضية = آخر TP تم لمسه (≥ TP1 دائماً).
        - بيع فوري إذا كُسرت الأرضية (floor breach).
        - أو بيع عند هبوط ≥1% من القمّة مع بقاء السعر فوق الأرضية.
    • SL: بيع فقط بعد إغلاق شمعة ساعة واحدة ≤ SL وبعد زمن الشراء.
    • احترام وضع المحاكاة إذا كانت الخانة موسومة simulated=True.
    • حارس مبكّر: يوقف المراقبة فور رصد إغلاق/إلغاء الصفقة أو مسح الخانة.
    """
    try:
        pair = format_symbol(symbol)
        meta = get_symbol_meta(pair)
        if not meta:
            await send_notification_tc("❌ Meta fetch failed. Cancel trade.",
                                       symbol=symbol, track_num=track_num, cycle_num=cycle_num)
            await update_trade_status(symbol, 'failed', track_num=track_num, cycle_num=cycle_num)
            await update_active_trades((track_num, cycle_num), {"symbol": normalize_symbol(symbol)}, final_status="failed")
            return

        bought_price = None
        qty = 0.0
        start_time = None

        quote_inc = meta['quoteIncrement']
        base_inc = meta['baseIncrement']
        min_base = meta['baseMinSize']

        # الأهداف (مرتبَة)
        try:
            targets = [float(x) for x in (targets or []) if x is not None]
        except Exception:
            targets = []
        if not targets:
            targets = [float(entry_price * 1.01)]  # احتياط
        targets = sorted(targets)
        tp1_val = float(targets[0])

        # حالة الـ TP ladder + التريلينغ
        highest_idx = -1                         # أعلى هدف تم لمسه
        trailing_armed = False                   # تم تفعيل Trailing بعد لمس TP1؟
        max_after_touch: Optional[float] = None  # أعلى قمة منذ التفعيل
        last_tp_floor: Optional[float] = None    # أرضية لا تقل عن آخر TP تم لمسه

        # مهلة إلغاء: إذا لم نستطع الحصول على سعر لمدة 10 دقائق متواصلة → إلغاء الصفقة
        last_price_ok_ts = time.time()

        # تنبيه SL (دون بيع) — لتفادي تكرار الإشعارات
        sl_alerted = False

        # sim_flag: ثابت للصفقة (من الخانة فقط — لا OFF)
        structure = get_trade_structure()
        cell0 = structure["tracks"][track_num]["cycles"][cycle_num]
        sim_flag = bool(cell0.get("simulated", is_simulation()))

        # --- Helper: DEBUG breakdown للرصد السريع ---
        async def _debug_post_funds(price_now: Optional[float], planned: float, funds_final: float, note: str = ""):
            if 'is_debug_funds' in globals() and is_debug_funds():
                try:
                    msg = (f"[DEBUG funds] {normalize_symbol(symbol)} | "
                           f"price={price_now if price_now is not None else 'N/A'} | "
                           f"qInc={quote_inc} | bInc={base_inc} | minBase={min_base} | "
                           f"planned={planned:.6f} | funds={funds_final:.6f} | sim={bool(sim_flag)} "
                           f"{('| ' + note) if note else ''}")
                    await send_notification_tc(msg, symbol=symbol, track_num=track_num, cycle_num=cycle_num)
                    if not sim_flag:
                        try:
                            accts = kucoin.get_accounts()
                            rows = []
                            for a in accts:
                                if (a.get('currency') or '').upper() == 'USDT':
                                    rows.append(f"{a.get('type','?'):6s} | avail={a.get('available','0')} | holds={a.get('holds','0')}")
                            if rows:
                                await send_notification_tc("DEBUG USDT breakdown:\n" + "\n".join(rows),
                                                           symbol=symbol, track_num=track_num, cycle_num=cycle_num)
                        except Exception as e:
                            await send_notification_tc(f"DEBUG breakdown error: {e}",
                                                       symbol=symbol, track_num=track_num, cycle_num=cycle_num)
                except Exception:
                    pass

        while True:
            # --- حارس: أوقف المراقبة إذا تغيّرت حالة الخانة/الصفقة نهائيًا ---
            try:
                struct_now = get_trade_structure()
                cell_now = (struct_now.get("tracks", {}).get(str(track_num), {}).get("cycles", {}) or {}).get(cycle_num)
                if not cell_now:
                    return
                st_now = (cell_now.get("status") or "").lower()
                if bought_price is None:
                    if st_now not in ("open", "reserved"):
                        return
                else:
                    if st_now != "buy":
                        return

                # تحقّق من TRADES_FILE: إذا أصبحت نهائية، أغلق الحلقة
                latest_state = None
                if os.path.exists(TRADES_FILE):
                    with open(TRADES_FILE, "r") as _f:
                        _td = json.load(_f) or {}
                    for _tr in _td.get("trades", []):
                        if (normalize_symbol(_tr.get("symbol", "")) == normalize_symbol(symbol)
                                and str(_tr.get("track_num")) == str(track_num)
                                and str(_tr.get("cycle_num")) == str(cycle_num)):
                            latest_state = (_tr.get("status") or "").lower()
                if latest_state in ("closed", "stopped", "drwn", "failed"):
                    return
            except Exception:
                pass

            price = await fetch_current_price(symbol)
            if price is None:
                if (time.time() - last_price_ok_ts) >= 600.0:
                    await send_notification_tc(
                        "⛔️ Canceled: لم يتم الحصول على سعر لمدة 10 دقائق. تم إلغاء الصفقة.",
                        symbol=symbol, track_num=track_num, cycle_num=cycle_num
                    )
                    await update_trade_status(symbol, 'failed', track_num=track_num, cycle_num=cycle_num)
                    await update_active_trades((track_num, cycle_num), {"symbol": normalize_symbol(symbol)}, final_status="failed")
                    return
                await asyncio.sleep(60)
                continue
            else:
                last_price_ok_ts = time.time()

            # =================== تنفيذ الشراء ===================
            if bought_price is None and price <= entry_price + EPS:
                try:
                    funds_planned = quantize_down(amount, quote_inc)
                    if funds_planned <= 0:
                        await send_notification_tc("⚠️ Funds too small.",
                                                   symbol=symbol, track_num=track_num, cycle_num=cycle_num)
                        await update_trade_status(symbol, 'failed', track_num=track_num, cycle_num=cycle_num)
                        await update_active_trades((track_num, cycle_num), {"symbol": normalize_symbol(symbol)}, final_status="failed")
                        return

                    available_usdt = get_trade_balance_usdt(sim_override=sim_flag)
                    await _debug_post_funds(price, funds_planned, funds_planned, note=f"avail_pre={available_usdt}")

                    if available_usdt <= 0:
                        await send_notification_tc("❌ Buy failed: USDT balance in Trading account is 0.",
                                                   symbol=symbol, track_num=track_num, cycle_num=cycle_num)
                        await _debug_post_funds(price, funds_planned, 0.0, note="avail==0")
                        await update_trade_status(symbol, 'failed', track_num=track_num, cycle_num=cycle_num)
                        await update_active_trades((track_num, cycle_num), {"symbol": normalize_symbol(symbol)}, final_status="failed")
                        return

                    funds = quantize_down(min(funds_planned, available_usdt), quote_inc)
                    if funds <= 0:
                        await send_notification_tc("❌ Buy failed: not enough USDT after quantization.",
                                                   symbol=symbol, track_num=track_num, cycle_num=cycle_num)
                        await _debug_post_funds(price, funds_planned, funds, note="funds_after_qtz<=0")
                        await update_trade_status(symbol, 'failed', track_num=track_num, cycle_num=cycle_num)
                        await update_active_trades((track_num, cycle_num), {"symbol": normalize_symbol(symbol)}, final_status="failed")
                        return

                    est_qty = quantize_down(funds / max(price, 1e-12), base_inc)
                    if est_qty < min_base:
                        min_funds_needed = (min_base * price)
                        await send_notification_tc(
                            (
                                "❌ Buy blocked: amount too small for pair min size.\n"
                                f"• est_qty={est_qty:.8f} < baseMinSize={min_base}\n"
                                f"• Approx min USDT needed: {min_funds_needed:.4f}"
                            ),
                            symbol=symbol, track_num=track_num, cycle_num=cycle_num
                        )
                        await _debug_post_funds(price, funds_planned, funds, note=f"est_qty={est_qty:.8f} < min")
                        await update_trade_status(symbol, 'failed', track_num=track_num, cycle_num=cycle_num)
                        await update_active_trades((track_num, cycle_num), {"symbol": normalize_symbol(symbol)}, final_status="failed")
                        return

                    order = place_market_order(pair, 'buy', funds=str(funds), symbol_hint=symbol, sim_override=sim_flag)
                    order_id = (order or {}).get("orderId")
                    if not order_id:
                        await send_notification_tc("❌ Buy error: no orderId returned.",
                                                   symbol=symbol, track_num=track_num, cycle_num=cycle_num)
                        await update_trade_status(symbol, 'failed', track_num=track_num, cycle_num=cycle_num)
                        await update_active_trades((track_num, cycle_num), {"symbol": normalize_symbol(symbol)}, final_status="failed")
                        return

                    await asyncio.sleep(1)
                    filled_qty, deal_funds = await get_order_deal_size(order_id, symbol=symbol, sim_override=sim_flag)
                    if filled_qty <= 0.0:
                        await send_notification_tc(
                            "❌ Buy issue: order executed but filled size = 0.\n"
                            f"🆔 orderId: {order_id}",
                            symbol=symbol, track_num=track_num, cycle_num=cycle_num
                        )
                        await update_trade_status(symbol, 'failed', track_num=track_num, cycle_num=cycle_num)
                        await update_active_trades((track_num, cycle_num), {"symbol": normalize_symbol(symbol)}, final_status="failed")
                        return

                    qty = filled_qty
                    bought_price = deal_funds / filled_qty
                    start_time = datetime.now(timezone.utc)

                    structure = get_trade_structure()
                    cell = structure["tracks"][track_num]["cycles"][cycle_num]
                    cell["status"] = "buy"
                    cell["start_time"] = start_time.isoformat()
                    cell["filled_qty"] = qty
                    cell["bought_price"] = bought_price
                    cell["simulated"] = bool(sim_flag)
                    save_trade_structure(structure)

                    if '_update_trade_exec_fields' in globals():
                        _update_trade_exec_fields(
                            normalize_symbol(symbol),
                            track_num, cycle_num,
                            bought_price=bought_price, sell_qty=qty
                        )

                    sim_tag = " (SIM)" if sim_flag else ""
                    await send_notification_tc(
                        (
                            f"✅ Bought{sim_tag}\n"
                            f"💰 Price: {bought_price:.6f}\n"
                            f"📦 Qty: {qty:.6f}\n"
                            f"💵 Amount: {amount:.2f} USDT"
                        ),
                        symbol=symbol, track_num=track_num, cycle_num=cycle_num
                    )

                except Exception as e:
                    await send_notification_tc(f"❌ Buy execution error: {e}",
                                               symbol=symbol, track_num=track_num, cycle_num=cycle_num)
                    await update_trade_status(symbol, 'failed', track_num=track_num, cycle_num=cycle_num)
                    await update_active_trades((track_num, cycle_num), {"symbol": normalize_symbol(symbol)}, final_status="failed")
                    return

            # =================== بعد الشراء: إدارة الخروج ===================
            if bought_price is not None:
                adjusted_qty = quantize_down(qty * 0.9998, base_inc)
                if adjusted_qty < min_base or adjusted_qty == 0.0:
                    await send_notification_tc("⚠️ Adjusted qty < min size. Cancel sell.",
                                               symbol=symbol, track_num=track_num, cycle_num=cycle_num)
                    await update_trade_status(symbol, 'failed', track_num=track_num, cycle_num=cycle_num)
                    await update_active_trades((track_num, cycle_num), {"symbol": normalize_symbol(symbol)}, final_status="failed")
                    break

                # -------- تقدّم الأهداف بدون بيع (TP ladder) --------
                progressed = False
                while (highest_idx + 1) < len(targets) and price >= targets[highest_idx + 1] - EPS:
                    highest_idx += 1
                    progressed = True
                    last_tp_floor = float(targets[highest_idx])

                if progressed:
                    # تفعيل Trailing-1% فور لمس TP1 (حماية الربح)
                    if not trailing_armed and price >= (tp1_val - EPS):
                        trailing_armed = True
                        max_after_touch = price
                        last_tp_floor = max(last_tp_floor or 0.0, tp1_val)
                        await send_notification_tc(
                            (
                                "🟢 Trailing-1% ARMED (on TP1 touch).\n"
                                f"• TP1: {tp1_val:.6f} | Price: {price:.6f}\n"
                                "• Floor ≥ last TP touched"
                            ),
                            symbol=symbol, track_num=track_num, cycle_num=cycle_num
                        )
                    else:
                        if trailing_armed:
                            if max_after_touch is None or price > max_after_touch:
                                max_after_touch = price
                            last_tp_floor = max(last_tp_floor or 0.0, float(targets[highest_idx]))

                    next_label = f"TP{highest_idx + 2}" if (highest_idx + 1) < len(targets) else "TRAILING-ONLY"
                    await send_notification_tc(
                        f"➡️ {normalize_symbol(symbol)} | T {track_num} | C {cycle_num} — touched TP{highest_idx+1} "
                        f"({float(targets[highest_idx]):.6f}); moving to {next_label}.",
                        symbol=symbol, track_num=track_num, cycle_num=cycle_num
                    )

                # -------- Trailing logic --------
                poll_sec = 60  # افتراضي قبل التفعيل
                if trailing_armed:
                    poll_sec = 10  # مراقبة أسرع بعد التفعيل

                    # حدّث القمّة
                    if max_after_touch is None or price > max_after_touch:
                        max_after_touch = price

                    # أرضية مضمونة ≥ آخر TP تم لمسه وإلا TP1
                    enforced_floor = max(float(last_tp_floor or 0.0), tp1_val)

                    # عتبة التريلينغ 1%
                    raw_trigger = (max_after_touch or price) * (1.0 - (RETRACE_PERCENT / 100.0))

                    try:
                        # (A) كسر الأرضية = بيع فوري
                        if price < enforced_floor - EPS:
                            sell_order = place_market_order(pair, 'sell', size=str(adjusted_qty),
                                                            symbol_hint=symbol, sim_override=sim_flag)
                            order_id = (sell_order or {}).get("orderId")
                            await asyncio.sleep(1)
                            sell_qty, deal_funds = await get_order_deal_size(order_id, symbol=symbol, sim_override=sim_flag) if order_id else (adjusted_qty, price * adjusted_qty)
                            sell_price = (deal_funds / sell_qty) if (sell_qty and sell_qty > 0) else price

                            if '_update_trade_exec_fields' in globals():
                                _update_trade_exec_fields(
                                    normalize_symbol(symbol),
                                    track_num, cycle_num,
                                    bought_price=bought_price, sell_price=sell_price, sell_qty=sell_qty
                                )

                            pnl = (sell_price - bought_price) * sell_qty
                            try:
                                if pnl >= 0:
                                    accumulate_summary(profit_delta=float(pnl))
                                else:
                                    accumulate_summary(loss_delta=float(-pnl))
                            except Exception:
                                pass

                            duration = datetime.now(timezone.utc) - start_time if start_time else None
                            duration_str = (f"{duration.days}d / {duration.seconds // 3600}h / {(duration.seconds % 3600) // 60}m") if duration else ""
                            pct = ((sell_price - bought_price) / max(bought_price, 1e-12)) * 100.0

                            await send_notification_tc(
                                (
                                    "🟥 Trailing exit: FLOOR BREACH\n"
                                    f"💵 PnL: {pnl:.4f} USDT  ({pct:+.2f}%)\n"
                                    f"{('⏱️ ' + duration_str) if duration_str else ''}"
                                ),
                                symbol=symbol, track_num=track_num, cycle_num=cycle_num
                            )

                            await update_trade_status(symbol, 'closed' if pnl >= 0 else 'drwn', track_num=track_num, cycle_num=cycle_num)
                            await update_active_trades((track_num, cycle_num), {"symbol": normalize_symbol(symbol)}, final_status=('closed' if pnl >= 0 else 'drwn'))
                            break

                        # (B) هبوط ≥1% عن القمّة مع البقاء فوق الأرضية = بيع تريلينغ
                        elif price <= raw_trigger + EPS and price >= enforced_floor - EPS:
                            sell_order = place_market_order(pair, 'sell', size=str(adjusted_qty),
                                                            symbol_hint=symbol, sim_override=sim_flag)
                            order_id = (sell_order or {}).get("orderId")
                            await asyncio.sleep(1)
                            sell_qty, deal_funds = await get_order_deal_size(order_id, symbol=symbol, sim_override=sim_flag) if order_id else (adjusted_qty, price * adjusted_qty)
                            sell_price = (deal_funds / sell_qty) if (sell_qty and sell_qty > 0) else price

                            if '_update_trade_exec_fields' in globals():
                                _update_trade_exec_fields(
                                    normalize_symbol(symbol),
                                    track_num, cycle_num,
                                    bought_price=bought_price, sell_price=sell_price, sell_qty=sell_qty
                                )

                            pnl = (sell_price - bought_price) * sell_qty
                            try:
                                if pnl >= 0:
                                    accumulate_summary(profit_delta=float(pnl))
                                else:
                                    accumulate_summary(loss_delta=float(-pnl))
                            except Exception:
                                pass

                            duration = datetime.now(timezone.utc) - start_time if start_time else None
                            duration_str = (f"{duration.days}d / {duration.seconds // 3600}h / {(duration.seconds % 3600) // 60}m") if duration else ""
                            pct = ((sell_price - bought_price) / max(bought_price, 1e-12)) * 100.0

                            await send_notification_tc(
                                (
                                    "🌟 Trailing exit: 1% from peak\n"
                                    f"💵 PnL: {pnl:.4f} USDT  ({pct:+.2f}%)\n"
                                    f"{('⏱️ ' + duration_str) if duration_str else ''}"
                                ),
                                symbol=symbol, track_num=track_num, cycle_num=cycle_num
                            )

                            await update_trade_status(symbol, 'closed' if pnl >= 0 else 'drwn', track_num=track_num, cycle_num=cycle_num)
                            await update_active_trades((track_num, cycle_num), {"symbol": normalize_symbol(symbol)}, final_status=('closed' if pnl >= 0 else 'drwn'))
                            break
                    except Exception as e:
                        await send_notification_tc(f"❌ Sell (trail) failed: {e}\n🕒 Check system time sync.",
                                                   symbol=symbol, track_num=track_num, cycle_num=cycle_num)
                        await update_trade_status(symbol, 'failed', track_num=track_num, cycle_num=cycle_num)
                        await update_active_trades((track_num, cycle_num), {"symbol": normalize_symbol(symbol)}, final_status="failed")
                        break

                # -------- SL: إشعار فقط بدون بيع --------
                if start_time is not None and not sl_alerted:
                    candle = get_latest_candle(symbol, interval='1hour')
                    now_ms = datetime.now(timezone.utc).timestamp() * 1000.0
                    interval_ms = _interval_to_ms('1hour')
                    if candle:
                        candle_start_ms = float(candle["timestamp"])
                        candle_end_ms = candle_start_ms + interval_ms
                        trade_start_ms = start_time.timestamp() * 1000.0

                        if (candle_end_ms <= now_ms and
                            candle_end_ms > trade_start_ms and
                            candle["close"] <= sl_price + EPS):
                            sl_alerted = True
                            await send_notification_tc(
                                (
                                    "🛑 SL touched (no sell).\n"
                                    "➡️ Continuing to monitor for TP1/targets."
                                ),
                                symbol=symbol, track_num=track_num, cycle_num=cycle_num
                            )

            # سرعة أخذ العينة
            await asyncio.sleep(poll_sec if 'poll_sec' in locals() else 60)

    except Exception as e:
        await send_notification_tc(f"⚠️ Monitor failed: {str(e)}",
                                   symbol=symbol, track_num=track_num, cycle_num=cycle_num)
        await update_trade_status(symbol, 'failed', track_num=track_num, cycle_num=cycle_num)
        await update_active_trades((track_num, cycle_num), {"symbol": normalize_symbol(symbol)}, final_status="failed")
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
    t = t.replace("\u200f", "").replace("\u200e", "")
    # symbol
    m_sym = re.search(r"#\s*([A-Z0-9\-_\/]+)", t, re.IGNORECASE)
    if not m_sym:
        raise ValueError("symbol not found")
    symbol = m_sym.group(1).upper().replace("-", "").replace("/", "")

    # buy
    m_buy = re.search(r"\bBUY\b\s*[-:]\s*([0-9]*\.?[0-9]+)", t, re.IGNORECASE)
    if not m_buy:
        raise ValueError("buy not found")
    entry = float(m_buy.group(1))

    # remove TP LONG lines before scanning
    t_clean = re.sub(r"TP\s*LONG.*", "", t, flags=re.IGNORECASE)
    # TPs
    tps_pairs = re.findall(r"\bTP\s*(\d+)\s*[-:]\s*([0-9]*\.?[0-9]+)", t_clean, re.IGNORECASE)
    tps_sorted = [float(val) for _, val in sorted(((int(n), v) for n, v in tps_pairs), key=lambda x: x[0])]

    # SL
    m_sl = re.search(r"\bSL\b\s*[-:]\s*([0-9]*\.?[0-9]+)", t, re.IGNORECASE)
    sl = float(m_sl.group(1)) if m_sl else 0.0

    return symbol, entry, tps_sorted, sl

# -- Channel: recommendations listener (Email Gate + Blacklist) --
def attach_channel_handler():
    if not CHANNEL_USERNAME or events is None:
        print("⚠️ CHANNEL_USERNAME not set or Telethon events unavailable; recommendations listener disabled.")
        return

    @client.on(events.NewMessage(chats=CHANNEL_USERNAME))
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

# ===== Commands on 'Saved Messages' =====
_pending_close_request = {"waiting": False}

@client.on(events.NewMessage(chats='me'))
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
# ============================================
# Section 6) Manual Close Monitor (FINAL, merged)
#  - لا بيع على TP بمجرد الملامسة: نُرقّي للهدف التالي
#  - Trailing يتفعّل فور لمس TP1 (حماية ربح مبكرة):
#       • أرضية = آخر TP مُلامس (≥ TP1 دائماً)
#       • بيع فوري عند كسر الأرضية (FLOOR BREACH)
#       • أو بيع عند هبوط ≥1% من القمّة مع بقاء السعر فوق الأرضية
#  - SL: إشعار فقط عند إغلاق شمعة 1h ≤ SL (لا بيع، يُستكمل البحث عن الأهداف)
#  - احترام وضع المحاكاة للخانات الموسومة simulated=True
#  - تحديث SUMMARY_FILE حسب الربح/الخسارة
#  - جميع الإشعارات عبر send_notification_tc (مع SYMBOL/T/C)
#  - حارس مبكّر: إيقاف المراقبة فور إغلاق الصفقة/تفريغ الخانة
#  - Polling: 60s قبل التفعيل، 10s بعد تفعيل التريلينغ
# ============================================

from datetime import datetime, timezone
from typing import List, Optional, Tuple
import asyncio, time, re, os, json

# ثوابت التريلينغ
RETRACE_PERCENT = 1.0     # هبوط 1% من القمّة
TP1_ARM_PCT     = 0.0     # <-- معدّل: تفعيل فوري عند لمس TP1
EPS             = 1e-9

async def manual_close_monitor(
    symbol: str,
    entry_price: float,
    sl_price: float,
    tp1: float,                     # TP1 مرجعي؛ بقية الأهداف تُقرأ من الخانة
    amount: float,
    track_num: str,
    cycle_num: str
):
    try:
        pair = format_symbol(symbol)
        meta = get_symbol_meta(pair)
        if not meta:
            await send_notification_tc(
                "❌ manual_close meta fetch failed.",
                symbol=symbol, track_num=track_num, cycle_num=cycle_num
            )
            return

        # قراءة الخانة + التأكد من أنها BUY وبها تنفيذ فعلي
        structure = get_trade_structure()
        cell = (structure.get("tracks", {}).get(str(track_num), {}).get("cycles", {}) or {}).get(cycle_num) or {}
        st0 = (cell.get("status") or "").lower()
        if st0 != "buy" or not cell.get("bought_price") or not cell.get("filled_qty"):
            await send_notification_tc(
                "ℹ️ Manual-close monitor skipped: no active BUY on this slot.",
                symbol=symbol, track_num=track_num, cycle_num=cycle_num
            )
            return

        # الأهداف
        targets: List[float] = list(cell.get("targets") or ([tp1] if tp1 else []))
        if targets and tp1 and abs(float(targets[0]) - float(tp1)) > 1e-12:
            targets[0] = float(tp1)
        if not targets:
            targets = [float(tp1)] if tp1 else []

        # محاكاة؟
        sim_override = bool(cell.get("simulated", is_simulation()))

        # بيانات تنفيذ الشراء
        bought_price = float(cell["bought_price"])
        qty          = float(cell["filled_qty"])
        try:
            start_time = datetime.fromisoformat(cell.get("start_time")).replace(tzinfo=timezone.utc) \
                         if cell.get("start_time") else None
        except Exception:
            start_time = None

        base_inc = meta['baseIncrement']
        min_base = meta['baseMinSize']

        # حالة الأهداف/التريلينغ
        def _get_progress_idx() -> int:
            try:
                i = int(cell.get("progress_target_idx", 0))
                return max(0, min(i, max(0, len(targets)-1)))
            except Exception:
                return 0

        def _set_progress_idx(i: int) -> None:
            try:
                s2 = get_trade_structure()
                c2 = (s2["tracks"][str(track_num)]["cycles"] or {})[cycle_num]
                c2["progress_target_idx"] = int(i)
                save_trade_structure(s2)
            except Exception:
                pass

        cur_idx = _get_progress_idx()

        trailing_active = bool(cell.get("trailing_active", False))
        peak_after_tp  = float(cell.get("trailing_peak", 0) or 0)
        last_tp_floor  = None  # أرضية = آخر TP مُلامس (≥ TP1)
        sl_alerted     = False

        def _persist_trailing(active: bool, peak: float, floor: Optional[float]):
            try:
                s2 = get_trade_structure()
                c2 = (s2["tracks"][str(track_num)]["cycles"] or {})[cycle_num]
                c2["trailing_active"] = bool(active)
                c2["trailing_peak"]   = float(peak)
                if floor is not None:
                    c2["last_tp_floor"] = float(floor)
                save_trade_structure(s2)
            except Exception:
                pass

        try:
            if cell.get("last_tp_floor") is not None:
                last_tp_floor = float(cell.get("last_tp_floor"))
        except Exception:
            last_tp_floor = None

        def _duration_str_from(start_dt: Optional[datetime]) -> str:
            try:
                if not start_dt: return ""
                delta = datetime.now(timezone.utc) - start_dt
                return f"{delta.days}d / {delta.seconds // 3600}h / {(delta.seconds % 3600)//60}m"
            except Exception:
                return ""

        async def _do_market_sell(exec_price_hint: Optional[float] = None) -> Tuple[float, float, float]:
            adj_qty = quantize_down(qty * 0.9998, base_inc)
            if adj_qty < min_base or adj_qty == 0.0:
                raise RuntimeError("adjusted qty below min size")
            sell_order = place_market_order(
                pair, 'sell', size=str(adj_qty), symbol_hint=symbol, sim_override=sim_override
            )
            order_id = (sell_order or {}).get("orderId")
            await asyncio.sleep(1)
            if order_id:
                sell_qty, deal_funds = await get_order_deal_size(order_id, symbol=symbol, sim_override=sim_override)
            else:
                sell_qty = adj_qty
                deal_funds = (exec_price_hint or bought_price) * adj_qty

            sell_price = (deal_funds / sell_qty) if (sell_qty and sell_qty > 0) else (exec_price_hint or bought_price)
            pnl = (sell_price - bought_price) * sell_qty

            if '_update_trade_exec_fields' in globals():
                _update_trade_exec_fields(
                    symbol.upper().replace('-', '').replace('/', ''),
                    track_num, cycle_num,
                    bought_price=bought_price, sell_price=sell_price, sell_qty=sell_qty
                )
            return sell_price, sell_qty, pnl

        async def _finalize(status: str, sell_price: float, sell_qty: float, pnl: float, tag: str):
            # Summary
            try:
                if pnl >= 0:
                    accumulate_summary(profit_delta=float(pnl))
                else:
                    accumulate_summary(loss_delta=float(-pnl))
            except Exception:
                pass

            dur_str = _duration_str_from(start_time)
            pct = ((sell_price - bought_price) / max(bought_price, 1e-12)) * 100.0

            if status == "closed":
                await send_notification_tc(
                    (
                        f"🟢 Manual close — {tag}\n"
                        f"💰 Buy: {bought_price:.6f} → Sell: {sell_price:.6f}\n"
                        f"📦 Qty: {sell_qty:.6f} | 💵 Amount: {amount:.2f} USDT\n"
                        f"💵 PnL: {pnl:.4f} USDT  ({pct:+.2f}%)\n"
                        f"{('⏱️ ' + dur_str) if dur_str else ''}"
                    ),
                    symbol=symbol, track_num=track_num, cycle_num=cycle_num
                )
            elif status == "stopped":
                # (SL الحقيقي فقط هو الذي يرجع 6 مسارات)
                current_track_idx = int(track_num)
                back_track_idx = max(1, current_track_idx - 6)

                s2 = get_trade_structure()
                if str(back_track_idx) not in s2["tracks"]:
                    s2["tracks"][str(back_track_idx)] = create_new_track(
                        back_track_idx, track_base_amount(back_track_idx)
                    )
                target_back_amount = s2["tracks"][str(back_track_idx)]["amount"]

                m = re.match(r"([A-Za-z]+)", str(cycle_num))
                cycle_label = m.group(1).upper() if m else str(cycle_num)

                await send_notification_tc(
                    (
                        f"🔴 Manual close — {tag}\n"
                        f"💰 Buy: {bought_price:.6f} → Sell: {sell_price:.6f}\n"
                        f"📦 Qty: {sell_qty:.6f} | 💵 Amount: {amount:.2f} USDT\n"
                        f"💵 PnL: {pnl:.4f} USDT  ({pct:+.2f}%)\n"
                        f"↩️ Back to Track {back_track_idx} (same letter {cycle_label})\n"
                        f"🎯 Target track base amount: {target_back_amount} USDT\n"
                        f"{('⏱️ ' + dur_str) if dur_str else ''}"
                    ),
                    symbol=symbol, track_num=track_num, cycle_num=cycle_num
                )
            elif status == "drwn":
                await send_notification_tc(
                    (
                        f"🔴 Manual close — {tag}\n"
                        f"💰 Buy: {bought_price:.6f} → Sell: {sell_price:.6f}\n"
                        f"📦 Qty: {sell_qty:.6f} | 💵 Amount: {amount:.2f} USDT\n"
                        f"💵 PnL: {pnl:.4f} USDT  ({pct:+.2f}%)\n"
                        f"{('⏱️ ' + dur_str) if dur_str else ''}"
                    ),
                    symbol=symbol, track_num=track_num, cycle_num=cycle_num
                )

            try:
                await update_trade_status(symbol, status, track_num=track_num, cycle_num=cycle_num)
            except Exception:
                await update_trade_status(symbol, status)
            await update_active_trades((track_num, cycle_num), {"symbol": symbol}, final_status=status)

        # ========= الحلقة =========
        while True:
            # حارس مبكّر
            try:
                struct_now = get_trade_structure()
                cell_now = (struct_now.get("tracks", {}).get(str(track_num), {}).get("cycles", {}) or {}).get(cycle_num)
                if not cell_now:
                    return
                st_now = (cell_now.get("status") or "").lower()
                if st_now != "buy":
                    return
                # TRADES_FILE
                latest_state = None
                if os.path.exists(TRADES_FILE):
                    with open(TRADES_FILE, "r") as _f:
                        _td = json.load(_f) or {}
                    for _tr in _td.get("trades", []):
                        if (normalize_symbol(_tr.get("symbol", "")) == normalize_symbol(symbol)
                                and str(_tr.get("track_num")) == str(track_num)
                                and str(_tr.get("cycle_num")) == str(cycle_num)):
                            latest_state = (_tr.get("status") or "").lower()
                if latest_state in ("closed", "stopped", "drwn", "failed"):
                    return
            except Exception:
                pass

            price = await fetch_current_price(symbol)
            if price is None:
                await asyncio.sleep(60)
                continue

            # 1) تسليح التريلينغ فور لمس TP1 (الأرضية ≥ TP1)
            if targets:
                tp1_val = float(targets[0])
                if not trailing_active and price >= tp1_val - EPS:
                    trailing_active = True
                    peak_after_tp = float(price)
                    last_tp_floor = max(last_tp_floor or 0.0, tp1_val)
                    _persist_trailing(True, peak_after_tp, last_tp_floor)
                    await send_notification_tc(
                        (
                            "🪝 Trailing-1% ARMED (on TP1 touch).\n"
                            f"• TP1: {tp1_val:.6f} | Peak: {peak_after_tp:.6f}\n"
                            "• Floor ≥ last TP touched"
                        ),
                        symbol=symbol, track_num=track_num, cycle_num=cycle_num
                    )

            # 2) ترقية الهدف عند الملامسة (بدون بيع)
            if targets and cur_idx < len(targets) and price >= float(targets[cur_idx]) - EPS:
                touched = cur_idx
                cur_idx = min(cur_idx + 1, len(targets))
                _set_progress_idx(cur_idx)
                last_tp_floor = max(float(targets[touched]), last_tp_floor or 0.0)
                _persist_trailing(trailing_active, peak_after_tp, last_tp_floor)
                await send_notification_tc(
                    f"➡️ {symbol} | T {track_num} | C {cycle_num} — touched TP{touched+1} "
                    f"({float(targets[touched]):.6f}); moving to "
                    f"{('TP'+str(cur_idx+1)) if cur_idx < len(targets) else 'TRAILING-ONLY'}.",
                    symbol=symbol, track_num=track_num, cycle_num=cycle_num
                )

            # 3) Trailing: بيع عند كسر الأرضية أو هبوط 1% من القمّة مع البقاء فوق الأرضية
            poll_sec = 60
            if trailing_active:
                poll_sec = 10
                if price > peak_after_tp:
                    peak_after_tp = float(price)
                    _persist_trailing(True, peak_after_tp, last_tp_floor)

                tp1_floor = float(targets[0]) if targets else float(tp1)
                enforced_floor = max(float(last_tp_floor or 0.0), tp1_floor)
                raw_trigger = (peak_after_tp or price) * (1.0 - RETRACE_PERCENT / 100.0)

                try:
                    # (A) كسر الأرضية = بيع فوري
                    if price < enforced_floor - EPS:
                        sell_price, sell_qty, pnl = await _do_market_sell(exec_price_hint=price)
                        status = "closed" if pnl >= 0 else "drwn"
                        await _finalize(status, sell_price, sell_qty, pnl, tag="Trailing FLOOR BREACH")
                        break

                    # (B) هبوط ≥1% من القمّة مع البقاء فوق الأرضية = بيع تريلينغ
                    elif price <= raw_trigger + EPS and price >= enforced_floor - EPS:
                        sell_price, sell_qty, pnl = await _do_market_sell(exec_price_hint=price)
                        status = "closed" if pnl >= 0 else "drwn"
                        await _finalize(status, sell_price, sell_qty, pnl, tag="Trailing 1%")
                        break
                except Exception as e:
                    await send_notification_tc(
                        f"❌ manual_close trailing sell failed\n🧰 {e}",
                        symbol=symbol, track_num=track_num, cycle_num=cycle_num
                    )
                    break

            # 4) SL: إشعار فقط (لا بيع) بعد إغلاق شمعة 1h ≤ SL
            if not sl_alerted:
                candle = get_latest_candle(symbol, interval='1hour')
                now_ms = datetime.now(timezone.utc).timestamp() * 1000.0
                if candle:
                    interval_ms = _interval_to_ms('1hour')
                    candle_start_ms = float(candle["timestamp"])
                    candle_end_ms = candle_start_ms + interval_ms
                    trade_start_ms = (start_time.timestamp() * 1000.0) if start_time else ((datetime.now(timezone.utc).timestamp() - 3600.0) * 1000.0)

                    if (candle_end_ms <= now_ms and
                        candle_end_ms > trade_start_ms and
                        candle["close"] <= sl_price + EPS):
                        sl_alerted = True
                        await send_notification_tc(
                            "🛑 SL touched (no sell). Continuing to monitor for targets.",
                            symbol=symbol, track_num=track_num, cycle_num=cycle_num
                        )

            await asyncio.sleep(poll_sec)

    except Exception as e:
        await send_notification_tc(
            f"⚠️ manual_close monitor crashed\n🧰 {e}",
            symbol=symbol, track_num=track_num, cycle_num=cycle_num
        )
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
