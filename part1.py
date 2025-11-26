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
