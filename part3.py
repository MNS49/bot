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
