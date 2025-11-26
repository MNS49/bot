# ============================================
# Section 4) Execution & Monitoring (UPDATED)
#      - TP ladder (no sell on touch; promote to next)
#      - Trailing-1% AFTER TP1 TOUCH (profit protection):
#          • أرضية = آخر TP مُلامس (≥ TP1 دائماً)
#          • بيع فوري عند كسر الأرضية (floor breach)
#          • أو بيع عند هبوط ≥1% من القمّة مع بقاء السعر فوق الأرضية
#      - Never sell below the last TP touched
#      - 1h-candle SL after buy time only (then back 6 tracks)
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
        "start_time": None
        # simulated: يُستخدم إن كانت الخانة موسومة سابقًا (توافقًا مع الماضي)
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

        # sim_flag: ثابت للصفقة (من الخانة فقط — لا OFF)
        structure = get_trade_structure()
        cell0 = structure["tracks"][track_num]["cycles"][cycle_num]
        sim_flag = bool(cell0.get("simulated", False))

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

                # -------- SL: بعد إغلاق شمعة 1h ≤ SL --------
                if start_time is not None:
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
                            try:
                                sell_order = place_market_order(pair, 'sell', size=str(adjusted_qty),
                                                                symbol_hint=symbol, sim_override=sim_flag)
                                order_id = (sell_order or {}).get("orderId")
                                await asyncio.sleep(1)

                                sell_qty, deal_funds = await get_order_deal_size(order_id, symbol=symbol, sim_override=sim_flag) if order_id else (adjusted_qty, candle["close"] * adjusted_qty)
                                sell_price = (deal_funds / sell_qty) if (sell_qty and sell_qty > 0) else candle["close"]

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

                                duration = datetime.now(timezone.utc) - start_time
                                duration_str = f"{duration.days}d / {duration.seconds // 3600}h / {(duration.seconds % 3600)//60}m"

                                current_track_idx = int(track_num)
                                back_track_idx = max(1, current_track_idx - 6)
                                structure2 = get_trade_structure()
                                if str(back_track_idx) not in structure2["tracks"]:
                                    structure2["tracks"][str(back_track_idx)] = create_new_track(back_track_idx, track_base_amount(back_track_idx))
                                target_back_amount = structure2["tracks"][str(back_track_idx)]["amount"]

                                m = re.match(r"([A-Za-z]+)", str(cycle_num))
                                cycle_label = m.group(1).upper() if m else str(cycle_num)

                                await send_notification_tc(
                                    (
                                        "🛑 SL hit (1h close):\n"
                                        f"💵 PnL: {pnl:.4f} USDT\n"
                                        f"⏱️ Duration: {duration_str}\n"
                                        f"↩️ Back to Track {back_track_idx} (same letter {cycle_label})\n"
                                        f"💵 Target track base amount: {target_back_amount} USDT"
                                    ),
                                    symbol=symbol, track_num=track_num, cycle_num=cycle_num
                                )

                                await update_trade_status(symbol, 'stopped', track_num=track_num, cycle_num=cycle_num)
                                await update_active_trades((track_num, cycle_num), {"symbol": normalize_symbol(symbol)}, final_status="stopped")
                                save_trade_structure(structure2)

                            except Exception as e:
                                await send_notification_tc(f"❌ Sell at SL failed: {e}",
                                                           symbol=symbol, track_num=track_num, cycle_num=cycle_num)
                                await update_trade_status(symbol, 'failed', track_num=track_num, cycle_num=cycle_num)
                                await update_active_trades((track_num, cycle_num), {"symbol": normalize_symbol(symbol)}, final_status="failed")
                            break

            # سرعة أخذ العينة
            await asyncio.sleep(poll_sec if 'poll_sec' in locals() else 60)

    except Exception as e:
        await send_notification_tc(f"⚠️ Monitor failed: {str(e)}",
                                   symbol=symbol, track_num=track_num, cycle_num=cycle_num)
        await update_trade_status(symbol, 'failed', track_num=track_num, cycle_num=cycle_num)
        await update_active_trades((track_num, cycle_num), {"symbol": normalize_symbol(symbol)}, final_status="failed")
