# ============================================
# Section 6) Manual Close Monitor (FINAL, merged)
#  - لا بيع على TP بمجرد الملامسة: نُرقّي للهدف التالي
#  - Trailing يتفعّل فور لمس TP1 (حماية ربح مبكرة):
#       • أرضية = آخر TP مُلامس (≥ TP1 دائماً)
#       • بيع فوري عند كسر الأرضية (FLOOR BREACH)
#       • أو بيع عند هبوط ≥1% من القمّة مع بقاء السعر فوق الأرضية
#  - SL الحقيقي الوحيد: بعد إغلاق شمعة 1h ≤ SL (بعد وقت الشراء) ⇒ رجوع 6 مسارات
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
        sim_override = bool(cell.get("simulated", False))

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

            # 4) SL: بعد إغلاق شمعة 1h ≤ SL (بعد وقت الشراء)
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
                    try:
                        sell_price, sell_qty, pnl = await _do_market_sell(exec_price_hint=candle["close"])
                        await _finalize("stopped", sell_price, sell_qty, pnl, tag="SL (1h close)")
                    except Exception as e:
                        await send_notification_tc(
                            f"❌ manual_close SL sell failed\n🧰 {e}",
                            symbol=symbol, track_num=track_num, cycle_num=cycle_num
                        )
                    break

            await asyncio.sleep(poll_sec)

    except Exception as e:
        await send_notification_tc(
            f"⚠️ manual_close monitor crashed\n🧰 {e}",
            symbol=symbol, track_num=track_num, cycle_num=cycle_num
        )
