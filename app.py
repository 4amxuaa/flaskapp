#!/usr/bin/env python3
"""
TkChart 后端服务 - 模拟版
本地生成行情 + 模拟交易 + 止损止盈 + 挂单追单 + WebSocket 通信
"""

import json
import os
import random
import threading
import time
from datetime import datetime
from typing import Dict, Set, List

from flask import Flask, render_template, jsonify, request
from flask_sock import Sock
from flask_cors import CORS


# ============ 配置 ============
class Config:
    INSTRUMENTS = [
        {"sym": "IM2410", "name": "中证100", "bp": 3700, "tick": 0.02, "margin_rate": 0.2, "contract_size": 300},
        {"sym": "IF2410", "name": "沪深300", "bp": 4200, "tick": 0.02, "margin_rate": 0.2, "contract_size": 300},
        {"sym": "rb2410", "name": "螺纹钢", "bp": 3700, "tick": 1, "margin_rate": 0.1, "contract_size": 10},
        {"sym": "ag2412", "name": "白银", "bp": 7800, "tick": 1, "margin_rate": 0.12, "contract_size": 15},
        {"sym": "au2412", "name": "黄金", "bp": 620, "tick": 0.02, "margin_rate": 0.08, "contract_size": 1000},
        {"sym": "sc2412", "name": "原油", "bp": 580, "tick": 0.1, "margin_rate": 0.15, "contract_size": 1000},
    ]
    INITIAL_BALANCE = 892355.0
    KLINE_INTERVAL = 2


# ============ Flask ============
app = Flask(__name__)
CORS(app)
sock = Sock(app)

clients: Set = set()
lock = threading.Lock()

# 数据缓存
klines: Dict[str, list] = {}
quotes: Dict[str, dict] = {}
account_info = {"balance": Config.INITIAL_BALANCE, "available": Config.INITIAL_BALANCE, "margin": 0.0, "pnl": 0.0}
positions: Dict[str, dict] = {}
pending_orders: Dict[str, dict] = {}
trade_history: List[dict] = []
instrument_configs: Dict[str, dict] = {}
price_params: Dict[str, dict] = {}
order_counter = 0


# ============ 工具 ============
def broadcast(msg: dict):
    text = json.dumps(msg, ensure_ascii=False)
    dead = set()
    with lock:
        for ws in clients:
            try:
                ws.send(text)
            except Exception:
                dead.add(ws)
        clients.difference_update(dead)


def broadcast_orders():
    """广播当前所有挂单/追单状态给所有客户端"""
    orders_list = []
    for ref, info in pending_orders.items():
        orders_list.append({
            "id": info.get("client_id", ref),
            "symbol": info["symbol"],
            "side": info["side"],
            "price": info["price"],
            "sl": info.get("sl"),
            "tp": info.get("tp"),
            "type": info.get("type", "limit"),
            "active": info.get("active", True),
            "volume": info.get("volume", 1),
        })
    broadcast({"type": "orders_sync", "data": orders_list})


def get_instrument(symbol: str) -> dict:
    if symbol not in instrument_configs:
        for inst in Config.INSTRUMENTS:
            if inst["sym"] == symbol:
                instrument_configs[symbol] = inst
                break
    return instrument_configs.get(symbol)


def calc_margin(sym, price, vol):
    inst = get_instrument(sym)
    if not inst:
        return 0
    return price * vol * inst["contract_size"] * inst["margin_rate"]


def calc_pnl(sym, side, entry, cur, vol):
    inst = get_instrument(sym)
    if not inst:
        return 0
    if side == "long":
        return (cur - entry) * vol * inst["contract_size"]
    return (entry - cur) * vol * inst["contract_size"]


def update_account():
    total_margin = 0.0
    total_pnl = 0.0
    for sym, pos in positions.items():
        cur = quotes.get(sym, {}).get("last_price", 0)
        for sk in ["long", "short"]:
            if pos.get(sk) and pos[sk].get("price"):
                p = pos[sk]
                total_margin += calc_margin(sym, p["price"], p.get("volume", 1))
                total_pnl += calc_pnl(sym, sk, p["price"], cur, p.get("volume", 1))
    account_info["margin"] = total_margin
    account_info["pnl"] = total_pnl
    account_info["available"] = account_info["balance"] + total_pnl - total_margin


def next_oid():
    global order_counter
    order_counter += 1
    return f"ORD{order_counter:06d}"


# ============ 止损止盈检查 ============
def check_sltp(sym, price):
    if sym not in positions:
        return
    for sk in ["long", "short"]:
        pos = positions[sym].get(sk)
        if not pos or not pos.get("price"):
            continue
        sl, tp = pos.get("sl"), pos.get("tp")
        trig = False
        ttype = ""
        if sk == "long":
            if sl is not None and price <= sl:
                trig = True
                ttype = "止损"
            elif tp is not None and price >= tp:
                trig = True
                ttype = "止盈"
        else:
            if sl is not None and price >= sl:
                trig = True
                ttype = "止损"
            elif tp is not None and price <= tp:
                trig = True
                ttype = "止盈"
        if trig:
            exit_p = sl if ttype == "止损" else tp
            pnl = calc_pnl(sym, sk, pos["price"], exit_p, pos.get("volume", 1))
            trade_history.append({
                "time": datetime.now().strftime("%H:%M:%S"),
                "symbol": sym,
                "side": sk,
                "action": f"{ttype}触发",
                "price": exit_p,
                "volume": pos.get("volume", 1),
            })
            broadcast({"type": "trade", "data": trade_history[-1]})
            account_info["balance"] += pnl
            positions[sym][sk] = None
            broadcast({"type": "position", "data": positions})
            update_account()
            broadcast({"type": "account", "data": account_info})
            print(f"[SLTP] {sym} {sk} {ttype}触发 @ {exit_p:.2f} 盈亏:{pnl:.2f}")


# ============ 挂单/追单触发检查 ============
def check_orders(sym, price):
    triggered = []
    for ref, info in list(pending_orders.items()):
        if info["symbol"] != sym:
            continue
        hit = False
        if info["type"] == "limit":
            if info["side"] == "long" and price <= info["price"]:
                hit = True
            elif info["side"] == "short" and price >= info["price"]:
                hit = True
        elif info["type"] == "chase":
            if info["side"] == "long" and price >= info["price"]:
                hit = True
            elif info["side"] == "short" and price <= info["price"]:
                hit = True
        if hit:
            triggered.append((ref, info))

    for ref, info in triggered:
        if sym not in positions:
            positions[sym] = {"long": None, "short": None}
        key = "long" if info["side"] == "long" else "short"
        positions[sym][key] = {
            "price": info["price"],
            "volume": info.get("volume", 1),
            "sl": info.get("sl"),
            "tp": info.get("tp"),
        }
        trade_history.append({
            "time": datetime.now().strftime("%H:%M:%S"),
            "symbol": sym,
            "side": info["side"],
            "action": f"{'挂单' if info['type'] == 'limit' else '追单'}触发",
            "price": info["price"],
            "volume": info.get("volume", 1),
        })
        broadcast({"type": "trade", "data": trade_history[-1]})
        broadcast({"type": "position", "data": positions})
        del pending_orders[ref]
        update_account()
        broadcast({"type": "account", "data": account_info})
        broadcast_orders()
        print(f"[ORDER] {info['type']}触发: {sym} {info['side']} @ {info['price']:.2f}")


# ============ 行情线程 ============
def market_thread():
    print("[行情] 启动...")
    for inst in Config.INSTRUMENTS:
        sym = inst["sym"]
        klines[sym] = []
        quotes[sym] = {
            "last_price": inst["bp"],
            "bid": inst["bp"] - inst["tick"],
            "ask": inst["bp"] + inst["tick"],
        }
        instrument_configs[sym] = inst
        price_params[sym] = {
            "price": inst["bp"],
            "trend": random.choice([-1, 1]),
            "volatility": inst["bp"] * 0.002,
        }

    # 历史K线
    for inst in Config.INSTRUMENTS:
        sym = inst["sym"]
        pp = price_params[sym]
        price = inst["bp"]
        now = time.time()
        for i in range(300, 0, -1):
            bt = now - i * Config.KLINE_INTERVAL
            ch = random.gauss(0, pp["volatility"])
            o, c = price, price + ch
            h = max(o, c) + abs(random.gauss(0, pp["volatility"] * 0.5))
            l = min(o, c) - abs(random.gauss(0, pp["volatility"] * 0.5))
            v = random.randint(100, 5000)
            klines[sym].append({
                "start_time": datetime.fromtimestamp(bt).isoformat(),
                "open": round(o, 2),
                "high": round(h, 2),
                "low": round(l, 2),
                "close": round(c, 2),
                "volume": v,
                "completed": True,
            })
            price = c
        pp["price"] = price
        quotes[sym]["last_price"] = round(price, 2)
        print(f"[行情] {sym} {len(klines[sym])}根K线, 最新:{price:.2f}")

    print("[行情] 实时推送...")
    last_bar = time.time()
    while True:
        time.sleep(3.5)
        now = time.time()
        new_bar = (now - last_bar) >= Config.KLINE_INTERVAL
        for inst in Config.INSTRUMENTS:
            sym = inst["sym"]
            pp = price_params[sym]
            if random.random() < 0.02:
                pp["trend"] = random.choice([-1, 1])
            tc = random.gauss(pp["trend"] * inst["tick"] * 0.3, pp["volatility"] * 0.1)
            np_ = max(inst["bp"] * 0.85, min(inst["bp"] * 1.15, pp["price"] + tc))
            pp["price"] = np_
            quotes[sym]["last_price"] = round(np_, 2)
            quotes[sym]["bid"] = round(np_ - inst["tick"], 2)
            quotes[sym]["ask"] = round(np_ + inst["tick"], 2)

            if new_bar:
                last_bar = now
                o = klines[sym][-1]["close"] if klines[sym] else np_
                h = max(o, np_) + abs(random.gauss(0, pp["volatility"] * 0.3))
                l = min(o, np_) - abs(random.gauss(0, pp["volatility"] * 0.3))
                v = random.randint(100, 5000)
                bar = {
                    "start_time": datetime.fromtimestamp(now).isoformat(),
                    "open": round(o, 2),
                    "high": round(max(o, np_, h), 2),
                    "low": round(min(o, np_, l), 2),
                    "close": round(np_, 2),
                    "volume": v,
                    "completed": True,
                }
                klines[sym].append(bar)
                if len(klines[sym]) > 500:
                    klines[sym] = klines[sym][-500:]
            elif klines[sym]:
                bar = klines[sym][-1]
                bar["high"] = round(max(bar["high"], np_), 2)
                bar["low"] = round(min(bar["low"], np_), 2)
                bar["close"] = round(np_, 2)
                bar["volume"] += random.randint(10, 200)

            if klines[sym]:
                broadcast({
                    "type": "kline",
                    "symbol": sym,
                    "name": inst["name"],
                    "data": klines[sym][-1],
                })

            check_sltp(sym, np_)
            check_orders(sym, np_)

        update_account()
        broadcast({"type": "account", "data": account_info})


# ============ 启动后台线程（模块级启动，兼容 gunicorn） ============
_market_started = False
_market_lock = threading.Lock()


def start_market_thread():
    global _market_started
    with _market_lock:
        if _market_started:
            return
        _market_started = True
    t = threading.Thread(target=market_thread, daemon=True, name="market")
    t.start()
    print("[启动] 行情线程已拉起")


# ★ 模块被 import 时即启动（gunicorn 部署场景）
start_market_thread()


# ============ WebSocket ============
@sock.route('/ws')
def ws_handler(ws):
    with lock:
        clients.add(ws)
    print(f"[WS] 连接, 总数:{len(clients)}")

    # 发送初始数据
    ws.send(json.dumps({
        "type": "instruments",
        "data": [{"sym": i["sym"], "name": i["name"], "tick": i["tick"]} for i in Config.INSTRUMENTS],
    }))
    for sym, bars in klines.items():
        inst = get_instrument(sym)
        for bar in bars[-200:]:
            ws.send(json.dumps({
                "type": "kline",
                "symbol": sym,
                "name": inst["name"] if inst else sym,
                "data": bar,
            }))
    ws.send(json.dumps({"type": "account", "data": account_info}))
    ws.send(json.dumps({"type": "position", "data": positions}))
    orders_list = [{
        "id": info.get("client_id", ref),
        "symbol": info["symbol"],
        "side": info["side"],
        "price": info["price"],
        "sl": info.get("sl"),
        "tp": info.get("tp"),
        "type": info.get("type", "limit"),
        "active": info.get("active", True),
        "volume": info.get("volume", 1),
    } for ref, info in pending_orders.items()]
    ws.send(json.dumps({"type": "orders_sync", "data": orders_list}))

    try:
        while True:
            data = ws.receive()
            if data is None:
                break
            try:
                handle_cmd(json.loads(data), ws)
            except json.JSONDecodeError:
                pass
    except Exception as e:
        print(f"[WS] 异常:{e}")
    finally:
        with lock:
            clients.discard(ws)
        print(f"[WS] 断开, 总数:{len(clients)}")
    return ''


def handle_cmd(msg: dict, ws):
    action = msg.get("action")
    symbol = msg.get("symbol", "rb2410")
    side = msg.get("side", "long")
    price = msg.get("price", 0)
    volume = msg.get("volume", 1)
    inst = get_instrument(symbol)
    if not inst:
        ws.send(json.dumps({"type": "error", "msg": f"未知合约:{symbol}"}))
        return
    cur = quotes.get(symbol, {}).get("last_price", price) or price

    # ---- 开仓 ----
    if action == "open":
        if symbol not in positions:
            positions[symbol] = {"long": None, "short": None}
        key = "long" if side == "long" else "short"
        if positions[symbol].get(key):
            ws.send(json.dumps({"type": "error", "msg": f"已有{side}持仓"}))
            return
        op = cur if cur > 0 else price
        positions[symbol][key] = {"price": op, "volume": volume, "sl": msg.get("sl"), "tp": msg.get("tp")}
        trade_history.append({
            "time": datetime.now().strftime("%H:%M:%S"),
            "symbol": symbol,
            "side": side,
            "action": "开仓",
            "price": op,
            "volume": volume,
        })
        broadcast({"type": "trade", "data": trade_history[-1]})
        broadcast({"type": "position", "data": positions})
        update_account()
        broadcast({"type": "account", "data": account_info})
        ws.send(json.dumps({"type": "info", "msg": f"开{side}仓:{symbol}@{op:.2f}"}))

    # ---- 平仓 ----
    elif action == "close":
        if symbol not in positions:
            ws.send(json.dumps({"type": "error", "msg": "无持仓"}))
            return
        key = "long" if side == "long" else "short"
        pos = positions[symbol].get(key)
        if not pos or not pos.get("price"):
            ws.send(json.dumps({"type": "error", "msg": f"无{side}持仓"}))
            return
        ep = cur if cur > 0 else price
        pnl = calc_pnl(symbol, side, pos["price"], ep, pos.get("volume", 1))
        trade_history.append({
            "time": datetime.now().strftime("%H:%M:%S"),
            "symbol": symbol,
            "side": side,
            "action": "平仓",
            "price": ep,
            "volume": pos.get("volume", 1),
        })
        account_info["balance"] += pnl
        positions[symbol][key] = None
        broadcast({"type": "trade", "data": trade_history[-1]})
        broadcast({"type": "position", "data": positions})
        update_account()
        broadcast({"type": "account", "data": account_info})
        ws.send(json.dumps({"type": "info", "msg": f"平{side}仓:{symbol}@{ep:.2f} 盈亏:{pnl:+.2f}"}))

    # ---- 挂单/追单 ----
    elif action == "order":
        otype = msg.get("order_type", "limit")
        cid = msg.get("order_id", f"o{int(time.time() * 1000)}")
        oid = next_oid()
        pending_orders[oid] = {
            "symbol": symbol,
            "side": side,
            "price": price,
            "sl": msg.get("sl"),
            "tp": msg.get("tp"),
            "type": otype,
            "client_id": cid,
            "volume": volume,
            "active": True,
        }
        broadcast_orders()
        ws.send(json.dumps({"type": "info", "msg": f"{'挂单' if otype == 'limit' else '追单'}:{symbol} {side}@{price:.2f}"}))

    # ---- 更新止盈止损 ----
    elif action == "update_sltp":
        if symbol not in positions:
            ws.send(json.dumps({"type": "error", "msg": "无持仓"}))
            return
        key = "long" if side == "long" else "short"
        pos = positions[symbol].get(key)
        if not pos or not pos.get("price"):
            ws.send(json.dumps({"type": "error", "msg": f"无{side}持仓"}))
            return
        pos["sl"] = msg.get("sl")
        pos["tp"] = msg.get("tp")
        broadcast({"type": "position", "data": positions})
        ws.send(json.dumps({"type": "info", "msg": f"止盈止损已更新:{side} 损{pos['sl']} 盈{pos['tp']}"}))

    # ---- 更新挂单/追单（拖拽后） ----
    elif action == "update_order":
        cid = msg.get("order_id")
        for ref, info in list(pending_orders.items()):
            if info.get("client_id") == cid:
                del pending_orders[ref]
                break
        oid = next_oid()
        pending_orders[oid] = {
            "symbol": symbol,
            "side": side,
            "price": msg.get("price"),
            "sl": msg.get("sl"),
            "tp": msg.get("tp"),
            "type": msg.get("type", "limit"),
            "client_id": cid,
            "volume": volume,
            "active": True,
        }
        broadcast_orders()
        ws.send(json.dumps({"type": "info", "msg": f"订单已更新:{symbol} {side}@{msg.get('price')}"}))

    # ---- 取消订单 ----
    elif action == "cancel_order":
        cid = msg.get("order_id")
        for ref, info in list(pending_orders.items()):
            if info.get("client_id") == cid:
                del pending_orders[ref]
                break
        broadcast_orders()
        ws.send(json.dumps({"type": "info", "msg": "订单已取消"}))

    # ---- 全平 ----
    elif action == "close_all":
        for sym, pos in positions.items():
            cp = quotes.get(sym, {}).get("last_price", 0)
            for sk in ["long", "short"]:
                if pos.get(sk) and pos[sk].get("price"):
                    p = pos[sk]
                    pnl = calc_pnl(sym, sk, p["price"], cp, p.get("volume", 1))
                    trade_history.append({
                        "time": datetime.now().strftime("%H:%M:%S"),
                        "symbol": sym,
                        "side": sk,
                        "action": "全平",
                        "price": cp,
                        "volume": p.get("volume", 1),
                    })
                    broadcast({"type": "trade", "data": trade_history[-1]})
                    account_info["balance"] += pnl
                    pos[sk] = None
        broadcast({"type": "position", "data": positions})
        update_account()
        broadcast({"type": "account", "data": account_info})
        ws.send(json.dumps({"type": "info", "msg": "全部平仓"}))

    # ---- 清除挂单 ----
    elif action == "clear_orders":
        pending_orders.clear()
        broadcast_orders()
        ws.send(json.dumps({"type": "info", "msg": "所有挂单已清除"}))

html = """
<!DOCTYPE html>
<html lang="zh-CN">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=no, viewport-fit=cover">
  <title>TkChart · 交易终端</title>
  <link rel="dns-prefetch" href="https://unpkg.com">
  <link rel="preconnect" href="https://unpkg.com" crossorigin>
  <style>
    :root {
      --bg:        #0a0e15;
      --bg-elev:   #0f1622;
      --panel:     #111a28;
      --panel-2:   #16202f;
      --control:   #1b2738;
      --control-h: #24344b;
      --border:    #223047;
      --border-soft: #1a2536;
      --text:      #e4e9f1;
      --text-2:    #93a1b5;
      --text-3:    #5f6e84;
      --accent:    #4d9fff;
      --accent-dim:rgba(77,159,255,0.18);
      --long:      #f25767;   /* 多 / 阳 (红) */
      --long-soft: rgba(242,87,103,0.14);
      --short:     #2ec27e;   /* 空 / 阴 (绿) */
      --short-soft:rgba(46,194,126,0.14);
      --sl:        #e0a93a;   /* 止损 */
      --tp:        #2bb7c4;   /* 止盈 */
      --chase:     #8a7cf0;   /* 追单 */
      --shadow:    0 10px 34px rgba(0,0,0,0.55);
      --radius:    8px;
    }

    * { margin: 0; padding: 0; box-sizing: border-box; }

    html, body { background: var(--bg); }

    body {
      font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', 'PingFang SC', 'Microsoft YaHei', sans-serif;
      color: var(--text);
      user-select: none;
      overflow: hidden;
      height: 100dvh;
      font-size: 12px;
      letter-spacing: 0.1px;
      touch-action: manipulation;
      -webkit-tap-highlight-color: transparent;
      -webkit-font-smoothing: antialiased;
    }

    .num { font-variant-numeric: tabular-nums; font-feature-settings: "tnum"; }

    .app {
      display: flex;
      flex-direction: column;
      height: 100%;
      position: relative;
    }
    /* 顶部品牌强调线 */
    .app::before {
      content: "";
      position: absolute; top: 0; left: 0; right: 0; height: 2px;
      background: linear-gradient(90deg, var(--accent), #2bb7c4 55%, transparent);
      opacity: 0.8; z-index: 40;
    }

    /* ====== 顶部栏 ====== */
    .topbar {
      display: flex;
      align-items: center;
      min-height: 46px;
      padding: 6px 14px;
      background: linear-gradient(180deg, var(--bg-elev), var(--panel));
      border-bottom: 1px solid var(--border);
      flex-shrink: 0;
      gap: 12px;
      z-index: 15;
    }

    .brand { display: flex; align-items: center; gap: 9px; flex-shrink: 0; }
    .brand .logo {
      width: 28px; height: 28px; border-radius: 7px;
      display: grid; place-items: center;
      background: linear-gradient(150deg, var(--accent), #2bb7c4);
      color: #04121f; font-weight: 800; font-size: 13px;
      box-shadow: 0 4px 12px rgba(77,159,255,0.35);
    }
    .brand .title { display: flex; flex-direction: column; line-height: 1.1; }
    .brand .title b { font-size: 14px; font-weight: 700; letter-spacing: 0.4px; }
    .brand .title small { font-size: 9px; color: var(--text-3); letter-spacing: 1px; text-transform: uppercase; }

    .conn {
      display: flex; align-items: center; gap: 6px;
      padding: 4px 9px; border-radius: 999px;
      background: var(--control); border: 1px solid var(--border);
      font-size: 10px; color: var(--text-2); flex-shrink: 0;
    }
    .conn .ws-dot { width: 7px; height: 7px; border-radius: 50%; flex-shrink: 0; transition: background .2s; }
    .conn .ws-dot.online  { background: var(--short); box-shadow: 0 0 0 3px var(--short-soft); }
    .conn .ws-dot.offline { background: var(--long);  box-shadow: 0 0 0 3px var(--long-soft); }

    /* 当前合约 + 价格 */
    .ticker { display: flex; align-items: baseline; gap: 10px; flex-shrink: 0; }
    .ticker .name { font-size: 13px; font-weight: 600; color: var(--text); }
    .ticker .name span { color: var(--text-3); font-size: 10px; margin-left: 4px; font-weight: 400; }
    .ticker .price { font-size: 20px; font-weight: 700; color: #fff; }
    .ticker .price.up { color: var(--long); }
    .ticker .price.dn { color: var(--short); }

    .spacer { flex: 1; }

    /* 账户统计芯片 */
    .stats { display: flex; gap: 8px; flex-shrink: 0; }
    .stat {
      display: flex; flex-direction: column; align-items: flex-end;
      padding: 4px 11px; border-radius: 7px;
      background: var(--control); border: 1px solid var(--border);
      min-width: 80px;
    }
    .stat .lbl { font-size: 9px; color: var(--text-3); text-transform: uppercase; letter-spacing: .6px; }
    .stat .val { font-size: 13px; font-weight: 600; color: var(--text); margin-top: 1px; }
    .stat .val.up { color: var(--short); }
    .stat .val.dn { color: var(--long); }
    @media (max-width: 980px) { .stats .stat.opt { display: none; } }
    @media (max-width: 767px) {
      .stats { display: none; }
      .ticker .price { font-size: 16px; }
      .brand .title small { display: none; }
    }

    /* ====== 工具栏 ====== */
    .toolbar {
      display: flex; align-items: center; gap: 6px;
      padding: 7px 14px;
      background: var(--panel);
      border-bottom: 1px solid var(--border-soft);
      flex-shrink: 0;
      flex-wrap: wrap;
      z-index: 14;
    }
    .group { display: flex; align-items: center; gap: 4px; padding: 3px; background: var(--bg-elev); border: 1px solid var(--border-soft); border-radius: 8px; }
    .sep { width: 1px; height: 20px; background: var(--border); margin: 0 2px; }

    .btn {
      height: 28px; padding: 0 13px;
      border: 1px solid transparent; border-radius: 6px;
      cursor: pointer; font-size: 11px; font-weight: 600; font-family: inherit;
      background: transparent; color: var(--text-2);
      white-space: nowrap; transition: background .12s, color .12s, border-color .12s, transform .05s;
    }
    .btn:hover { background: var(--control); color: var(--text); }
    .btn:active { transform: scale(0.96); }
    .btn.on { color: #fff; background: var(--accent-dim); border-color: rgba(77,159,255,0.5); }

    .btn-long  { background: var(--long);  color: #fff; box-shadow: 0 2px 8px rgba(242,87,103,0.28); }
    .btn-long:hover  { background: #ff6a79; color: #fff; }
    .btn-short { background: var(--short); color: #04140d; box-shadow: 0 2px 8px rgba(46,194,126,0.28); }
    .btn-short:hover { background: #38d690; color: #04140d; }

    .btn-ghost { border-color: var(--border); }
    .btn-ob { color: var(--long);  border-color: rgba(242,87,103,0.4); }
    .btn-ob:hover { background: var(--long-soft); }
    .btn-os { color: var(--short); border-color: rgba(46,194,126,0.4); }
    .btn-os:hover { background: var(--short-soft); }
    .btn-cb { color: var(--chase); border-color: rgba(138,124,240,0.4); }
    .btn-cb:hover { background: rgba(138,124,240,0.14); }
    .btn-cs { color: var(--tp); border-color: rgba(43,183,196,0.4); }
    .btn-cs:hover { background: rgba(43,183,196,0.14); }

    .qty { display: flex; align-items: center; gap: 6px; margin-left: auto; color: var(--text-3); font-size: 10px; }
    .qty label { text-transform: uppercase; letter-spacing: .5px; }
    .qty input {
      width: 58px; height: 28px; text-align: center;
      background: var(--bg-elev); border: 1px solid var(--border); border-radius: 6px;
      color: #fff; font-size: 12px; font-weight: 600; font-family: inherit;
      font-variant-numeric: tabular-nums;
    }
    .qty input:focus { outline: none; border-color: var(--accent); }
    .qty .hint { color: var(--text-3); }
    @media (max-width: 767px) {
      .toolbar .pc-only { display: none; }
      .qty { margin-left: 0; }
    }

    /* ====== 品种条 ====== */
    .symbol-bar {
      display: flex; align-items: stretch;
      height: 34px; padding: 0 8px;
      background: var(--bg-elev);
      border-bottom: 1px solid var(--border);
      overflow-x: auto; flex-shrink: 0;
      -webkit-overflow-scrolling: touch;
    }
    .symbol-bar::-webkit-scrollbar { display: none; }
    .symbol-bar .sym {
      display: flex; align-items: center; gap: 6px;
      padding: 0 14px; cursor: pointer;
      font-size: 11px; color: var(--text-2); white-space: nowrap;
      border-bottom: 2px solid transparent; flex-shrink: 0;
      transition: color .12s, background .12s;
    }
    .symbol-bar .sym:hover { color: var(--text); background: rgba(255,255,255,0.02); }
    .symbol-bar .sym.active { color: #fff; border-bottom-color: var(--accent); background: rgba(77,159,255,0.06); }
    .symbol-bar .sym .s-price { font-weight: 600; font-size: 10px; color: var(--text-3); font-variant-numeric: tabular-nums; }
    .symbol-bar .sym.active .s-price { color: var(--accent); }
    .symbol-bar .sym .ind { width: 6px; height: 6px; border-radius: 50%; flex-shrink: 0; }
    .symbol-bar .sym .ind.long  { background: var(--long); }
    .symbol-bar .sym .ind.short { background: var(--short); }

    /* ====== 图表区 ====== */
    .chart-container { flex: 1; position: relative; overflow: hidden; min-height: 0; touch-action: none; background: var(--bg); }

    .tag-layer { position: absolute; inset: 0; pointer-events: none; z-index: 8; }
    .tag {
      position: absolute; right: 10px; pointer-events: none;
      padding: 2px 7px; border-radius: 4px;
      font-size: 9px; font-weight: 700; color: #fff; white-space: nowrap;
      transform: translateY(-50%);
      box-shadow: 0 2px 6px rgba(0,0,0,0.4); font-variant-numeric: tabular-nums;
    }
    .tag.cost-long  { background: var(--long); }
    .tag.cost-short { background: var(--short); color: #04140d; }
    .tag.sl { background: var(--sl); color: #1a1304; }
    .tag.tp { background: var(--tp); color: #04181a; }
    .tag.ob { background: rgba(242,87,103,0.85); border: 1px dashed #fff3; }
    .tag.os { background: rgba(46,194,126,0.85); color:#04140d; border: 1px dashed #fff3; }
    .tag.cb { background: rgba(138,124,240,0.85); border: 1px dashed #fff3; }
    .tag.cs { background: rgba(43,183,196,0.85); color:#04181a; border: 1px dashed #fff3; }

    /* 浮动账户面板 */
    .side-panel {
      position: absolute; left: 12px; top: 12px;
      width: 280px;
      background: rgba(15,22,34,0.92);
      backdrop-filter: blur(16px);
      border: 1px solid var(--border);
      border-radius: 10px;
      padding: 14px;
      z-index: 20;
      display: none;
      box-shadow: var(--shadow);
      max-height: calc(100% - 24px);
      overflow-y: auto;
    }
    .side-panel.show { display: block; }
    .side-panel .ph {
      display: flex; justify-content: space-between; align-items: center;
      margin-bottom: 12px;
    }
    .side-panel .ph b { font-size: 13px; font-weight: 700; }
    .side-panel .ph .cls { cursor: pointer; font-size: 15px; color: var(--text-3); line-height: 1; padding: 2px 4px; border-radius: 4px; }
    .side-panel .ph .cls:hover { color: var(--long); background: var(--long-soft); }

    .acct-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 8px; margin-bottom: 14px; }
    .acct-grid .cell { background: var(--bg-elev); border: 1px solid var(--border-soft); border-radius: 8px; padding: 8px 10px; }
    .acct-grid .cell .k { font-size: 9px; color: var(--text-3); text-transform: uppercase; letter-spacing: .5px; }
    .acct-grid .cell .v { font-size: 14px; font-weight: 700; margin-top: 3px; font-variant-numeric: tabular-nums; }
    .acct-grid .cell .v.up { color: var(--short); }
    .acct-grid .cell .v.dn { color: var(--long); }

    .sect {
      font-size: 10px; font-weight: 700; color: var(--text-3);
      margin: 12px 0 6px; padding-bottom: 5px;
      border-bottom: 1px solid var(--border-soft);
      display: flex; justify-content: space-between; text-transform: uppercase; letter-spacing: .6px;
    }
    .list .item {
      display: flex; justify-content: space-between; align-items: center; gap: 8px;
      padding: 5px 0; font-size: 11px;
      border-bottom: 1px solid rgba(34,48,71,0.5);
    }
    .list .item:last-child { border-bottom: none; }
    .list .item .lhs { display: flex; align-items: center; gap: 6px; color: var(--text-2); }
    .list .item .v { font-weight: 600; font-variant-numeric: tabular-nums; }
    .list .item .v.up { color: var(--short); }
    .list .item .v.dn { color: var(--long); }
    .list .empty { color: var(--text-3); font-size: 10px; padding: 6px 0; }
    .pill { font-size: 9px; font-weight: 700; padding: 1px 6px; border-radius: 4px; }
    .pill.long  { background: var(--long-soft);  color: var(--long); }
    .pill.short { background: var(--short-soft); color: var(--short); }
    .pill.chase { background: rgba(138,124,240,0.16); color: var(--chase); }
    .x-btn { cursor: pointer; color: var(--text-3); font-size: 13px; line-height: 1; padding: 0 3px; border-radius: 4px; }
    .x-btn:hover { color: var(--long); background: var(--long-soft); }

    /* ====== 底部面板 ====== */
    .bottom-panel {
      background: var(--panel);
      border-top: 1px solid var(--border);
      flex-shrink: 0; overflow: hidden;
      transition: height 0.25s ease; z-index: 25;
    }
    .bottom-panel.closed { height: 30px; }
    .bottom-panel.open   { height: 42vh; }
    .bottom-panel .handle { display: flex; justify-content: center; align-items: center; height: 30px; cursor: pointer; gap: 8px; color: var(--text-3); font-size: 10px; }
    .bottom-panel .handle:hover { background: rgba(255,255,255,0.03); color: var(--text-2); }
    .bottom-panel .handle .handle-bar { width: 34px; height: 3px; background: #3a485e; border-radius: 2px; }
    .bottom-panel .bp-content { height: calc(100% - 30px); display: flex; flex-direction: column; }

    .trade-buttons-mobile { display: none; flex-wrap: wrap; gap: 6px; padding: 10px 12px; border-bottom: 1px solid var(--border-soft); flex-shrink: 0; }
    .trade-buttons-mobile .btn { height: 34px; flex: 1 1 28%; }
    @media (max-width: 767px) { .trade-buttons-mobile { display: flex; } }

    .panel-tabs { display: flex; gap: 4px; padding: 8px 12px 0; flex-shrink: 0; }
    .panel-tabs .tab { font-size: 11px; font-weight: 600; color: var(--text-3); padding: 6px 12px; border-radius: 6px 6px 0 0; cursor: pointer; }
    .panel-tabs .tab:hover { color: var(--text-2); }
    .panel-tabs .tab.active { color: #fff; background: var(--bg-elev); }
    .extra-content { flex: 1; overflow-y: auto; padding: 10px 12px; background: var(--bg-elev); }

    /* ====== 状态栏 ====== */
    .status-bar {
      display: flex; align-items: center; min-height: 26px; padding: 3px 14px;
      background: var(--bg-elev); border-top: 1px solid var(--border);
      font-size: 9px; color: var(--text-3); flex-shrink: 0; gap: 12px; flex-wrap: wrap;
    }
    .status-bar .leg { display: flex; align-items: center; gap: 4px; white-space: nowrap; }
    .status-bar .lc { width: 9px; height: 3px; border-radius: 2px; flex-shrink: 0; }
    .status-bar .keys { margin-left: auto; display: flex; align-items: center; gap: 6px; flex-wrap: wrap; }
    .status-bar .keys kbd {
      background: var(--control); padding: 1px 6px; border-radius: 4px;
      border: 1px solid var(--border); font-size: 9px; color: var(--text-2);
      font-family: inherit; box-shadow: 0 1px 0 rgba(0,0,0,0.3);
    }
    @media (max-width: 767px) { .status-bar .keys { display: none; } }

    /* ====== Toast ====== */
    .toast {
      position: fixed; top: 64px; left: 50%; transform: translateX(-50%) translateY(-8px);
      background: rgba(15,22,34,0.96); color: var(--text);
      padding: 9px 18px; border-radius: 8px; font-size: 12px; font-weight: 600;
      z-index: 1000; pointer-events: none; opacity: 0; transition: opacity .2s, transform .2s;
      border: 1px solid var(--border); box-shadow: var(--shadow); white-space: nowrap;
    }
    .toast.show { opacity: 1; transform: translateX(-50%) translateY(0); }

    ::-webkit-scrollbar { width: 6px; height: 6px; }
    ::-webkit-scrollbar-track { background: transparent; }
    ::-webkit-scrollbar-thumb { background: var(--border); border-radius: 3px; }
    ::-webkit-scrollbar-thumb:hover { background: var(--control-h); }
  </style>
</head>
<body>
<div class="app">

  <!-- 顶部栏 -->
  <header class="topbar">
    <div class="brand">
      <div class="logo">TK</div>
      <div class="title"><b>TkChart</b><small>Trading Terminal</small></div>
    </div>
    <div class="conn">
      <span class="ws-dot offline" id="wsDot"></span>
      <span id="wsLabel">离线</span>
    </div>
    <div class="ticker">
      <span class="name" id="activeName">--</span>
      <span class="price num" id="price">--</span>
    </div>

    <span class="spacer"></span>

    <div class="stats">
      <div class="stat opt"><span class="lbl">余额</span><span class="val num" id="statBalance">--</span></div>
      <div class="stat opt"><span class="lbl">可用</span><span class="val num" id="statAvail">--</span></div>
      <div class="stat opt"><span class="lbl">保证金</span><span class="val num" id="statMargin">--</span></div>
      <div class="stat"><span class="lbl">浮动盈亏</span><span class="val num" id="statFloat">0.00</span></div>
    </div>
  </header>

  <!-- 工具栏 -->
  <div class="toolbar">
    <div class="group">
      <button class="btn on" id="bMA">MA</button>
      <button class="btn on" id="bVOL">VOL</button>
    </div>
    <div class="sep pc-only"></div>
    <div class="group pc-only">
      <button class="btn btn-long"  id="btnBuy">做多</button>
      <button class="btn btn-short" id="btnSell">做空</button>
    </div>
    <div class="group pc-only">
      <button class="btn btn-ob" id="btnOB">挂多</button>
      <button class="btn btn-os" id="btnOS">挂空</button>
      <button class="btn btn-cb" id="btnCB">追多</button>
      <button class="btn btn-cs" id="btnCS">追空</button>
    </div>
    <div class="group pc-only">
      <button class="btn btn-ghost" id="btnClose">平仓</button>
      <button class="btn btn-ghost" id="btnClear">清空</button>
      <button class="btn btn-ghost" id="btnPanel">面板</button>
    </div>

    <div class="qty">
      <label>手数</label>
      <input type="number" id="orderVol" value="1" min="1" step="1">
      <span class="hint">止损 / 止盈自动计算</span>
    </div>
  </div>

  <!-- 品种条 -->
  <div class="symbol-bar" id="symbolBar"></div>

  <!-- 图表区 -->
  <div class="chart-container" id="chartContainer">
    <div class="tag-layer" id="tagLayer"></div>
    <aside class="side-panel" id="sidePanel">
      <div class="ph"><b>账户概览</b><span class="cls" id="panelClose">✕</span></div>
      <div class="acct-grid">
        <div class="cell"><div class="k">余额</div><div class="v num" id="ab">--</div></div>
        <div class="cell"><div class="k">可用</div><div class="v num" id="aa">--</div></div>
        <div class="cell"><div class="k">保证金</div><div class="v num" id="am">--</div></div>
        <div class="cell"><div class="k">浮动盈亏</div><div class="v num" id="tp">0</div></div>
      </div>
      <div class="sect">持仓 <span id="ss">0/0/0</span></div>
      <div class="list" id="pl"><div class="empty">暂无持仓</div></div>
      <div class="sect">挂单</div>
      <div class="list" id="ol"><div class="empty">暂无挂单</div></div>
      <div class="sect">成交</div>
      <div class="list" id="tl"><div class="empty">暂无成交</div></div>
    </aside>
  </div>

  <!-- 底部面板 -->
  <div class="bottom-panel closed" id="bottomPanel">
    <div class="handle" id="panelHandle">
      <div class="handle-bar"></div>
      <span id="handleHint">上拉展开持仓 / 挂单 / 成交</span>
    </div>
    <div class="bp-content">
      <div class="trade-buttons-mobile" id="mobileTradeBtns">
        <button class="btn btn-long"  id="btnBuyM">做多</button>
        <button class="btn btn-short" id="btnSellM">做空</button>
        <button class="btn btn-ob" id="btnOBM">挂多</button>
        <button class="btn btn-os" id="btnOSM">挂空</button>
        <button class="btn btn-cb" id="btnCBM">追多</button>
        <button class="btn btn-cs" id="btnCSM">追空</button>
        <button class="btn btn-ghost" id="btnCloseM">平仓</button>
        <button class="btn btn-ghost" id="btnClearM">清空</button>
        <button class="btn btn-ghost" id="btnPanelM">面板</button>
      </div>
      <div class="panel-tabs" id="panelTabs">
        <div class="tab active" data-tab="pos">持仓</div>
        <div class="tab" data-tab="ord">挂单</div>
        <div class="tab" data-tab="trd">成交</div>
      </div>
      <div class="extra-content list" id="panelContent"></div>
    </div>
  </div>

  <!-- 状态栏 -->
  <footer class="status-bar">
    <div class="leg"><span class="lc" style="background:var(--long)"></span>阳 / 多</div>
    <div class="leg"><span class="lc" style="background:var(--short)"></span>阴 / 空</div>
    <div class="leg"><span class="lc" style="background:var(--sl)"></span>止损</div>
    <div class="leg"><span class="lc" style="background:var(--tp)"></span>止盈</div>
    <div class="leg"><span class="lc" style="background:var(--chase)"></span>追单</div>
    <div class="keys">
      <kbd>1</kbd>多 <kbd>2</kbd>空 <kbd>3</kbd>挂多 <kbd>4</kbd>挂空 <kbd>5</kbd>平 <kbd>6</kbd>追多 <kbd>7</kbd>追空 <kbd>0</kbd>清
    </div>
  </footer>
</div>

<div class="toast" id="toast"></div>

<script src="https://unpkg.com/lightweight-charts@3.8.0/dist/lightweight-charts.standalone.production.js"></script>
<script>
(function () {
  const L = LightweightCharts;
  const $ = id => document.getElementById(id);

  // ====== 主题色（与 CSS 变量同步） ======
  const C = {
    bg: '#0a0e15', grid: '#16202f', text: '#5f6e84', border: '#223047',
    long: '#f25767', short: '#2ec27e', sl: '#e0a93a', tp: '#2bb7c4', chase: '#8a7cf0',
    ma5: '#f25767', ma10: '#e0a93a', ma20: '#4d9fff', ma60: '#2ec27e'
  };

  let CONTRACTS = [], insts = {}, lastP = {}, trades = [];
  let activeSym = '', pm = 'closed', showMA = true, showVol = true, activeTab = 'pos';
  const chartContainer = $('chartContainer'), tagLayer = $('tagLayer');
  let sPos = {}, sOrds = [], lineMeta = new Map();

  function toast(t, d = 2200) {
    const b = $('toast');
    b.textContent = t;
    b.classList.add('show');
    clearTimeout(b._t);
    b._t = setTimeout(() => b.classList.remove('show'), d);
  }

  function setOnline(v) {
    $('wsDot').className = 'ws-dot ' + (v ? 'online' : 'offline');
    $('wsLabel').textContent = v ? '已连接' : '离线';
  }

  // ====== WebSocket ======
    // ====== ★ 只改这一段 WebSocket 连接 ======
    let ws = null;
    function connect() {
      if (ws && ws.readyState === WebSocket.OPEN) return;
      // ✅ 自适应协议 + 同源地址：本地 ws://，线上自动 wss://
      const proto = location.protocol === 'https:' ? 'wss:' : 'ws:';
      const wsUrl = proto + '//' + location.host + '/ws';
      ws = new WebSocket(wsUrl);
      ws.onopen = () => setOnline(true);
      ws.onmessage = e => { try { handleMsg(JSON.parse(e.data)); } catch (err) {} };
      ws.onclose = () => { setOnline(false); setTimeout(connect, 3000); };
    }

  function send(cmd) {
    if (ws && ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify(cmd));
    else toast('未连接服务器');
  }

  function handleMsg(m) {
    switch (m.type) {
      case 'instruments':
        CONTRACTS = m.data || [];
        CONTRACTS.forEach(c => { if (!insts[c.sym]) createInst(c.sym); });
        if (!activeSym || !CONTRACTS.find(c => c.sym === activeSym)) {
          activeSym = (CONTRACTS[0] && CONTRACTS[0].sym) || '';
          if (activeSym) switchSym(activeSym);
        }
        renderSymbols();
        break;
      case 'kline':
        if (m.symbol && insts[m.symbol] && m.data) {
          if (m.name) { const c = CONTRACTS.find(x => x.sym === m.symbol); if (c && !c.name) c.name = m.name; }
          addKline(insts[m.symbol], m.data);
          lastP[m.symbol] = m.data.close;
          refreshLines(insts[m.symbol]);
        }
        refreshUI(); renderSymbols();
        break;
      case 'quote':
        if (m.symbol) lastP[m.symbol] = m.data.last_price || lastP[m.symbol] || 0;
        refreshUI(); renderSymbols();
        break;
      case 'account':
        setText('ab', (m.data.balance || 0).toFixed(2));
        setText('aa', (m.data.available || 0).toFixed(2));
        setText('am', (m.data.margin || 0).toFixed(2));
        setText('statBalance', fmt(m.data.balance));
        setText('statAvail', fmt(m.data.available));
        setText('statMargin', fmt(m.data.margin));
        const fl = m.data.pnl || m.data.position_pnl || 0;
        const tpEl = $('tp'); tpEl.textContent = fl.toFixed(2);
        tpEl.className = 'v num ' + (fl >= 0 ? 'up' : 'dn');
        break;
      case 'position':
        sPos = m.data; rebuild(); refreshUI(); renderSymbols();
        break;
      case 'orders_sync':
        sOrds = m.data; rebuild(); refreshUI();
        break;
      case 'trade':
        trades.push({ time: m.data.time || new Date().toLocaleTimeString(), sym: m.data.symbol, side: m.data.side || '--', action: m.data.action || '', entry: m.data.price, exit: m.data.price });
        refreshUI();
        break;
      case 'info': toast(m.msg); break;
      case 'error': toast('⚠ ' + m.msg, 3000); break;
    }
  }

  function setText(id, v) { const el = $(id); if (el) el.textContent = v; }
  function fmt(n) { n = n || 0; return n >= 10000 ? (n / 10000).toFixed(2) + 'w' : n.toFixed(0); }

  // ====== 图表实例 ======
  function createInst(sym) {
    if (insts[sym]) return;
    const div = document.createElement('div');
    div.style.cssText = 'position:absolute;top:0;left:0;width:100%;height:100%';
    div.id = 'ch-' + sym;
    chartContainer.appendChild(div);

    const ch = L.createChart(div, {
      layout: { background: { color: C.bg }, textColor: C.text, fontFamily: "-apple-system, 'PingFang SC', sans-serif" },
      grid: { vertLines: { color: C.grid }, horzLines: { color: C.grid } },
      crosshair: { mode: 0, vertLine: { color: '#3a485e', width: 1, style: 3, labelBackgroundColor: '#1b2738' }, horzLine: { color: '#3a485e', width: 1, style: 3, labelBackgroundColor: '#1b2738' } },
      rightPriceScale: { borderColor: C.border, scaleMargins: { top: 0.08, bottom: 0.15 } },
      timeScale: { borderColor: C.border, timeVisible: true },
      handleScroll: { vertTouchDrag: true, horzTouchDrag: true, mouseWheel: false },
      handleScale: { mouseWheel: false }
    });

    const candleSeries = ch.addCandlestickSeries({
      upColor: C.long, downColor: C.short,
      borderUpColor: C.long, borderDownColor: C.short,
      wickUpColor: C.long, wickDownColor: C.short
    });

    const volumeSeries = ch.addHistogramSeries({ priceFormat: { type: 'volume' }, priceScaleId: 'volume', visible: showVol });
    ch.priceScale('volume').applyOptions({ scaleMargins: { top: 0.85, bottom: 0 } });

    const maSeries = {
      ma5:  ch.addLineSeries({ color: C.ma5,  lineWidth: 1, visible: showMA, priceLineVisible: false, lastValueVisible: false }),
      ma10: ch.addLineSeries({ color: C.ma10, lineWidth: 1, visible: showMA, priceLineVisible: false, lastValueVisible: false }),
      ma20: ch.addLineSeries({ color: C.ma20, lineWidth: 1, visible: showMA, priceLineVisible: false, lastValueVisible: false }),
      ma60: ch.addLineSeries({ color: C.ma60, lineWidth: 1, visible: showMA, priceLineVisible: false, lastValueVisible: false })
    };

    insts[sym] = { dom: div, chart: ch, candleSeries, volumeSeries, maSeries, data: [], lines: [] };

    new ResizeObserver(() => {
      const r = div.getBoundingClientRect();
      if (r.width > 0 && r.height > 0) ch.applyOptions({ width: r.width, height: r.height });
    }).observe(div);

    lastP[sym] = 0;
    if (sym !== activeSym) div.style.display = 'none';
  }

  function addKline(inst, k) {
    const t = Math.floor(new Date(k.start_time).getTime() / 1e3);
    const c = { time: t, open: k.open, high: k.high, low: k.low, close: k.close };
    const idx = inst.data.findIndex(x => x.time >= t);
    if (idx === -1) inst.data.push(c);
    else if (inst.data[idx].time === t) inst.data[idx] = c;
    else inst.data.splice(idx, 0, c);
    if (inst.data.length > 500) inst.data = inst.data.slice(-500);
    inst.candleSeries.setData(inst.data);
    inst.volumeSeries.update({ time: t, value: k.volume, color: k.close >= k.open ? 'rgba(242,87,103,0.45)' : 'rgba(46,194,126,0.45)' });
    updateMA(inst);
  }

  function calcMA(data, period) {
    const result = [];
    for (let i = 0; i < data.length; i++) {
      if (i < period - 1) { result.push({ time: data[i].time }); continue; }
      let sum = 0;
      for (let j = i - period + 1; j <= i; j++) sum += data[j].close;
      result.push({ time: data[i].time, value: sum / period });
    }
    return result;
  }

  function updateMA(inst) {
    if (!showMA || inst.data.length < 5) { Object.values(inst.maSeries).forEach(s => s.setData([])); return; }
    inst.maSeries.ma5.setData(calcMA(inst.data, 5));
    inst.maSeries.ma10.setData(calcMA(inst.data, 10));
    inst.maSeries.ma20.setData(calcMA(inst.data, 20));
    inst.maSeries.ma60.setData(calcMA(inst.data, 60));
  }

  function toY(inst, price) { try { return inst.candleSeries.priceToCoordinate(price); } catch { return null; } }
  function toP(inst, y) { try { return inst.candleSeries.coordinateToPrice(y); } catch { return null; } }

  function clearLines(inst) {
    inst.lines.forEach(s => { try { inst.chart.removeSeries(s); } catch (e) {} lineMeta.delete(s); });
    inst.lines = [];
  }

  function addLine(inst, color, price, style = 2, width = 2.2, meta = {}) {
    const s = inst.chart.addLineSeries({
      color, lineWidth: width, lineStyle: style,
      priceLineVisible: false, lastValueVisible: false, crosshairMarkerVisible: false,
      priceFormat: { type: 'price', precision: 2 }
    });
    s.setData(inst.data.map(d => ({ time: d.time, value: price })));
    inst.lines.push(s);
    lineMeta.set(s, { ...meta, price });
    return s;
  }

  function refreshLines(inst) {
    inst.lines.forEach(s => {
      const m = lineMeta.get(s);
      if (m && m.price != null) s.setData(inst.data.map(d => ({ time: d.time, value: m.price })));
    });
  }

  function rebuild() {
    Object.keys(insts).forEach(sym => {
      const inst = insts[sym]; clearLines(inst); const pos = sPos[sym];
      if (pos) {
        if (pos.long && pos.long.price && pos.long.volume > 0) {
          addLine(inst, C.long, pos.long.price, 0, 2.4, { sym, type: 'pos', side: 'long', kind: 'cost' });
          if (pos.long.sl) addLine(inst, C.sl, pos.long.sl, 2, 2, { sym, type: 'pos', side: 'long', kind: 'sl' });
          if (pos.long.tp) addLine(inst, C.tp, pos.long.tp, 2, 2, { sym, type: 'pos', side: 'long', kind: 'tp' });
        }
        if (pos.short && pos.short.price && pos.short.volume > 0) {
          addLine(inst, C.short, pos.short.price, 0, 2.4, { sym, type: 'pos', side: 'short', kind: 'cost' });
          if (pos.short.sl) addLine(inst, C.sl, pos.short.sl, 2, 2, { sym, type: 'pos', side: 'short', kind: 'sl' });
          if (pos.short.tp) addLine(inst, C.tp, pos.short.tp, 2, 2, { sym, type: 'pos', side: 'short', kind: 'tp' });
        }
      }
      sOrds.forEach(o => {
        if (o.symbol !== sym || o.active === false) return;
        const color = o.type === 'chase' ? (o.side === 'long' ? C.chase : C.tp) : (o.side === 'long' ? C.long : C.short);
        addLine(inst, color, o.price, 3, 1.5, { sym, type: 'order', side: o.side, kind: 'entry', oid: o.id, sl: o.sl, tp: o.tp });
        if (o.sl) addLine(inst, C.sl, o.sl, 3, 1.2, { sym, type: 'order', side: o.side, kind: 'sl', oid: o.id });
        if (o.tp) addLine(inst, C.tp, o.tp, 3, 1.2, { sym, type: 'order', side: o.side, kind: 'tp', oid: o.id });
      });
    });
  }

  // ====== 拖拽 ======
  let drag = null;

  function findLine(inst, clientY) {
    const rect = chartContainer.getBoundingClientRect(), ry = clientY - rect.top;
    let best = null, minDist = Infinity;
    const threshold = window.innerWidth < 768 ? 35 : 16;
    inst.lines.forEach(s => {
      const m = lineMeta.get(s);
      if (!m || m.price == null) return;
      const y = toY(inst, m.price);
      if (y == null || isNaN(y)) return;
      const dist = Math.abs(y - ry);
      if (dist < threshold && dist < minDist) { minDist = dist; best = { series: s, meta: m }; }
    });
    return best;
  }

  function startDrag(e, clientX, clientY) {
    const inst = insts[activeSym]; if (!inst) return false;
    const found = findLine(inst, clientY); if (!found) return false;
    e.preventDefault(); e.stopPropagation();
    drag = { inst, series: found.series, meta: found.meta, startY: clientY, startX: clientX, startPrice: found.meta.price };
    chartContainer.style.cursor = 'row-resize';
    if (found.meta.kind === 'cost' && found.meta.type === 'pos') toast('↑ 拖动设止盈    ↓ 拖动设止损', 2500);
    return true;
  }

  function moveDrag(clientX, clientY) {
    if (!drag) return;
    const inst = drag.inst, meta = drag.meta, dy = clientY - drag.startY, y0 = toY(inst, drag.startPrice);
    if (y0 == null) return;
    const newPrice = toP(inst, y0 + dy); if (newPrice == null) return;
    const rp = +newPrice.toFixed(2);

    if (meta.type === 'pos' && meta.kind === 'cost') {
      const pos = sPos[meta.sym]; if (!pos) return;
      const pd = meta.side === 'long' ? pos.long : pos.short; if (!pd || !pd.price) return;
      const costPrice = pd.price;

      if (meta.side === 'long') {
        if (rp > costPrice) {
          if (pd.tp == null) { addLine(inst, C.tp, rp, 2, 2, { sym: meta.sym, type: 'pos', side: 'long', kind: 'tp' }); if (!sPos[meta.sym].long) sPos[meta.sym].long = {}; sPos[meta.sym].long.tp = rp; }
          else inst.lines.forEach(s => { const m = lineMeta.get(s); if (m && m.sym === meta.sym && m.type === 'pos' && m.side === 'long' && m.kind === 'tp') { s.setData(inst.data.map(d => ({ time: d.time, value: rp }))); m.price = rp; sPos[meta.sym].long.tp = rp; } });
        } else if (rp < costPrice) {
          if (pd.sl == null) { addLine(inst, C.sl, rp, 2, 2, { sym: meta.sym, type: 'pos', side: 'long', kind: 'sl' }); if (!sPos[meta.sym].long) sPos[meta.sym].long = {}; sPos[meta.sym].long.sl = rp; }
          else inst.lines.forEach(s => { const m = lineMeta.get(s); if (m && m.sym === meta.sym && m.type === 'pos' && m.side === 'long' && m.kind === 'sl') { s.setData(inst.data.map(d => ({ time: d.time, value: rp }))); m.price = rp; sPos[meta.sym].long.sl = rp; } });
        }
      } else {
        if (rp > costPrice) {
          if (pd.sl == null) { addLine(inst, C.sl, rp, 2, 2, { sym: meta.sym, type: 'pos', side: 'short', kind: 'sl' }); if (!sPos[meta.sym].short) sPos[meta.sym].short = {}; sPos[meta.sym].short.sl = rp; }
          else inst.lines.forEach(s => { const m = lineMeta.get(s); if (m && m.sym === meta.sym && m.type === 'pos' && m.side === 'short' && m.kind === 'sl') { s.setData(inst.data.map(d => ({ time: d.time, value: rp }))); m.price = rp; sPos[meta.sym].short.sl = rp; } });
        } else if (rp < costPrice) {
          if (pd.tp == null) { addLine(inst, C.tp, rp, 2, 2, { sym: meta.sym, type: 'pos', side: 'short', kind: 'tp' }); if (!sPos[meta.sym].short) sPos[meta.sym].short = {}; sPos[meta.sym].short.tp = rp; }
          else inst.lines.forEach(s => { const m = lineMeta.get(s); if (m && m.sym === meta.sym && m.type === 'pos' && m.side === 'short' && m.kind === 'tp') { s.setData(inst.data.map(d => ({ time: d.time, value: rp }))); m.price = rp; sPos[meta.sym].short.tp = rp; } });
        }
      }

      drag.series.setData(inst.data.map(d => ({ time: d.time, value: costPrice })));
      lineMeta.get(drag.series).price = costPrice;
    } else if (meta.type === 'order' && meta.kind === 'entry') {
      const diff = rp - meta.price, oid = meta.oid;
      inst.lines.forEach(s => { const m = lineMeta.get(s); if (m && m.oid === oid) { const np2 = +(m.price + diff).toFixed(2); s.setData(inst.data.map(d => ({ time: d.time, value: np2 }))); m.price = np2; } });
      const order = sOrds.find(o => o.id === oid);
      if (order) { order.price = +(order.price + diff).toFixed(2); if (order.sl) order.sl = +(order.sl + diff).toFixed(2); if (order.tp) order.tp = +(order.tp + diff).toFixed(2); }
    } else {
      drag.series.setData(inst.data.map(d => ({ time: d.time, value: rp })));
      lineMeta.get(drag.series).price = rp;
      if (meta.type === 'pos') { const pd = sPos[meta.sym] && sPos[meta.sym][meta.side]; if (pd) pd[meta.kind] = rp; }
      else if (meta.type === 'order') { const order = sOrds.find(o => o.id === meta.oid); if (order) order[meta.kind] = rp; }
    }

    drag.startPrice = rp; drag.startY = clientY; drag.startX = clientX;
    refreshUI();
  }

  function endDrag() {
    if (!drag) return;
    const meta = drag.meta, sym = meta.sym || activeSym;
    if (meta.type === 'pos') {
      const pd = sPos[sym] && sPos[sym][meta.side];
      send({ action: 'update_sltp', symbol: sym, side: meta.side, sl: (pd && pd.sl != null) ? pd.sl : null, tp: (pd && pd.tp != null) ? pd.tp : null });
    } else if (meta.type === 'order') {
      const order = sOrds.find(o => o.id === meta.oid);
      if (order) send({ action: 'update_order', order_id: order.id, symbol: sym, side: order.side, price: order.price, sl: order.sl != null ? order.sl : null, tp: order.tp != null ? order.tp : null, type: order.type || 'limit' });
    }
    drag = null; chartContainer.style.cursor = '';
    refreshUI();
  }

  chartContainer.addEventListener('mousedown', e => { if (e.button !== 0) return; startDrag(e, e.clientX, e.clientY); });
  document.addEventListener('mousemove', e => { if (drag) moveDrag(e.clientX, e.clientY); });
  document.addEventListener('mouseup', () => { if (drag) endDrag(); });
  chartContainer.addEventListener('touchstart', e => { if (e.touches.length !== 1) return; const t = e.touches[0]; startDrag(e, t.clientX, t.clientY); }, { passive: false });
  document.addEventListener('touchmove', e => { if (!drag) return; e.preventDefault(); const t = e.touches[0]; moveDrag(t.clientX, t.clientY); }, { passive: false });
  document.addEventListener('touchend', () => { if (drag) endDrag(); });

  // ====== 价格标签 ======
  function updateTags() {
    tagLayer.innerHTML = '';
    const inst = insts[activeSym]; if (!inst) return;
    const at = (cls, txt, p) => { const y = toY(inst, p); if (y == null || isNaN(y)) return; const el = document.createElement('div'); el.className = 'tag ' + cls; el.textContent = txt; el.style.top = y + 'px'; tagLayer.appendChild(el); };
    const pos = sPos[activeSym];
    if (pos) {
      if (pos.long && pos.long.price && pos.long.volume > 0) { at('cost-long', '多 ' + pos.long.price.toFixed(2), pos.long.price); if (pos.long.sl) at('sl', '损 ' + pos.long.sl.toFixed(2), pos.long.sl); if (pos.long.tp) at('tp', '盈 ' + pos.long.tp.toFixed(2), pos.long.tp); }
      if (pos.short && pos.short.price && pos.short.volume > 0) { at('cost-short', '空 ' + pos.short.price.toFixed(2), pos.short.price); if (pos.short.sl) at('sl', '损 ' + pos.short.sl.toFixed(2), pos.short.sl); if (pos.short.tp) at('tp', '盈 ' + pos.short.tp.toFixed(2), pos.short.tp); }
    }
    sOrds.forEach(o => { if (o.symbol !== activeSym || o.active === false) return; const pf = o.type === 'chase' ? (o.side === 'long' ? '追多' : '追空') : (o.side === 'long' ? '挂多' : '挂空'); const cls = o.type === 'chase' ? (o.side === 'long' ? 'cb' : 'cs') : (o.side === 'long' ? 'ob' : 'os'); at(cls, pf + ' ' + o.price.toFixed(2), o.price); if (o.sl) at('sl', '损 ' + o.sl.toFixed(2), o.sl); if (o.tp) at('tp', '盈 ' + o.tp.toFixed(2), o.tp); });
  }
  function startTagLoop() { (function l() { updateTags(); requestAnimationFrame(l); })(); }

  // ====== UI 刷新 ======
  function refreshUI() {
    const p = lastP[activeSym];
    const c = CONTRACTS.find(x => x.sym === activeSym);
    setText('activeName', c ? (c.name || activeSym) + ' ' + '<' + activeSym + '>' : activeSym || '--');
    const priceEl = $('price');
    if (p) {
      priceEl.textContent = p.toFixed(2);
      const inst = insts[activeSym];
      const open = inst && inst.data.length ? inst.data[inst.data.length - 1].open : p;
      priceEl.className = 'price num ' + (p >= open ? 'up' : 'dn');
    }

    const pos = sPos[activeSym];
    let pnl = 0;
    if (pos && p) {
      if (pos.long && pos.long.price) pnl += (p - pos.long.price) * (pos.long.volume || 1);
      if (pos.short && pos.short.price) pnl += (pos.short.price - p) * (pos.short.volume || 1);
    }
    const fEl = $('statFloat');
    if (fEl) { fEl.textContent = (pnl >= 0 ? '+' : '') + pnl.toFixed(2); fEl.className = 'val num ' + (pnl >= 0 ? 'up' : 'dn'); }

    const ao = sOrds.filter(o => o.active !== false);
    const nPos = (pos && pos.long && pos.long.price ? 1 : 0) + (pos && pos.short && pos.short.price ? 1 : 0);
    setText('ss', nPos + ' 持仓 / ' + ao.length + ' 挂单 / ' + trades.length + ' 成交');

    renderPositions('pl', p);
    renderOrders('ol');
    renderTrades('tl');
    renderPanelContent(p);
  }

  function renderPositions(targetId, p) {
    const el = $(targetId); if (!el) return;
    let html = '';
    Object.keys(sPos).forEach(sym => {
      const pos = sPos[sym]; if (!pos) return;
      ['long', 'short'].forEach(side => {
        const pd = pos[side];
        if (pd && pd.price && pd.volume > 0) {
          const cur = lastP[sym] || pd.price;
          const pp = (side === 'long' ? cur - pd.price : pd.price - cur) * (pd.volume || 1);
          html += '<div class="item"><span class="lhs"><span class="pill ' + side + '">' + (side === 'long' ? '多' : '空') + '</span>' + (sym === activeSym ? '<b>' : '') + sym + (sym === activeSym ? '</b>' : '') + ' @' + pd.price.toFixed(2) + ' ×' + (pd.volume || 1) + '</span><span class="v ' + (pp >= 0 ? 'up' : 'dn') + '">' + (pp >= 0 ? '+' : '') + pp.toFixed(2) + '</span></div>';
        }
      });
    });
    el.innerHTML = html || '<div class="empty">暂无持仓</div>';
  }

  function renderOrders(targetId) {
    const el = $(targetId); if (!el) return;
    const ao = sOrds.filter(o => o.active !== false);
    let html = '';
    ao.forEach(o => {
      const isChase = o.type === 'chase';
      const pf = isChase ? (o.side === 'long' ? '追多' : '追空') : (o.side === 'long' ? '挂多' : '挂空');
      const cls = isChase ? 'chase' : o.side;
      html += '<div class="item"><span class="lhs"><span class="pill ' + cls + '">' + pf + '</span>' + o.symbol + ' @' + o.price.toFixed(2) + '</span><span class="lhs"><span style="color:var(--text-3)">损 ' + (o.sl || '--') + ' · 盈 ' + (o.tp || '--') + '</span><span class="x-btn" data-cancel="' + o.id + '">✕</span></span></div>';
    });
    el.innerHTML = html || '<div class="empty">暂无挂单</div>';
    el.querySelectorAll('[data-cancel]').forEach(b => { b.onclick = () => send({ action: 'cancel_order', order_id: b.dataset.cancel }); });
  }

  function renderTrades(targetId) {
    const el = $(targetId); if (!el) return;
    let html = '';
    trades.slice().reverse().slice(0, 20).forEach(t => {
      const sideCls = t.side === 'long' ? 'long' : (t.side === 'short' ? 'short' : '');
      html += '<div class="item"><span class="lhs"><span style="color:var(--text-3)">' + t.time + '</span>' + (sideCls ? '<span class="pill ' + sideCls + '">' + (sideCls === 'long' ? '多' : '空') + '</span>' : '') + t.sym + '</span><span class="v">' + (t.action || '') + ' ' + (t.entry != null ? Number(t.entry).toFixed(2) : '') + '</span></div>';
    });
    el.innerHTML = html || '<div class="empty">暂无成交</div>';
  }

  function renderPanelContent(p) {
    const el = $('panelContent'); if (!el) return;
    if (activeTab === 'pos') renderPositions('panelContent', p);
    else if (activeTab === 'ord') renderOrders('panelContent');
    else renderTrades('panelContent');
  }

  // ====== 品种条 ======
  function renderSymbols() {
    const bar = $('symbolBar');
    bar.innerHTML = CONTRACTS.map(c => {
      const p = lastP[c.sym] || 0, pos = sPos[c.sym];
      let ind = '';
      if (pos) {
        if (pos.long && pos.long.price && pos.long.volume > 0) ind += '<span class="ind long"></span>';
        if (pos.short && pos.short.price && pos.short.volume > 0) ind += '<span class="ind short"></span>';
      }
      return '<div class="sym' + (c.sym === activeSym ? ' active' : '') + '" data-sym="' + c.sym + '">' + (c.name || c.sym) + ' <span class="s-price">' + p.toFixed(2) + '</span>' + ind + '</div>';
    }).join('');
    bar.querySelectorAll('.sym').forEach(el => { el.onclick = () => { if (el.dataset.sym !== activeSym) switchSym(el.dataset.sym); }; });
  }

  function switchSym(sym) {
    if (!insts[sym]) createInst(sym);
    const inst = insts[sym]; inst.dom.style.display = 'block';
    Object.entries(insts).forEach(([s, i]) => { if (s !== sym) i.dom.style.display = 'none'; });
    activeSym = sym; tagLayer.innerHTML = '';
    setTimeout(() => {
      const r = inst.dom.getBoundingClientRect();
      if (r.width > 0 && r.height > 0) {
        inst.chart.applyOptions({ width: r.width, height: r.height });
        if (inst.data.length > 0) inst.candleSeries.setData(inst.data);
        inst.lines.forEach(s => { const m = lineMeta.get(s); if (m && m.price != null) s.setData(inst.data.map(d => ({ time: d.time, value: m.price }))); });
        updateMA(inst);
      }
    }, 200);
    refreshUI(); renderSymbols();
  }

  function toggleIndi(type) {
    if (type === 'MA') {
      showMA = !showMA;
      Object.values(insts).forEach(i => { Object.values(i.maSeries).forEach(s => s.applyOptions({ visible: showMA })); if (showMA) updateMA(i); });
      $('bMA').classList.toggle('on', showMA);
    } else if (type === 'VOL') {
      showVol = !showVol;
      Object.values(insts).forEach(i => i.volumeSeries.applyOptions({ visible: showVol }));
      $('bVOL').classList.toggle('on', showVol);
    }
  }

  // ====== 交易动作 ======
  function getVol() { return parseInt($('orderVol').value) || 1; }
  function openLong()  { if (activeSym) send({ action: 'open', side: 'long',  symbol: activeSym, volume: getVol() }); }
  function openShort() { if (activeSym) send({ action: 'open', side: 'short', symbol: activeSym, volume: getVol() }); }
  function placeOrder(side) {
    if (!activeSym) return;
    const p = lastP[activeSym] || 0;
    const ep = side === 'long' ? +(p * 0.99).toFixed(2) : +(p * 1.01).toFixed(2);
    const sl = side === 'long' ? +(ep * 0.98).toFixed(2) : +(ep * 1.02).toFixed(2);
    const tp = side === 'long' ? +(ep * 1.04).toFixed(2) : +(ep * 0.96).toFixed(2);
    send({ action: 'order', side, symbol: activeSym, price: ep, volume: getVol(), order_type: 'limit', order_id: 'o' + Date.now(), sl, tp });
  }
  function placeChase(side) {
    if (!activeSym) return;
    const p = lastP[activeSym] || 0;
    const trigger = side === 'long' ? +(p * 1.005).toFixed(2) : +(p * 0.995).toFixed(2);
    const sl = side === 'long' ? +(trigger * 0.98).toFixed(2) : +(trigger * 1.02).toFixed(2);
    const tp = side === 'long' ? +(trigger * 1.04).toFixed(2) : +(trigger * 0.96).toFixed(2);
    send({ action: 'order', side, symbol: activeSym, price: trigger, volume: getVol(), order_type: 'chase', order_id: 'c' + Date.now(), sl, tp });
  }
  function closeAll() { if (!activeSym) return; send({ action: 'close', side: 'long', symbol: activeSym, volume: 1 }); send({ action: 'close', side: 'short', symbol: activeSym, volume: 1 }); }
  function clearAll() { closeAll(); send({ action: 'clear_orders' }); }
  function togglePanel() { $('sidePanel').classList.toggle('show'); refreshUI(); }

  // ====== 绑定 ======
  $('bMA').onclick = () => toggleIndi('MA');
  $('bVOL').onclick = () => toggleIndi('VOL');
  const bind = (id, fn) => { const el = $(id); if (el) el.onclick = fn; };
  bind('btnBuy', openLong);   bind('btnBuyM', openLong);
  bind('btnSell', openShort); bind('btnSellM', openShort);
  bind('btnOB', () => placeOrder('long'));  bind('btnOBM', () => placeOrder('long'));
  bind('btnOS', () => placeOrder('short')); bind('btnOSM', () => placeOrder('short'));
  bind('btnCB', () => placeChase('long'));  bind('btnCBM', () => placeChase('long'));
  bind('btnCS', () => placeChase('short')); bind('btnCSM', () => placeChase('short'));
  bind('btnClose', closeAll);  bind('btnCloseM', closeAll);
  bind('btnClear', clearAll);  bind('btnClearM', clearAll);
  bind('btnPanel', togglePanel); bind('btnPanelM', togglePanel);
  bind('panelClose', togglePanel);

  $('panelTabs').querySelectorAll('.tab').forEach(t => {
    t.onclick = () => {
      activeTab = t.dataset.tab;
      $('panelTabs').querySelectorAll('.tab').forEach(x => x.classList.toggle('active', x === t));
      refreshUI();
    };
  });

  $('panelHandle').onclick = () => {
    pm = pm === 'closed' ? 'open' : 'closed';
    $('bottomPanel').className = 'bottom-panel ' + pm;
    $('handleHint').textContent = pm === 'open' ? '下拉收起' : '上拉展开持仓 / 挂单 / 成交';
    const inst = insts[activeSym];
    if (inst) setTimeout(() => inst.chart.applyOptions({ width: chartContainer.clientWidth, height: chartContainer.clientHeight }), 260);
  };

  document.addEventListener('keydown', e => {
    if (e.target.tagName === 'INPUT' || e.target.tagName === 'TEXTAREA') return;
    switch (e.key) {
      case '1': e.preventDefault(); openLong(); break;
      case '2': e.preventDefault(); openShort(); break;
      case '3': e.preventDefault(); placeOrder('long'); break;
      case '4': e.preventDefault(); placeOrder('short'); break;
      case '5': e.preventDefault(); closeAll(); break;
      case '6': e.preventDefault(); placeChase('long'); break;
      case '7': e.preventDefault(); placeChase('short'); break;
      case '0': e.preventDefault(); clearAll(); break;
    }
  });

  window.addEventListener('resize', () => {
    Object.values(insts).forEach(i => i.chart.applyOptions({ width: chartContainer.clientWidth, height: chartContainer.clientHeight }));
  });

  startTagLoop();
  connect();
})();
</script>
</body>
</html>

"""



@app.route('/')
def index():
    """首页"""
    return str(html)


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5173))
    print("=" * 60)
    print("🚀 TkChart 后端 (模拟版)")
    print(f"   本地访问: http://127.0.0.1:{port}")
    print(f"   WebSocket: ws://127.0.0.1:{port}/ws")
    print(f"   合约: {[i['sym'] for i in Config.INSTRUMENTS]}")
    print("=" * 60)
    # 行情线程已在模块加载时启动，这里不再重复启动
    app.run(host="0.0.0.0", port=port, debug=False, threaded=True)