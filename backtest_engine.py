# -*- coding: utf-8 -*-
"""
回测引擎模块
完整实现XPK加速策略的回测逻辑
"""
import pandas as pd
import numpy as np
from datetime import datetime


# ======================
# 工具函数
# ======================
def board_type(ts_code):
    """判断板块类型"""
    return "创业板" if ts_code.startswith("300") else "主板"


def is_new_stock(list_date, trade_date, n_days=60):
    """判断是否为次新股"""
    if pd.isna(list_date):
        return False
    if not isinstance(list_date, pd.Timestamp):
        list_date = pd.to_datetime(list_date)
    if not isinstance(trade_date, pd.Timestamp):
        trade_date = pd.to_datetime(trade_date)
    return (trade_date - list_date).days < n_days


# ======================
# 信号预计算
# ======================
def precompute_signals(daily: pd.DataFrame, params: dict) -> pd.DataFrame:
    """
    预计算加速策略信号所需字段
    daily: 包含日线行情的DataFrame（需含ts_code, trade_date, open, high, low, close, pre_close, pct_chg, vol, amount, name, list_date等）
    params: 策略参数字典
    """
    df = daily.copy()

    # 涨停判断
    cond_cyb = df["ts_code"].str.startswith("300")
    df["limit_up_price"] = np.round(
        df["pre_close"] * np.where(cond_cyb, 1.20, 1.10), 2
    )
    df["is_limit"] = (
        (df["pct_chg"] >= np.where(cond_cyb, 19.8, 9.8)) &
        (df["close"] >= df["limit_up_price"] * 0.995)
    )

    # 量比（如果API已有volume_ratio则直接用，否则用前日成交量计算）
    if 'volume_ratio' not in df.columns or df['volume_ratio'].isna().all():
        df["prev_vol"] = df.groupby("ts_code")["vol"].shift(1)
        df["volume_ratio"] = np.where(
            df["prev_vol"] > 0, df["vol"] / df["prev_vol"], 0.0
        )
    else:
        df["volume_ratio"] = df["volume_ratio"].fillna(0.0)

    # 上影线比例
    df["upper_shadow_ratio"] = np.where(
        df["high"] > df["low"],
        (df["high"] - df["close"]) / (df["high"] - df["low"]),
        0.0
    )

    # VWAP
    df["vwap"] = np.where(
        df["vol"] > 0, df["amount"] / df["vol"], df["close"]
    )

    # 前N日累计涨幅
    n_days_lookback = params.get("n_days_lookback", 20)
    df["cum_pct_chg_N"] = df.groupby("ts_code")["pct_chg"].transform(
        lambda x: x.rolling(window=n_days_lookback, min_periods=n_days_lookback).sum()
    )

    # 收盘高于VWAP
    require_close_above_vwap = params.get("require_close_above_vwap", True)
    close_above_vwap_pct = params.get("close_above_vwap_pct", 0.3)
    if require_close_above_vwap and close_above_vwap_pct > 0:
        df["close_above_vwap"] = df["close"] >= df["vwap"] * (1 + close_above_vwap_pct / 100)
    elif require_close_above_vwap:
        df["close_above_vwap"] = df["close"] >= df["vwap"]
    else:
        df["close_above_vwap"] = True

    # ST过滤
    if 'is_st' not in df.columns:
        df["is_st"] = df["name"].str.upper().str.contains("ST", na=False) if 'name' in df.columns else False

    # 次新股
    df["trade_date"] = pd.to_datetime(df["trade_date"])
    df["list_date"] = pd.to_datetime(df["list_date"], errors="coerce")
    df["is_new"] = df.apply(
        lambda row: is_new_stock(row.get("list_date"), row["trade_date"]), axis=1
    )

    # 停牌过滤
    if 'is_suspended' not in df.columns:
        df['is_suspended'] = False

    return df


# ======================
# 信号识别
# ======================
def identify_signals(daily: pd.DataFrame, params: dict) -> pd.DataFrame:
    """识别加速策略信号"""
    volume_ratio_min = params.get("volume_ratio_min", 1.5)
    volume_ratio_max = params.get("volume_ratio_max", 2.5)
    pct_chg_min = params.get("pct_chg_min", 2.0)
    pct_chg_max = params.get("pct_chg_max", 8.0)
    upper_shadow_ratio_min = params.get("upper_shadow_ratio_min", 0.25)
    upper_shadow_ratio_max = params.get("upper_shadow_ratio_max", 0.5)
    cum_pct_chg_min = params.get("cum_pct_chg_min", 20.0)
    cum_pct_chg_max = params.get("cum_pct_chg_max", 100.0)

    signals = daily[
        (daily["ts_code"].str.startswith("300")) &
        (~daily["is_st"]) &
        (~daily["is_new"]) &
        (~daily["is_suspended"]) &
        (daily["pct_chg"] >= pct_chg_min) &
        (daily["pct_chg"] <= pct_chg_max) &
        (daily["volume_ratio"] >= volume_ratio_min) &
        (daily["volume_ratio"] <= volume_ratio_max) &
        (daily["upper_shadow_ratio"] >= upper_shadow_ratio_min) &
        (daily["upper_shadow_ratio"] <= upper_shadow_ratio_max) &
        (daily["close_above_vwap"]) &
        (daily["cum_pct_chg_N"] > cum_pct_chg_min) &
        (daily["cum_pct_chg_N"] <= cum_pct_chg_max)
    ].copy()

    return signals


# ======================
# 回测核心
# ======================
def collect_trade_base(signals: pd.DataFrame, daily: pd.DataFrame, params: dict) -> list:
    """收集基础交易片段"""
    buy_amount = params.get("buy_amount", 100000)
    lot_size = params.get("lot_size", 100)
    max_hold_days = params.get("max_hold_days", 20)

    stock_data = {}
    for ts_code, group in daily.groupby("ts_code"):
        group = group.sort_values("trade_date").reset_index(drop=True)
        date_to_idx = {d.date() if isinstance(d, pd.Timestamp) else d: i
                       for i, d in enumerate(group["trade_date"])}
        stock_data[ts_code] = {"df": group, "date_map": date_to_idx}

    trade_base = []
    signal_dates = sorted(signals["trade_date"].unique())

    for signal_date in signal_dates:
        day_signals = signals[signals["trade_date"] == signal_date]
        for _, sig in day_signals.iterrows():
            ts_code = sig["ts_code"]

            # 二次过滤
            if sig.get("is_st", False):
                continue
            if sig.get("is_new", False):
                continue
            if sig.get("is_suspended", False):
                continue

            stock_info = stock_data.get(ts_code)
            if stock_info is None:
                continue
            stock_df = stock_info["df"]
            date_map = stock_info["date_map"]

            signal_date_obj = signal_date.date() if isinstance(signal_date, pd.Timestamp) else signal_date
            if signal_date_obj not in date_map:
                continue
            sig_idx = date_map[signal_date_obj]

            buy_idx = sig_idx
            if buy_idx >= len(stock_df):
                continue
            buy_day_row = stock_df.iloc[buy_idx]

            # 涨停不买
            if buy_day_row.get("is_limit", False):
                continue
            # 停牌不买
            if buy_day_row.get("is_suspended", False):
                continue

            buy_price = buy_day_row["close"]
            shares = buy_amount // buy_price // lot_size * lot_size
            if shares <= 0:
                continue

            slice_len = min(max_hold_days * 2 + 5, len(stock_df) - buy_idx)
            future_df = stock_df.iloc[buy_idx: buy_idx + slice_len]

            trade_base.append({
                "ts_code": ts_code,
                "name": sig.get("name", ""),
                "board": board_type(ts_code),
                "signal_date": signal_date_obj,
                "buy_index": buy_idx,
                "buy_price": buy_price,
                "initial_shares": shares,
                "stock_df_slice": future_df
            })

    return trade_base


def compute_for_profit_target(trade_base: list, profit_target: float, params: dict) -> pd.DataFrame:
    """计算指定止盈目标的回测结果"""
    supplement_rate = params.get("supplement_rate", 0.86)
    max_hold_days = params.get("max_hold_days", 20)
    lot_size = params.get("lot_size", 100)

    trade_logs = []
    for trade in trade_base:
        stock_slice = trade["stock_df_slice"]
        initial_buy_price = trade["buy_price"]
        avg_cost = initial_buy_price
        total_shares = trade["initial_shares"]
        total_cost = total_shares * avg_cost
        supplemented = False
        supplement_relative_idx = -1
        supplement_date = None
        supplement_price = None
        supplement_shares = 0
        hold_limit_relative = max_hold_days - 1
        sell_found = False
        sell_relative_idx = -1
        sell_price = 0.0
        sell_type = ""

        highs = stock_slice["high"].values
        lows = stock_slice["low"].values
        closes = stock_slice["close"].values
        dates = [d.date() if isinstance(d, pd.Timestamp) else d for d in stock_slice["trade_date"]]

        for i in range(len(stock_slice)):
            if i > hold_limit_relative:
                break
            # 补仓逻辑
            if not supplemented and lows[i] <= avg_cost * supplement_rate:
                supplement_price = avg_cost * supplement_rate
                add_shares = trade["initial_shares"]
                add_shares = (add_shares // lot_size) * lot_size
                if add_shares > 0:
                    add_cost = add_shares * supplement_price
                    total_shares += add_shares
                    total_cost += add_cost
                    avg_cost = total_cost / total_shares
                    supplemented = True
                    supplement_relative_idx = i
                    supplement_date = dates[i]
                    supplement_price = round(supplement_price, 3)
                    supplement_shares = add_shares
                    hold_limit_relative = i + max_hold_days - 1
                    continue

            # 卖出判断
            if not supplemented:
                can_sell_today = (i >= 1)
            else:
                can_sell_today = (i >= supplement_relative_idx + 1)

            if not sell_found and can_sell_today:
                target_price = round(avg_cost * (1 + profit_target / 100), 2)
                if highs[i] >= target_price:
                    sell_price = min(highs[i], target_price)
                    sell_relative_idx = i
                    sell_type = f"止盈{profit_target}%卖出"
                    sell_found = True
                    break
                elif i == hold_limit_relative:
                    sell_price = closes[i]
                    sell_relative_idx = i
                    sell_type = "补仓后持有到期卖出" if supplemented else "持有到期未达标尾盘卖出"
                    sell_found = True
                    break

        if not sell_found:
            last_i = len(stock_slice) - 1
            sell_price = closes[last_i]
            sell_relative_idx = last_i
            sell_type = "数据截止强制卖出"

        sell_day = dates[sell_relative_idx]
        trade_days = sell_relative_idx + 1
        revenue = total_shares * sell_price
        profit = revenue - total_cost

        trade_logs.append({
            "ts_code": trade["ts_code"],
            "股票名称": trade["name"],
            "板块": trade["board"],
            "信号日": trade["signal_date"],
            "买入日": dates[0],
            "卖出日": sell_day,
            "交易日数": trade_days,
            "是否补仓": "是" if supplemented else "否",
            "初始买入价": round(initial_buy_price, 3),
            "补仓日期": supplement_date,
            "补仓价格": supplement_price,
            "补仓股数": supplement_shares,
            "卖出价格": round(sell_price, 3),
            "补仓后成本": round(avg_cost, 2),
            "卖出方式": sell_type,
            "投入金额": round(total_cost, 2),
            "盈利金额": round(profit, 2),
            "收益率": round(profit / total_cost, 4) if total_cost > 0 else 0
        })

    return pd.DataFrame(trade_logs)


def run_backtest(daily: pd.DataFrame, params: dict,
                 profit_targets: list = None,
                 progress_callback=None) -> tuple:
    """
    执行完整回测
    返回: (summary_df, all_trades_df, signals_df)
    """
    if profit_targets is None:
        profit_targets = [0.5] + list(range(1, 21))

    # 预计算
    daily = precompute_signals(daily, params)

    # 信号识别
    signals = identify_signals(daily, params)
    end_date = params.get("end_date", "20991231")
    signals = signals[signals["trade_date"] <= pd.to_datetime(end_date)]

    if signals.empty:
        return pd.DataFrame(), pd.DataFrame(), signals

    # 收集交易
    trade_base = collect_trade_base(signals, daily, params)
    if not trade_base:
        return pd.DataFrame(), pd.DataFrame(), signals

    # 逐止盈点计算
    summary_rows = []
    all_trades_list = []

    for idx, profit_target in enumerate(profit_targets):
        if progress_callback:
            progress_callback(idx / len(profit_targets),
                              f"正在计算止盈点 {profit_target}%...")

        df_trades = compute_for_profit_target(trade_base, profit_target, params)
        df_trades["止盈目标(%)"] = profit_target
        all_trades_list.append(df_trades)

        for board in ["主板", "创业板"]:
            df_board = df_trades[df_trades["板块"] == board]
            if len(df_board) == 0:
                continue
            total_trades = len(df_board)
            win_trades = len(df_board[df_board["盈利金额"] > 0])
            win_rate = round(win_trades / total_trades, 4) if total_trades > 0 else 0
            avg_trade_days = round(df_board["交易日数"].mean(), 2)
            total_profit = round(df_board["盈利金额"].sum(), 2)
            total_invest = round(df_board["投入金额"].sum(), 2)
            overall_return = round(total_profit / total_invest, 4) if total_invest > 0 else 0
            unit_time_return = round(overall_return / avg_trade_days, 4) if avg_trade_days > 0 else 0
            summary_rows.append({
                "板块": board,
                "止盈点(%)": profit_target,
                "总交易数": total_trades,
                "胜率": win_rate,
                "平均交易日数": avg_trade_days,
                "单位时间收益率": unit_time_return,
                "总投入": total_invest,
                "总盈利": total_profit,
                "整体收益率": overall_return
            })

    df_summary = pd.DataFrame(summary_rows)
    if not df_summary.empty:
        df_summary = df_summary.sort_values(by=["板块", "止盈点(%)"])

    df_all_trades = pd.concat(all_trades_list, ignore_index=True) if all_trades_list else pd.DataFrame()

    if progress_callback:
        progress_callback(1.0, "回测完成！")

    return df_summary, df_all_trades, signals


def detect_daily_signals(trade_date: str, params: dict) -> pd.DataFrame:
    """
    检测指定日期的加速策略信号
    用于"前一日信号提醒"功能
    """
    from data_service import get_signal_date_daily

    daily = get_signal_date_daily(trade_date)
    if daily.empty:
        return pd.DataFrame()

    # 计算必要字段
    cond_cyb = daily["ts_code"].str.startswith("300")
    daily["upper_shadow_ratio"] = np.where(
        daily["high"] > daily["low"],
        (daily["high"] - daily["close"]) / (daily["high"] - daily["low"]),
        0.0
    )
    daily["vwap"] = np.where(
        daily["vol"] > 0, daily["amount"] / daily["vol"], daily["close"]
    )

    # 信号过滤参数
    volume_ratio_min = params.get("volume_ratio_min", 1.5)
    volume_ratio_max = params.get("volume_ratio_max", 2.5)
    pct_chg_min = params.get("pct_chg_min", 2.0)
    pct_chg_max = params.get("pct_chg_max", 8.0)
    upper_shadow_ratio_min = params.get("upper_shadow_ratio_min", 0.25)
    upper_shadow_ratio_max = params.get("upper_shadow_ratio_max", 0.5)
    n_days_lookback = params.get("n_days_lookback", 20)
    cum_pct_min = params.get("cum_pct_chg_min", 20.0)
    cum_pct_max = params.get("cum_pct_chg_max", 100.0)
    require_close_above_vwap = params.get("require_close_above_vwap", True)
    close_above_vwap_pct = params.get("close_above_vwap_pct", 0.3)

    # 第一步：先用基本条件（涨跌幅、量比、上影线、ST、停牌、VWAP）快速过滤
    # 这些条件用当日数据即可判断，无需额外API调用
    cyb_stocks = daily[daily["ts_code"].str.startswith("300")]

    pre_filtered = []
    for _, row in cyb_stocks.iterrows():
        upper_shadow = row["upper_shadow_ratio"]
        vr = row.get("volume_ratio", 0)
        pct = row["pct_chg"]
        vwap = row["vwap"]
        close = row["close"]

        close_above = True
        if require_close_above_vwap and close_above_vwap_pct > 0:
            close_above = close >= vwap * (1 + close_above_vwap_pct / 100)
        elif require_close_above_vwap:
            close_above = close >= vwap

        if (not row.get("is_st", False) and
            not row.get("is_suspended", False) and
            pct >= pct_chg_min and pct <= pct_chg_max and
            vr >= volume_ratio_min and vr <= volume_ratio_max and
            upper_shadow >= upper_shadow_ratio_min and upper_shadow <= upper_shadow_ratio_max and
            close_above):
            pre_filtered.append(row)

    # 第二步：仅对通过初筛的股票查询历史数据，计算累计涨幅
    from data_service import get_daily_data
    results = []

    for i, row in enumerate(pre_filtered):
        ts_code = row["ts_code"]
        hist = get_daily_data(ts_code=ts_code, end_date=trade_date)
        if hist.empty or len(hist) < n_days_lookback:
            continue
        hist = hist.sort_values('trade_date')
        hist['cum_pct'] = hist['pct_chg'].rolling(
            window=n_days_lookback, min_periods=n_days_lookback).sum()
        today_data = hist[hist['trade_date'] == trade_date]
        if today_data.empty:
            continue
        cum_pct = today_data['cum_pct'].values[0]
        if pd.isna(cum_pct):
            continue

        if cum_pct > cum_pct_min and cum_pct <= cum_pct_max:

            is_new = is_new_stock(row.get("list_date"), pd.to_datetime(trade_date))
            if is_new:
                continue

            results.append({
                "ts_code": ts_code,
                "股票名称": row.get("name", ""),
                "板块": "创业板",
                "信号日": trade_date,
                "涨跌幅(%)": pct,
                "量比": round(vr, 2),
                "上影线比例": round(upper_shadow, 4),
                "前N日累计涨幅(%)": round(cum_pct, 2),
                "收盘价": close,
                "VWAP": round(vwap, 2),
                "成交量(手)": row.get("vol", 0),
                "成交额(千元)": row.get("amount", 0),
            })

    return pd.DataFrame(results)
