# -*- coding: utf-8 -*-
"""
技术指标计算模块
计算MACD、PSY等常用技术指标
"""
import pandas as pd
import numpy as np


def compute_ema(series: pd.Series, period: int) -> pd.Series:
    """计算指数移动平均线"""
    return series.ewm(span=period, adjust=False).mean()


def compute_macd(df: pd.DataFrame,
                 fast_period: int = 12,
                 slow_period: int = 26,
                 signal_period: int = 9) -> pd.DataFrame:
    """
    计算MACD指标
    df: 需含'close'列
    返回: DataFrame含MACD_DIF, MACD_DEA, MACD_HIST
    """
    close = df['close']
    ema_fast = compute_ema(close, fast_period)
    ema_slow = compute_ema(close, slow_period)
    dif = ema_fast - ema_slow
    dea = compute_ema(dif, signal_period)
    hist = 2 * (dif - dea)  # 柱状图（放大2倍方便显示）

    result = pd.DataFrame({
        'MACD_DIF': dif,
        'MACD_DEA': dea,
        'MACD_HIST': hist
    }, index=df.index)
    return result


def compute_psy(df: pd.DataFrame, period: int = 12) -> pd.Series:
    """
    计算PSY心理线指标
    PSY = N日内上涨天数 / N * 100
    """
    close = df['close']
    up_count = (close > close.shift(1)).rolling(window=period, min_periods=1).sum()
    psy = (up_count / period) * 100
    return psy


def compute_ma(df: pd.DataFrame, periods: list = None) -> pd.DataFrame:
    """计算移动平均线"""
    if periods is None:
        periods = [5, 10, 20, 30]
    close = df['close']
    result = pd.DataFrame(index=df.index)
    for p in periods:
        result[f'MA{p}'] = close.rolling(window=p, min_periods=1).mean()
    return result


def compute_volume_ma(df: pd.DataFrame, periods: list = None) -> pd.DataFrame:
    """计算成交量移动平均"""
    if periods is None:
        periods = [5, 10]
    vol = df['vol']
    result = pd.DataFrame(index=df.index)
    for p in periods:
        result[f'VOL_MA{p}'] = vol.rolling(window=p, min_periods=1).mean()
    return result


def compute_rsi(df: pd.DataFrame, period: int = 14) -> pd.Series:
    """计算RSI指标"""
    close = df['close']
    delta = close.diff()
    gain = delta.where(delta > 0, 0)
    loss = -delta.where(delta < 0, 0)
    avg_gain = gain.rolling(window=period, min_periods=1).mean()
    avg_loss = loss.rolling(window=period, min_periods=1).mean()
    rs = avg_gain / avg_loss.replace(0, np.nan)
    rsi = 100 - (100 / (1 + rs))
    return rsi.fillna(50)


def add_all_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """为DataFrame添加所有技术指标"""
    df = df.copy()

    # MACD
    macd = compute_macd(df)
    df = pd.concat([df, macd], axis=1)

    # PSY
    df['PSY'] = compute_psy(df)

    # MA
    ma = compute_ma(df)
    df = pd.concat([df, ma], axis=1)

    # 成交量MA
    vol_ma = compute_volume_ma(df)
    df = pd.concat([df, vol_ma], axis=1)

    return df
