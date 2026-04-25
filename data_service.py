# -*- coding: utf-8 -*-
"""
Tushare数据服务模块
封装所有Tushare API调用，提供数据缓存和限流控制
"""
import tushare as ts
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path
import streamlit as st
from concurrent.futures import ThreadPoolExecutor, as_completed


# 默认Token和API地址（内置，打开网页即可用）
DEFAULT_TOKEN = "hTASoWevdIQVKNJgEUGoDEWIMufHKYuLTSUGZfUOImwssjguKASNmWMywBkFgpjF"
TUSHARE_API_URL = "http://124.222.60.121:8020/"
BACKTEST_CACHE_DIR = Path(__file__).resolve().parent / ".cache" / "backtest_daily_v1"


@st.cache_resource
def get_pro_api():
    """获取Tushare Pro API实例（全局缓存）"""
    # 优先使用用户在页面上输入的token，其次使用secrets配置，最后使用默认token
    token = st.session_state.get("tushare_token", "")
    if not token:
        token = st.secrets.get("TUSHARE_TOKEN", "")
    if not token or token == "your_tushare_token_here":
        token = DEFAULT_TOKEN
    pro = ts.pro_api(token)
    pro._DataApi__http_url = TUSHARE_API_URL
    return pro


@st.cache_data(ttl=7200, show_spinner=False)
def get_trade_calendar(start_date: str, end_date: str) -> pd.DataFrame:
    """获取交易日历"""
    pro = get_pro_api()
    if pro is None:
        return pd.DataFrame()
    try:
        df = pro.trade_cal(exchange='SSE', start_date=start_date, end_date=end_date)
        return df
    except Exception as e:
        st.error(f"获取交易日历失败: {e}")
        return pd.DataFrame()


@st.cache_data(ttl=7200, show_spinner=False)
def get_latest_trade_date() -> str:
    """获取最近的交易日期"""
    today = datetime.now()
    # 向前查找30天范围内的交易日
    start = (today - timedelta(days=30)).strftime('%Y%m%d')
    end = today.strftime('%Y%m%d')
    cal = get_trade_calendar(start, end)
    if cal.empty:
        return end
    open_dates = cal[cal['is_open'] == 1]['cal_date'].sort_values()
    # 取今天或之前最近的交易日
    past_dates = open_dates[open_dates <= end]
    if past_dates.empty:
        return end
    return past_dates.iloc[-1]


@st.cache_data(ttl=7200, show_spinner=False)
def get_stock_basic() -> pd.DataFrame:
    """获取股票基础信息列表"""
    pro = get_pro_api()
    if pro is None:
        return pd.DataFrame()
    try:
        df = pro.stock_basic(exchange='', list_status='L',
                             fields='ts_code,symbol,name,area,industry,market,list_date')
        return df
    except Exception as e:
        st.error(f"获取股票列表失败: {e}")
        return pd.DataFrame()


@st.cache_data(ttl=3600, show_spinner=False)
def get_daily_data(ts_code: str = "", trade_date: str = "",
                   start_date: str = "", end_date: str = "") -> pd.DataFrame:
    """获取日线行情数据"""
    pro = get_pro_api()
    if pro is None:
        return pd.DataFrame()
    try:
        kwargs = {}
        if ts_code:
            kwargs['ts_code'] = ts_code
        if trade_date:
            kwargs['trade_date'] = trade_date
        if start_date:
            kwargs['start_date'] = start_date
        if end_date:
            kwargs['end_date'] = end_date
        df = pro.daily(**kwargs)
        return df
    except Exception as e:
        st.error(f"获取日线数据失败: {e}")
        return pd.DataFrame()


@st.cache_data(ttl=3600, show_spinner=False)
def get_daily_basic(trade_date: str = "", ts_code: str = "",
                    fields: str = "ts_code,trade_date,turnover_rate,volume_ratio,pe,pb") -> pd.DataFrame:
    """获取每日指标数据（含量比）"""
    pro = get_pro_api()
    if pro is None:
        return pd.DataFrame()
    try:
        kwargs = {'fields': fields}
        if trade_date:
            kwargs['trade_date'] = trade_date
        if ts_code:
            kwargs['ts_code'] = ts_code
        df = pro.daily_basic(**kwargs)
        return df
    except Exception as e:
        st.error(f"获取每日指标失败: {e}")
        return pd.DataFrame()


@st.cache_data(ttl=3600, show_spinner=False)
def get_weekly_data(ts_code: str, start_date: str = "", end_date: str = "") -> pd.DataFrame:
    """获取周线行情数据"""
    pro = get_pro_api()
    if pro is None:
        return pd.DataFrame()
    try:
        kwargs = {'ts_code': ts_code}
        if start_date:
            kwargs['start_date'] = start_date
        if end_date:
            kwargs['end_date'] = end_date
        df = pro.weekly(**kwargs)
        return df
    except Exception as e:
        st.error(f"获取周线数据失败: {e}")
        return pd.DataFrame()


@st.cache_data(ttl=7200, show_spinner=False)
def get_adj_factor(ts_code: str = "", trade_date: str = "") -> pd.DataFrame:
    """获取复权因子"""
    pro = get_pro_api()
    if pro is None:
        return pd.DataFrame()
    try:
        kwargs = {}
        if ts_code:
            kwargs['ts_code'] = ts_code
        if trade_date:
            kwargs['trade_date'] = trade_date
        df = pro.adj_factor(**kwargs)
        return df
    except Exception as e:
        st.error(f"获取复权因子失败: {e}")
        return pd.DataFrame()


@st.cache_data(ttl=7200, show_spinner=False)
def get_stk_limit(trade_date: str = "", ts_code: str = "",
                  start_date: str = "", end_date: str = "") -> pd.DataFrame:
    """获取涨跌停价格"""
    pro = get_pro_api()
    if pro is None:
        return pd.DataFrame()
    try:
        kwargs = {}
        if trade_date:
            kwargs['trade_date'] = trade_date
        if ts_code:
            kwargs['ts_code'] = ts_code
        if start_date:
            kwargs['start_date'] = start_date
        if end_date:
            kwargs['end_date'] = end_date
        df = pro.stk_limit(**kwargs)
        return df
    except Exception as e:
        st.error(f"获取涨跌停价格失败: {e}")
        return pd.DataFrame()


@st.cache_data(ttl=7200, show_spinner=False)
def get_suspend_d(trade_date: str = "", ts_code: str = "",
                  suspend_type: str = "S") -> pd.DataFrame:
    """获取停复牌信息"""
    pro = get_pro_api()
    if pro is None:
        return pd.DataFrame()
    try:
        kwargs = {'suspend_type': suspend_type}
        if trade_date:
            kwargs['trade_date'] = trade_date
        if ts_code:
            kwargs['ts_code'] = ts_code
        df = pro.suspend_d(**kwargs)
        return df
    except Exception as e:
        st.error(f"获取停复牌信息失败: {e}")
        return pd.DataFrame()


@st.cache_data(ttl=7200, show_spinner=False)
def get_st_stock_list(trade_date: str = "") -> pd.DataFrame:
    """获取ST股票列表（trade_date为空时自动使用最新交易日）"""
    pro = get_pro_api()
    if pro is None:
        return pd.DataFrame()
    try:
        if not trade_date:
            trade_date = get_latest_trade_date()
        df = pro.stock_st(trade_date=trade_date)
        return df
    except Exception as e:
        st.error(f"获取ST股票列表失败: {e}")
        return pd.DataFrame()


def _ensure_backtest_cache_dir() -> Path:
    BACKTEST_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    return BACKTEST_CACHE_DIR


def _get_backtest_cache_file(trade_date: str) -> Path:
    return _ensure_backtest_cache_dir() / f"{trade_date}.pkl"


def _load_backtest_day_cache(trade_date: str, require_adj_factor: bool):
    cache_file = _get_backtest_cache_file(trade_date)
    if not cache_file.exists():
        return None
    try:
        df = pd.read_pickle(cache_file)
    except Exception:
        try:
            cache_file.unlink()
        except OSError:
            pass
        return None

    required_columns = {"ts_code", "trade_date", "is_st", "is_suspended"}
    if require_adj_factor:
        required_columns.add("adj_factor")
    if not required_columns.issubset(df.columns):
        return None
    return df


def _save_backtest_day_cache(trade_date: str, df: pd.DataFrame) -> None:
    cache_file = _get_backtest_cache_file(trade_date)
    df.to_pickle(cache_file)


def _fetch_trade_date_bundle(trade_date: str, need_adj_factor: bool) -> pd.DataFrame:
    pro = get_pro_api()
    if pro is None:
        return pd.DataFrame()

    try:
        daily = pro.daily(trade_date=trade_date)
    except Exception:
        return pd.DataFrame()

    if daily is None or daily.empty:
        return pd.DataFrame()

    try:
        basic = pro.daily_basic(
            trade_date=trade_date,
            fields='ts_code,trade_date,volume_ratio,turnover_rate'
        )
        if basic is not None and not basic.empty:
            daily = daily.merge(
                basic[['ts_code', 'trade_date', 'volume_ratio', 'turnover_rate']],
                on=['ts_code', 'trade_date'],
                how='left'
            )
    except Exception:
        pass

    if need_adj_factor:
        try:
            adj = pro.adj_factor(trade_date=trade_date)
            if adj is not None and not adj.empty:
                daily = daily.merge(
                    adj[['ts_code', 'trade_date', 'adj_factor']],
                    on=['ts_code', 'trade_date'],
                    how='left'
                )
        except Exception:
            pass

    try:
        st_df = pro.stock_st(trade_date=trade_date)
        st_codes = set(st_df['ts_code'].tolist()) if st_df is not None and not st_df.empty else set()
    except Exception:
        st_codes = set()

    try:
        sus = pro.suspend_d(trade_date=trade_date, suspend_type='S')
        suspend_codes = set(sus['ts_code'].tolist()) if sus is not None and not sus.empty else set()
    except Exception:
        suspend_codes = set()

    if 'volume_ratio' not in daily.columns:
        daily['volume_ratio'] = np.nan
    if 'turnover_rate' not in daily.columns:
        daily['turnover_rate'] = np.nan
    if need_adj_factor and 'adj_factor' not in daily.columns:
        daily['adj_factor'] = np.nan

    daily['is_st'] = daily['ts_code'].isin(st_codes)
    daily['is_suspended'] = daily['ts_code'].isin(suspend_codes)

    return daily


def get_daily_data_with_info(start_date: str, end_date: str, progress_placeholder=None, adj_type: str = "qfq") -> pd.DataFrame:
    """
    Get all daily backtest data with metadata.
    Fetches by trade date to avoid per-request row limits, and caches each trade date locally.
    """
    progress = progress_placeholder if progress_placeholder else st.progress(0, text="Preparing data...")

    cal = get_trade_calendar(start_date, end_date)
    if cal.empty:
        return pd.DataFrame()
    trade_dates = cal[cal['is_open'] == 1]['cal_date'].sort_values().tolist()
    if not trade_dates:
        return pd.DataFrame()

    need_adj_factor = adj_type in ('qfq', 'hfq')
    stock_info = get_stock_basic()

    cached_frames = []
    missing_dates = []
    for td in trade_dates:
        cached_df = _load_backtest_day_cache(td, need_adj_factor)
        if cached_df is None:
            missing_dates.append(td)
        else:
            cached_frames.append(cached_df)

    n_dates = len(trade_dates)
    cached_count = len(cached_frames)
    progress.progress(
        5,
        text=f"Preparing backtest data: {n_dates} trade dates, {cached_count} loaded from local cache..."
    )

    fetched_frames = []
    if missing_dates:
        max_workers = min(8, max(2, len(missing_dates)))
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(_fetch_trade_date_bundle, td, need_adj_factor): td
                for td in missing_dates
            }
            total_missing = len(missing_dates)
            for idx, future in enumerate(as_completed(futures), start=1):
                td = futures[future]
                result = future.result()
                if result is not None and not result.empty:
                    _save_backtest_day_cache(td, result)
                    fetched_frames.append(result)
                pct = 5 + int(idx / total_missing * 80)
                progress.progress(
                    pct,
                    text=f"Fetching uncached trade dates: {idx}/{total_missing}..."
                )
    else:
        progress.progress(85, text="All requested trade dates were loaded from local cache...")

    all_frames = cached_frames + fetched_frames
    if not all_frames:
        return pd.DataFrame()

    progress.progress(88, text="Merging data...")
    daily = pd.concat(all_frames, ignore_index=True)
    daily = daily.drop_duplicates(subset=['ts_code', 'trade_date'], keep='first')
    daily = daily.sort_values(['ts_code', 'trade_date']).reset_index(drop=True)

    if not stock_info.empty:
        daily = daily.merge(
            stock_info[['ts_code', 'name', 'list_date', 'market']],
            on='ts_code',
            how='left'
        )

    if 'is_st' not in daily.columns:
        daily['is_st'] = False
    else:
        daily['is_st'] = daily['is_st'].fillna(False)
    if 'is_suspended' not in daily.columns:
        daily['is_suspended'] = False
    else:
        daily['is_suspended'] = daily['is_suspended'].fillna(False)

    if 'name' in daily.columns:
        daily['is_st'] = daily['is_st'] | daily['name'].str.upper().str.contains('ST', na=False)

    progress.progress(96, text=f"Applying adjustment mode: {adj_type}...")
    if adj_type in ('qfq', 'hfq') and 'adj_factor' in daily.columns and daily['adj_factor'].notna().any():
        for col in ['open', 'high', 'low', 'close', 'pre_close']:
            if col in daily.columns:
                daily[f'{col}_bfq'] = daily[col]

        if adj_type == 'qfq':
            latest_adj_map = daily.groupby('ts_code')['adj_factor'].transform('last')
            valid_mask = latest_adj_map.notna() & (latest_adj_map != 0)
            for col in ['open', 'high', 'low', 'close', 'pre_close']:
                if col in daily.columns:
                    daily.loc[valid_mask, col] = daily.loc[valid_mask, col] * daily.loc[valid_mask, 'adj_factor'] / latest_adj_map[valid_mask]
        elif adj_type == 'hfq':
            earliest_adj_map = daily.groupby('ts_code')['adj_factor'].transform('first')
            valid_mask = earliest_adj_map.notna() & (earliest_adj_map != 0)
            for col in ['open', 'high', 'low', 'close', 'pre_close']:
                if col in daily.columns:
                    daily.loc[valid_mask, col] = daily.loc[valid_mask, col] * daily.loc[valid_mask, 'adj_factor'] / earliest_adj_map[valid_mask]

    progress.progress(
        100,
        text=f"Data ready: {len(daily)} rows, {daily['ts_code'].nunique()} stocks, {cached_count} cached dates, {len(missing_dates)} fetched dates."
    )

    return daily
def get_signal_date_daily(trade_date: str) -> pd.DataFrame:
    """
    获取某一日信号检测所需的全部数据
    用于"前一日信号提醒"功能
    """
    pro = get_pro_api()
    if pro is None:
        return pd.DataFrame()

    # 获取当日日线
    daily = get_daily_data(trade_date=trade_date)
    if daily.empty:
        return pd.DataFrame()

    # 获取前一交易日数据（计算量比需要前日成交额）
    cal = get_trade_calendar(
        start_date=(datetime.strptime(trade_date, '%Y%m%d') - timedelta(days=10)).strftime('%Y%m%d'),
        end_date=trade_date
    )
    open_dates = cal[cal['is_open'] == 1]['cal_date'].sort_values().tolist()
    prev_date = ""
    for d in open_dates:
        if d < trade_date:
            prev_date = d

    if prev_date:
        prev_daily = get_daily_data(trade_date=prev_date)
        if not prev_daily.empty:
            prev_amt = prev_daily[['ts_code', 'amount']].rename(columns={'amount': 'prev_amount'})
            daily = daily.merge(prev_amt, on='ts_code', how='left')
            daily['volume_ratio'] = np.where(
                daily['prev_amount'] > 0,
                daily['amount'] / daily['prev_amount'],
                0.0
            )
    else:
        daily['volume_ratio'] = 0.0
        daily['prev_amount'] = 0.0

    # 获取股票基础信息
    stock_info = get_stock_basic()
    if not stock_info.empty:
        daily = daily.merge(stock_info[['ts_code', 'name', 'list_date', 'market']],
                            on='ts_code', how='left')

    # 获取ST列表
    st_df = get_st_stock_list(trade_date)
    daily['is_st'] = False
    if not st_df.empty:
        daily['is_st'] = daily['ts_code'].isin(set(st_df['ts_code'].tolist()))
    if 'name' in daily.columns:
        daily['is_st'] = daily['is_st'] | daily['name'].str.upper().str.contains('ST', na=False)

    # 获取涨跌停价格
    lim = get_stk_limit(trade_date=trade_date)
    if not lim.empty:
        daily = daily.merge(lim[['ts_code', 'up_limit', 'down_limit']],
                            on='ts_code', how='left')

    # 获取停复牌信息
    sus = get_suspend_d(trade_date=trade_date, suspend_type='S')
    suspend_codes = set(sus['ts_code'].tolist()) if not sus.empty else set()
    daily['is_suspended'] = daily['ts_code'].isin(suspend_codes)

    return daily


def get_stock_kline_data(ts_code: str, end_date: str, n_days: int = 40) -> pd.DataFrame:
    """
    获取单只股票近N个交易日的日线数据（含前复权）
    用于K线图展示
    """
    # 计算起始日期（多取一些以确保足够交易日）
    start_dt = datetime.strptime(end_date, '%Y%m%d') - timedelta(days=n_days * 2 + 30)
    start_date = start_dt.strftime('%Y%m%d')

    daily = get_daily_data(ts_code=ts_code, start_date=start_date, end_date=end_date)
    if daily.empty:
        return pd.DataFrame()

    # 获取复权因子并计算前复权价格
    adj = get_adj_factor(ts_code=ts_code)
    if not adj.empty:
        daily = daily.merge(adj[['ts_code', 'trade_date', 'adj_factor']],
                            on=['ts_code', 'trade_date'], how='left')
        daily['adj_factor'] = daily['adj_factor'].ffill()
        if 'adj_factor' in daily.columns and daily['adj_factor'].notna().any():
            latest_adj = daily['adj_factor'].iloc[-1] if daily['adj_factor'].iloc[-1] != 0 else 1
            for col in ['open', 'high', 'low', 'close']:
                daily[col] = daily[col] * daily['adj_factor'] / latest_adj

    daily = daily.sort_values('trade_date').tail(n_days).reset_index(drop=True)
    return daily


def get_stock_weekly_kline(ts_code: str, end_date: str, n_weeks: int = 40) -> pd.DataFrame:
    """获取单只股票近N周的周线数据（含前复权）"""
    start_dt = datetime.strptime(end_date, '%Y%m%d') - timedelta(days=n_weeks * 7 + 60)
    start_date = start_dt.strftime('%Y%m%d')

    weekly = get_weekly_data(ts_code=ts_code, start_date=start_date, end_date=end_date)
    if weekly.empty:
        return pd.DataFrame()

    # 获取复权因子并计算前复权价格
    adj = get_adj_factor(ts_code=ts_code)
    if not adj.empty:
        # 将日复权因子映射到周线日期
        weekly = weekly.merge(adj[['ts_code', 'trade_date', 'adj_factor']],
                              on=['ts_code', 'trade_date'], how='left')
        # 前填复权因子
        weekly['adj_factor'] = weekly['adj_factor'].ffill()
        if weekly['adj_factor'].notna().any() and len(weekly) > 0:
            latest_adj = weekly['adj_factor'].iloc[-1] if weekly['adj_factor'].iloc[-1] != 0 else 1
            for col in ['open', 'high', 'low', 'close']:
                if col in weekly.columns:
                    weekly[col] = weekly[col] * weekly['adj_factor'] / latest_adj

    weekly = weekly.sort_values('trade_date').tail(n_weeks).reset_index(drop=True)
    return weekly
