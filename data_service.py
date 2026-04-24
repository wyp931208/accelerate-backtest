# -*- coding: utf-8 -*-
"""
Tushare数据服务模块
封装所有Tushare API调用，提供数据缓存和限流控制
"""
import tushare as ts
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import streamlit as st
import time


# 默认Token和API地址（内置，打开网页即可用）
DEFAULT_TOKEN = "hTASoWevdIQVKNJgEUGoDEWIMufHKYuLTSUGZfUOImwssjguKASNmWMywBkFgpjF"
TUSHARE_API_URL = "http://124.222.60.121:8020/"


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


@st.cache_data(ttl=3600, show_spinner=False)
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


@st.cache_data(ttl=3600, show_spinner=False)
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


@st.cache_data(ttl=3600, show_spinner=False)
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


@st.cache_data(ttl=1800, show_spinner=False)
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


@st.cache_data(ttl=1800, show_spinner=False)
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


@st.cache_data(ttl=1800, show_spinner=False)
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


@st.cache_data(ttl=3600, show_spinner=False)
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


@st.cache_data(ttl=3600, show_spinner=False)
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


@st.cache_data(ttl=3600, show_spinner=False)
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


@st.cache_data(ttl=3600, show_spinner=False)
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


def get_daily_data_with_info(start_date: str, end_date: str, progress_placeholder=None) -> pd.DataFrame:
    """
    获取回测所需的全部日线数据（含股票名称、上市日期等）
    分步获取: 日线行情 + 股票基础信息 + 复权因子 + 每日指标 + 涨跌停 + 停复牌 + ST列表
    
    progress_placeholder: 可选的st.progress占位符，如果为None则自动创建
    """
    pro = get_pro_api()
    if pro is None:
        return pd.DataFrame()

    progress = progress_placeholder if progress_placeholder else st.progress(0, text="正在获取日线行情数据...")

    # 1. 获取日线行情（按交易日批量获取）
    cal = get_trade_calendar(start_date, end_date)
    if cal.empty:
        return pd.DataFrame()
    trade_dates = cal[cal['is_open'] == 1]['cal_date'].sort_values().tolist()

    all_daily = []
    for i, td in enumerate(trade_dates):
        progress.progress(int((i + 1) / len(trade_dates) * 30),
                          text=f"正在获取日线行情 {i+1}/{len(trade_dates)}...")
        df = get_daily_data(trade_date=td)
        if not df.empty:
            all_daily.append(df)
        time.sleep(0.15)  # 限流

    if not all_daily:
        return pd.DataFrame()
    daily = pd.concat(all_daily, ignore_index=True)

    # 2. 获取股票基础信息
    progress.progress(35, text="正在获取股票基础信息...")
    stock_info = get_stock_basic()

    # 3. 获取复权因子
    progress.progress(45, text="正在获取复权因子...")
    all_adj = []
    for i, td in enumerate(trade_dates):
        adj = get_adj_factor(trade_date=td)
        if not adj.empty:
            all_adj.append(adj)
        time.sleep(0.15)
    if all_adj:
        adj_df = pd.concat(all_adj, ignore_index=True)
        daily = daily.merge(adj_df[['ts_code', 'trade_date', 'adj_factor']],
                            on=['ts_code', 'trade_date'], how='left')

    # 4. 获取每日指标（量比等）
    progress.progress(60, text="正在获取每日指标...")
    all_basic = []
    for i, td in enumerate(trade_dates):
        db = get_daily_basic(trade_date=td)
        if not db.empty:
            all_basic.append(db[['ts_code', 'trade_date', 'volume_ratio', 'turnover_rate']])
        time.sleep(0.15)
    if all_basic:
        basic_df = pd.concat(all_basic, ignore_index=True)
        daily = daily.merge(basic_df, on=['ts_code', 'trade_date'], how='left')

    # 5. 合并股票基础信息
    progress.progress(75, text="正在合并基础信息...")
    if not stock_info.empty:
        daily = daily.merge(stock_info[['ts_code', 'name', 'list_date', 'market']],
                            on='ts_code', how='left')

    # 6. 获取涨跌停价格
    progress.progress(85, text="正在获取涨跌停价格...")
    all_limits = []
    for i, td in enumerate(trade_dates):
        lim = get_stk_limit(trade_date=td)
        if not lim.empty:
            all_limits.append(lim[['ts_code', 'trade_date', 'up_limit', 'down_limit']])
        time.sleep(0.15)
    if all_limits:
        limit_df = pd.concat(all_limits, ignore_index=True)
        daily = daily.merge(limit_df, on=['ts_code', 'trade_date'], how='left')

    # 7. 获取停复牌信息
    progress.progress(92, text="正在获取停复牌信息...")
    suspend_codes = set()
    for i, td in enumerate(trade_dates):
        sus = get_suspend_d(trade_date=td, suspend_type='S')
        if not sus.empty:
            suspend_codes.update(sus['ts_code'].tolist())
        time.sleep(0.15)
    daily['is_suspended'] = daily['ts_code'].isin(suspend_codes)

    # 8. 获取ST列表
    progress.progress(97, text="正在获取ST股票列表...")
    st_df = get_st_stock_list()
    st_codes = set()
    if not st_df.empty:
        st_codes = set(st_df['ts_code'].tolist())
    daily['is_st'] = daily['ts_code'].isin(st_codes)

    # 补充：名称中含ST也标记
    if 'name' in daily.columns:
        daily['is_st'] = daily['is_st'] | daily['name'].str.upper().str.contains('ST', na=False)

    progress.progress(100, text="数据获取完成！")

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

    # 获取前一交易日数据（计算量比需要前日成交量）
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
            prev_vol = prev_daily[['ts_code', 'vol']].rename(columns={'vol': 'prev_vol'})
            daily = daily.merge(prev_vol, on='ts_code', how='left')
            daily['volume_ratio'] = np.where(
                daily['prev_vol'] > 0,
                daily['vol'] / daily['prev_vol'],
                0.0
            )
    else:
        daily['volume_ratio'] = 0.0
        daily['prev_vol'] = 0.0

    # 获取股票基础信息
    stock_info = get_stock_basic()
    if not stock_info.empty:
        daily = daily.merge(stock_info[['ts_code', 'name', 'list_date', 'market']],
                            on='ts_code', how='left')

    # 获取ST列表
    st_df = get_st_stock_list()
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
    """获取单只股票近N周的周线数据"""
    start_dt = datetime.strptime(end_date, '%Y%m%d') - timedelta(days=n_weeks * 7 + 60)
    start_date = start_dt.strftime('%Y%m%d')

    weekly = get_weekly_data(ts_code=ts_code, start_date=start_date, end_date=end_date)
    if weekly.empty:
        return pd.DataFrame()

    weekly = weekly.sort_values('trade_date').tail(n_weeks).reset_index(drop=True)
    return weekly
