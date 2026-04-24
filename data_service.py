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


def get_daily_data_with_info(start_date: str, end_date: str, progress_placeholder=None, adj_type: str = "qfq") -> pd.DataFrame:
    """
    获取回测所需的全部日线数据（含股票名称、上市日期等）
    优化：使用日期范围批量获取，而非逐日查询
    
    adj_type: 复权方式
        - "qfq": 前复权（以最新价格为基准向前调整，推荐）
        - "hfq": 后复权（以上市首日为基准向后调整）
        - "none": 不复权（使用原始价格）
    
    progress_placeholder: 可选的st.progress占位符，如果为None则自动创建
    """
    pro = get_pro_api()
    if pro is None:
        return pd.DataFrame()

    progress = progress_placeholder if progress_placeholder else st.progress(0, text="正在获取数据...")

    # 1. 获取日线行情（按日期范围一次性获取，Tushare每次最多返回5000行）
    progress.progress(5, text="正在获取日线行情数据（批量模式）...")
    try:
        all_daily = []
        # Tushare daily接口用start_date/end_date可以批量获取
        # 但单次返回可能有限，需要分批（按月分段）
        cal = get_trade_calendar(start_date, end_date)
        if cal.empty:
            return pd.DataFrame()
        trade_dates = cal[cal['is_open'] == 1]['cal_date'].sort_values().tolist()
        
        # 按月分段获取，避免单次返回数据量过大
        from datetime import datetime as dt
        start_dt = dt.strptime(start_date, '%Y%m%d')
        end_dt = dt.strptime(end_date, '%Y%m%d')
        
        # 生成年月分段
        segments = []
        cur_start = start_date
        cur_year, cur_month = start_dt.year, start_dt.month
        while True:
            # 每段最多3个月
            if cur_month + 3 > 12:
                next_year = cur_year + 1
                next_month = (cur_month + 3) - 12
            else:
                next_year = cur_year
                next_month = cur_month + 3
            
            seg_end_dt = dt(next_year, next_month, 1) - timedelta(days=1)
            seg_end = min(seg_end_dt.strftime('%Y%m%d'), end_date)
            segments.append((cur_start, seg_end))
            
            if seg_end >= end_date:
                break
            cur_start = dt(next_year, next_month, 1).strftime('%Y%m%d')
            cur_year, cur_month = next_year, next_month
        
        for i, (seg_s, seg_e) in enumerate(segments):
            progress.progress(int(5 + (i + 1) / len(segments) * 30),
                              text=f"正在获取日线行情 {i+1}/{len(segments)} ({seg_s}~{seg_e})...")
            df = pro.daily(start_date=seg_s, end_date=seg_e)
            if not df.empty:
                all_daily.append(df)
            time.sleep(0.2)
    except Exception as e:
        st.error(f"获取日线数据失败: {e}")
        # 降级：逐日获取
        all_daily = []
        for i, td in enumerate(trade_dates):
            progress.progress(int((i + 1) / len(trade_dates) * 30),
                              text=f"正在获取日线行情(降级模式) {i+1}/{len(trade_dates)}...")
            df = get_daily_data(trade_date=td)
            if not df.empty:
                all_daily.append(df)
            time.sleep(0.15)

    if not all_daily:
        return pd.DataFrame()
    daily = pd.concat(all_daily, ignore_index=True)

    # 2. 获取股票基础信息（一次调用）
    progress.progress(35, text="正在获取股票基础信息...")
    stock_info = get_stock_basic()

    # 3. 获取复权因子（按日期范围批量获取）
    progress.progress(45, text="正在获取复权因子（批量模式）...")
    try:
        adj_dfs = []
        for i, (seg_s, seg_e) in enumerate(segments):
            adj = pro.adj_factor(start_date=seg_s, end_date=seg_e)
            if not adj.empty:
                adj_dfs.append(adj[['ts_code', 'trade_date', 'adj_factor']])
            time.sleep(0.2)
        if adj_dfs:
            adj_df = pd.concat(adj_dfs, ignore_index=True)
            daily = daily.merge(adj_df, on=['ts_code', 'trade_date'], how='left')
    except Exception:
        # 降级：逐日获取
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

    # 4. 获取每日指标（量比等，按日期范围批量获取）
    progress.progress(60, text="正在获取每日指标（批量模式）...")
    try:
        basic_dfs = []
        for i, (seg_s, seg_e) in enumerate(segments):
            db = pro.daily_basic(start_date=seg_s, end_date=seg_e,
                                 fields='ts_code,trade_date,volume_ratio,turnover_rate')
            if not db.empty:
                basic_dfs.append(db[['ts_code', 'trade_date', 'volume_ratio', 'turnover_rate']])
            time.sleep(0.2)
        if basic_dfs:
            basic_df = pd.concat(basic_dfs, ignore_index=True)
            daily = daily.merge(basic_df, on=['ts_code', 'trade_date'], how='left')
    except Exception:
        # 降级：逐日获取
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

    # 6. 获取涨跌停价格（按日期范围批量获取）
    progress.progress(85, text="正在获取涨跌停价格（批量模式）...")
    try:
        limit_dfs = []
        for i, (seg_s, seg_e) in enumerate(segments):
            lim = pro.stk_limit(start_date=seg_s, end_date=seg_e)
            if not lim.empty:
                limit_dfs.append(lim[['ts_code', 'trade_date', 'up_limit', 'down_limit']])
            time.sleep(0.2)
        if limit_dfs:
            limit_df = pd.concat(limit_dfs, ignore_index=True)
            daily = daily.merge(limit_df, on=['ts_code', 'trade_date'], how='left')
    except Exception:
        # 降级：逐日获取
        all_limits = []
        for i, td in enumerate(trade_dates):
            lim = get_stk_limit(trade_date=td)
            if not lim.empty:
                all_limits.append(lim[['ts_code', 'trade_date', 'up_limit', 'down_limit']])
            time.sleep(0.15)
        if all_limits:
            limit_df = pd.concat(all_limits, ignore_index=True)
            daily = daily.merge(limit_df, on=['ts_code', 'trade_date'], how='left')

    # 7. 获取停复牌信息（按日期范围批量获取）
    progress.progress(92, text="正在获取停复牌信息（批量模式）...")
    try:
        sus_dfs = []
        for i, (seg_s, seg_e) in enumerate(segments):
            sus = pro.suspend_d(start_date=seg_s, end_date=seg_e, suspend_type='S')
            if not sus.empty:
                sus_dfs.append(sus)
            time.sleep(0.2)
        suspend_codes = set()
        for sdf in sus_dfs:
            suspend_codes.update(sdf['ts_code'].tolist())
    except Exception:
        # 降级：逐日获取
        suspend_codes = set()
        for i, td in enumerate(trade_dates):
            sus = get_suspend_d(trade_date=td, suspend_type='S')
            if not sus.empty:
                suspend_codes.update(sus['ts_code'].tolist())
            time.sleep(0.15)
    daily['is_suspended'] = daily['ts_code'].isin(suspend_codes)

    # 8. 获取ST列表（一次调用）
    progress.progress(97, text="正在获取ST股票列表...")
    st_df = get_st_stock_list()
    st_codes = set()
    if not st_df.empty:
        st_codes = set(st_df['ts_code'].tolist())
    daily['is_st'] = daily['ts_code'].isin(st_codes)

    # 补充：名称中含ST也标记
    if 'name' in daily.columns:
        daily['is_st'] = daily['is_st'] | daily['name'].str.upper().str.contains('ST', na=False)

    # 9. 根据复权方式调整价格
    progress.progress(99, text=f"正在应用复权方式: {'前复权' if adj_type == 'qfq' else '后复权' if adj_type == 'hfq' else '不复权'}...")
    if adj_type in ('qfq', 'hfq') and 'adj_factor' in daily.columns and daily['adj_factor'].notna().any():
        # 保留原始不复权价格（用于某些需要原始价格的逻辑，如涨跌停判断）
        for col in ['open', 'high', 'low', 'close', 'pre_close']:
            if col in daily.columns:
                daily[f'{col}_bfq'] = daily[col]

        if adj_type == 'qfq':
            # 前复权：每只股票以最新复权因子为基准，向前调整
            for ts_code, group in daily.groupby('ts_code'):
                mask = daily['ts_code'] == ts_code
                adj_factors = daily.loc[mask, 'adj_factor']
                # 找到该股票最新的复权因子
                latest_idx = group['trade_date'].idxmax()
                latest_adj = adj_factors.loc[latest_idx]
                if pd.isna(latest_adj) or latest_adj == 0:
                    continue
                # 前复权价格 = 原始价格 × 当日复权因子 / 最新复权因子
                for col in ['open', 'high', 'low', 'close', 'pre_close']:
                    if col in daily.columns:
                        daily.loc[mask, col] = daily.loc[mask, col] * adj_factors / latest_adj
        elif adj_type == 'hfq':
            # 后复权：以上市首日复权因子为基准，向后调整
            # 后复权价格 = 原始价格 × 当日复权因子 / 上市首日复权因子
            for ts_code, group in daily.groupby('ts_code'):
                mask = daily['ts_code'] == ts_code
                adj_factors = daily.loc[mask, 'adj_factor']
                # 找到该股票最早的复权因子
                earliest_idx = group['trade_date'].idxmin()
                earliest_adj = adj_factors.loc[earliest_idx]
                if pd.isna(earliest_adj) or earliest_adj == 0:
                    continue
                for col in ['open', 'high', 'low', 'close', 'pre_close']:
                    if col in daily.columns:
                        daily.loc[mask, col] = daily.loc[mask, col] * adj_factors / earliest_adj

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
