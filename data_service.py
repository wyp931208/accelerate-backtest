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
from concurrent.futures import ThreadPoolExecutor, as_completed


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


def get_daily_data_with_info(start_date: str, end_date: str, progress_placeholder=None, adj_type: str = "qfq") -> pd.DataFrame:
    """
    获取回测所需的全部日线数据（含股票名称、上市日期等）
    核心策略：按交易日逐天获取，避免Tushare 5000行限制导致数据截断
    
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

    # 获取交易日历
    cal = get_trade_calendar(start_date, end_date)
    if cal.empty:
        return pd.DataFrame()
    trade_dates = cal[cal['is_open'] == 1]['cal_date'].sort_values().tolist()
    n_dates = len(trade_dates)

    # ========== 第1步：按交易日获取日线行情 ==========
    # Tushare按日期范围查询每次最多返回5000行，全市场一天约5000只股票
    # 所以按天获取是唯一可靠方案，避免数据截断
    progress.progress(5, text=f"正在按交易日获取日线行情（共{n_dates}个交易日）...")

    def _fetch_one_day(td):
        """获取单日全市场日线数据"""
        try:
            df = pro.daily(trade_date=td)
            time.sleep(0.08)
            return df if df is not None and not df.empty else None
        except Exception:
            return None

    all_daily = []
    # 使用线程池并行获取（4线程），每天一次API调用
    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = {executor.submit(_fetch_one_day, td): i for i, td in enumerate(trade_dates)}
        for i, future in enumerate(as_completed(futures)):
            pct = int(5 + (i + 1) / n_dates * 35)
            progress.progress(pct, text=f"正在获取日线行情 {i+1}/{n_dates}...")
            result = future.result()
            if result is not None:
                all_daily.append(result)

    if not all_daily:
        return pd.DataFrame()
    daily = pd.concat(all_daily, ignore_index=True)
    # 去重（防止日期边界重复获取）
    daily = daily.drop_duplicates(subset=['ts_code', 'trade_date'], keep='first')

    total_rows = len(daily)
    progress.progress(40, text=f"日线行情获取完成，共{total_rows}条记录")

    # ========== 第2步：获取股票基础信息 ==========
    progress.progress(42, text="正在获取股票基础信息...")
    stock_info = get_stock_basic()

    # ========== 第3-5步：按交易日并行获取复权因子/每日指标/涨跌停 ==========
    progress.progress(45, text=f"正在按交易日获取辅助数据（复权/量比/涨跌停）...")

    def _fetch_adj_one_day(td):
        try:
            adj = pro.adj_factor(trade_date=td)
            time.sleep(0.08)
            return ('adj', adj[['ts_code', 'trade_date', 'adj_factor']]) if adj is not None and not adj.empty else None
        except Exception:
            return None

    def _fetch_basic_one_day(td):
        try:
            db = pro.daily_basic(trade_date=td,
                                 fields='ts_code,trade_date,volume_ratio,turnover_rate')
            time.sleep(0.08)
            return ('basic', db[['ts_code', 'trade_date', 'volume_ratio', 'turnover_rate']]) if db is not None and not db.empty else None
        except Exception:
            return None

    def _fetch_limit_one_day(td):
        try:
            lim = pro.stk_limit(trade_date=td)
            time.sleep(0.08)
            return ('limit', lim[['ts_code', 'trade_date', 'up_limit', 'down_limit']]) if lim is not None and not lim.empty else None
        except Exception:
            return None

    adj_result = []
    basic_result = []
    limit_result = []

    # 每天并行获取3类数据，天与天之间串行
    for i, td in enumerate(trade_dates):
        pct = int(45 + (i + 1) / n_dates * 40)
        progress.progress(pct, text=f"正在获取辅助数据 {i+1}/{n_dates}（{td}）...")
        with ThreadPoolExecutor(max_workers=3) as executor:
            futures = [
                executor.submit(_fetch_adj_one_day, td),
                executor.submit(_fetch_basic_one_day, td),
                executor.submit(_fetch_limit_one_day, td),
            ]
            for future in as_completed(futures):
                result = future.result()
                if result is not None:
                    data_type, df = result
                    if data_type == 'adj':
                        adj_result.append(df)
                    elif data_type == 'basic':
                        basic_result.append(df)
                    elif data_type == 'limit':
                        limit_result.append(df)

    # ========== 第6步：合并所有数据 ==========
    progress.progress(87, text="正在合并数据...")

    # 合并复权因子
    if adj_result:
        adj_df = pd.concat(adj_result, ignore_index=True).drop_duplicates(subset=['ts_code', 'trade_date'], keep='first')
        daily = daily.merge(adj_df, on=['ts_code', 'trade_date'], how='left')

    # 合并每日指标
    if basic_result:
        basic_df = pd.concat(basic_result, ignore_index=True).drop_duplicates(subset=['ts_code', 'trade_date'], keep='first')
        daily = daily.merge(basic_df, on=['ts_code', 'trade_date'], how='left')

    # 合并股票基础信息
    if not stock_info.empty:
        daily = daily.merge(stock_info[['ts_code', 'name', 'list_date', 'market']],
                            on='ts_code', how='left')

    # 合并涨跌停价格
    if limit_result:
        limit_df = pd.concat(limit_result, ignore_index=True).drop_duplicates(subset=['ts_code', 'trade_date'], keep='first')
        daily = daily.merge(limit_df, on=['ts_code', 'trade_date'], how='left')

    # ========== 第7步：获取停复牌信息 ==========
    progress.progress(92, text="正在获取停复牌信息...")
    suspend_codes = set()
    # 按天获取停牌信息（数量少，也避免截断）
    for i, td in enumerate(trade_dates):
        try:
            sus = pro.suspend_d(trade_date=td, suspend_type='S')
            time.sleep(0.08)
            if sus is not None and not sus.empty:
                suspend_codes.update(sus['ts_code'].tolist())
        except Exception:
            pass
        if (i + 1) % 10 == 0:
            progress.progress(int(92 + (i + 1) / n_dates * 3), text=f"正在获取停复牌信息 {i+1}/{n_dates}...")

    daily['is_suspended'] = daily['ts_code'].isin(suspend_codes)

    # ========== 第8步：获取ST列表 ==========
    progress.progress(96, text="正在获取ST股票列表...")
    st_df = get_st_stock_list()
    st_codes = set()
    if not st_df.empty:
        st_codes = set(st_df['ts_code'].tolist())
    daily['is_st'] = daily['ts_code'].isin(st_codes)

    # 补充：名称中含ST也标记
    if 'name' in daily.columns:
        daily['is_st'] = daily['is_st'] | daily['name'].str.upper().str.contains('ST', na=False)

    # ========== 第9步：根据复权方式调整价格 ==========
    progress.progress(98, text=f"正在应用复权方式: {'前复权' if adj_type == 'qfq' else '后复权' if adj_type == 'hfq' else '不复权'}...")
    if adj_type in ('qfq', 'hfq') and 'adj_factor' in daily.columns and daily['adj_factor'].notna().any():
        # 保留原始不复权价格（用于某些需要原始价格的逻辑，如涨跌停判断）
        for col in ['open', 'high', 'low', 'close', 'pre_close']:
            if col in daily.columns:
                daily[f'{col}_bfq'] = daily[col]

        if adj_type == 'qfq':
            # 前复权：每只股票以最新复权因子为基准，向前调整（向量化）
            latest_adj_map = daily.groupby('ts_code')['adj_factor'].transform('last')
            valid_mask = latest_adj_map.notna() & (latest_adj_map != 0)
            for col in ['open', 'high', 'low', 'close', 'pre_close']:
                if col in daily.columns:
                    daily.loc[valid_mask, col] = daily.loc[valid_mask, col] * daily.loc[valid_mask, 'adj_factor'] / latest_adj_map[valid_mask]
        elif adj_type == 'hfq':
            # 后复权：以上市首日复权因子为基准，向后调整（向量化）
            earliest_adj_map = daily.groupby('ts_code')['adj_factor'].transform('first')
            valid_mask = earliest_adj_map.notna() & (earliest_adj_map != 0)
            for col in ['open', 'high', 'low', 'close', 'pre_close']:
                if col in daily.columns:
                    daily.loc[valid_mask, col] = daily.loc[valid_mask, col] * daily.loc[valid_mask, 'adj_factor'] / earliest_adj_map[valid_mask]

    progress.progress(100, text=f"数据获取完成！共{len(daily)}条记录，{daily['ts_code'].nunique()}只股票")

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
