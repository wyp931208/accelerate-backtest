# -*- coding: utf-8 -*-
"""
K线图和图表生成模块
生成日K线、周K线、成交量柱状图、MACD图、PSY图
"""
import pandas as pd
import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib.patches import FancyBboxPatch
from io import BytesIO
import os

# 设置中文字体（兼容Linux/Streamlit Cloud环境）
def _setup_chinese_font():
    """自动寻找并配置中文字体"""
    import matplotlib.font_manager as fm
    # 常见中文字体路径
    font_candidates = [
        # 项目内字体（优先）
        os.path.join(os.path.dirname(os.path.abspath(__file__)), 'fonts', 'SimHei.ttf'),
        # Windows
        'C:/Windows/Fonts/simhei.ttf',
        'C:/Windows/Fonts/msyh.ttc',
        # macOS
        '/System/Library/Fonts/STHeiti Medium.ttc',
        '/Library/Fonts/Arial Unicode.ttf',
        # Linux
        '/usr/share/fonts/truetype/wqy/wqy-zenhei.ttc',
        '/usr/share/fonts/truetype/wqy/wqy-microhei.ttc',
        '/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc',
        '/usr/share/fonts/truetype/droid/DroidSansFallbackFull.ttf',
        '/usr/share/fonts/noto-cjk/NotoSansCJK-Regular.ttc',
    ]
    for fp in font_candidates:
        if os.path.exists(fp):
            try:
                fm.fontManager.addfont(fp)
                prop = fm.FontProperties(fname=fp)
                plt.rcParams['font.sans-serif'] = [prop.get_name()] + plt.rcParams['font.sans-serif']
                return
            except Exception:
                continue
    # 最后兜底：查找系统中所有可用中文字体
    for f in fm.fontManager.ttflist:
        if any(kw in f.name.lower() for kw in ['cjk', 'chinese', 'simhei', 'yahei', 'wqy', 'noto sans sc', 'heiti', 'songti']):
            plt.rcParams['font.sans-serif'] = [f.name] + plt.rcParams['font.sans-serif']
            return

_setup_chinese_font()
plt.rcParams['axes.unicode_minus'] = False


def plot_daily_kline_with_indicators(df: pd.DataFrame, ts_code: str = "",
                                      stock_name: str = "") -> BytesIO:
    """
    绘制日K线 + 成交量 + MACD + PSY 四合一图表
    返回BytesIO对象（PNG格式）
    """
    from indicators import add_all_indicators

    df = add_all_indicators(df)
    df = df.copy()
    df['trade_date'] = pd.to_datetime(df['trade_date'])
    df = df.sort_values('trade_date').reset_index(drop=True)

    # 创建4行子图：K线、成交量、MACD、PSY
    fig, axes = plt.subplots(4, 1, figsize=(14, 12),
                              gridspec_kw={'height_ratios': [4, 1.5, 1.5, 1]},
                              sharex=True)
    fig.suptitle(f'{stock_name} ({ts_code}) 日K线与技术指标',
                 fontsize=14, fontweight='bold', y=0.98)

    ax_kline = axes[0]
    ax_vol = axes[1]
    ax_macd = axes[2]
    ax_psy = axes[3]

    x = np.arange(len(df))
    dates = df['trade_date']

    # ---- K线图 ----
    for i in range(len(df)):
        open_p = df.iloc[i]['open']
        close_p = df.iloc[i]['close']
        high_p = df.iloc[i]['high']
        low_p = df.iloc[i]['low']

        color = '#ef5350' if close_p >= open_p else '#26a69a'

        # 实体
        ax_kline.plot([i, i], [low_p, high_p], color=color, linewidth=0.8)
        ax_kline.plot([i, i], [min(open_p, close_p), max(open_p, close_p)],
                      color=color, linewidth=4)

    # MA线
    for ma_col in ['MA5', 'MA10', 'MA20']:
        if ma_col in df.columns:
            ax_kline.plot(x, df[ma_col], linewidth=1, label=ma_col, alpha=0.8)

    ax_kline.legend(loc='upper left', fontsize=8)
    ax_kline.set_ylabel('价格', fontsize=9)
    ax_kline.grid(True, alpha=0.3)
    ax_kline.set_title('日K线', fontsize=10, loc='left')

    # ---- 成交量柱状图 ----
    colors_vol = ['#ef5350' if df.iloc[i]['close'] >= df.iloc[i]['open']
                  else '#26a69a' for i in range(len(df))]
    ax_vol.bar(x, df['vol'], color=colors_vol, width=0.7, alpha=0.8)

    if 'VOL_MA5' in df.columns:
        ax_vol.plot(x, df['VOL_MA5'], color='#FF9800', linewidth=1, label='VOL_MA5')
    if 'VOL_MA10' in df.columns:
        ax_vol.plot(x, df['VOL_MA10'], color='#2196F3', linewidth=1, label='VOL_MA10')

    ax_vol.legend(loc='upper left', fontsize=7)
    ax_vol.set_ylabel('成交量', fontsize=9)
    ax_vol.grid(True, alpha=0.3)
    ax_vol.set_title('成交量', fontsize=10, loc='left')

    # ---- MACD图 ----
    if 'MACD_HIST' in df.columns:
        colors_macd = ['#ef5350' if v >= 0 else '#26a69a' for v in df['MACD_HIST']]
        ax_macd.bar(x, df['MACD_HIST'], color=colors_macd, width=0.7, alpha=0.8)
    if 'MACD_DIF' in df.columns:
        ax_macd.plot(x, df['MACD_DIF'], color='#FF9800', linewidth=1, label='DIF')
    if 'MACD_DEA' in df.columns:
        ax_macd.plot(x, df['MACD_DEA'], color='#2196F3', linewidth=1, label='DEA')

    ax_macd.axhline(y=0, color='gray', linewidth=0.5, linestyle='--')
    ax_macd.legend(loc='upper left', fontsize=7)
    ax_macd.set_ylabel('MACD', fontsize=9)
    ax_macd.grid(True, alpha=0.3)
    ax_macd.set_title('MACD', fontsize=10, loc='left')

    # ---- PSY图 ----
    if 'PSY' in df.columns:
        ax_psy.plot(x, df['PSY'], color='#9C27B0', linewidth=1.2, label='PSY(12)')
        ax_psy.axhline(y=75, color='#ef5350', linewidth=0.8, linestyle='--', alpha=0.7, label='超买(75)')
        ax_psy.axhline(y=25, color='#26a69a', linewidth=0.8, linestyle='--', alpha=0.7, label='超卖(25)')
        ax_psy.fill_between(x, 25, 75, alpha=0.05, color='gray')

    ax_psy.legend(loc='upper left', fontsize=7)
    ax_psy.set_ylabel('PSY', fontsize=9)
    ax_psy.set_xlabel('交易日期', fontsize=9)
    ax_psy.grid(True, alpha=0.3)
    ax_psy.set_title('PSY心理线', fontsize=10, loc='left')

    # 设置x轴日期标签
    tick_positions = np.linspace(0, len(df) - 1, min(8, len(df))).astype(int)
    tick_labels = [dates.iloc[i].strftime('%m-%d') if i < len(dates) else '' for i in tick_positions]
    ax_psy.set_xticks(tick_positions)
    ax_psy.set_xticklabels(tick_labels, rotation=30, fontsize=8)

    plt.tight_layout(rect=[0, 0, 1, 0.96])

    buf = BytesIO()
    fig.savefig(buf, format='png', dpi=150, bbox_inches='tight')
    plt.close(fig)
    buf.seek(0)
    return buf


def plot_weekly_kline_with_indicators(df: pd.DataFrame, ts_code: str = "",
                                       stock_name: str = "") -> BytesIO:
    """
    绘制周K线 + 成交量 + MACD + PSY 四合一图表
    """
    from indicators import add_all_indicators

    df = df.copy()
    # 周线数据字段可能不完全，补齐必要的列
    if 'MACD_DIF' not in df.columns:
        df = add_all_indicators(df)

    df['trade_date'] = pd.to_datetime(df['trade_date'])
    df = df.sort_values('trade_date').reset_index(drop=True)

    fig, axes = plt.subplots(4, 1, figsize=(14, 12),
                              gridspec_kw={'height_ratios': [4, 1.5, 1.5, 1]},
                              sharex=True)
    fig.suptitle(f'{stock_name} ({ts_code}) 周K线与技术指标',
                 fontsize=14, fontweight='bold', y=0.98)

    ax_kline = axes[0]
    ax_vol = axes[1]
    ax_macd = axes[2]
    ax_psy = axes[3]

    x = np.arange(len(df))
    dates = df['trade_date']

    # ---- K线图 ----
    for i in range(len(df)):
        open_p = df.iloc[i]['open']
        close_p = df.iloc[i]['close']
        high_p = df.iloc[i]['high']
        low_p = df.iloc[i]['low']
        color = '#ef5350' if close_p >= open_p else '#26a69a'
        ax_kline.plot([i, i], [low_p, high_p], color=color, linewidth=0.8)
        ax_kline.plot([i, i], [min(open_p, close_p), max(open_p, close_p)],
                      color=color, linewidth=4)

    for ma_col in ['MA5', 'MA10', 'MA20']:
        if ma_col in df.columns:
            ax_kline.plot(x, df[ma_col], linewidth=1, label=ma_col, alpha=0.8)

    ax_kline.legend(loc='upper left', fontsize=8)
    ax_kline.set_ylabel('价格', fontsize=9)
    ax_kline.grid(True, alpha=0.3)
    ax_kline.set_title('周K线', fontsize=10, loc='left')

    # ---- 成交量柱状图 ----
    colors_vol = ['#ef5350' if df.iloc[i]['close'] >= df.iloc[i]['open']
                  else '#26a69a' for i in range(len(df))]
    ax_vol.bar(x, df['vol'], color=colors_vol, width=0.7, alpha=0.8)
    if 'VOL_MA5' in df.columns:
        ax_vol.plot(x, df['VOL_MA5'], color='#FF9800', linewidth=1, label='VOL_MA5')
    ax_vol.legend(loc='upper left', fontsize=7)
    ax_vol.set_ylabel('成交量', fontsize=9)
    ax_vol.grid(True, alpha=0.3)
    ax_vol.set_title('成交量', fontsize=10, loc='left')

    # ---- MACD图 ----
    if 'MACD_HIST' in df.columns:
        colors_macd = ['#ef5350' if v >= 0 else '#26a69a' for v in df['MACD_HIST']]
        ax_macd.bar(x, df['MACD_HIST'], color=colors_macd, width=0.7, alpha=0.8)
    if 'MACD_DIF' in df.columns:
        ax_macd.plot(x, df['MACD_DIF'], color='#FF9800', linewidth=1, label='DIF')
    if 'MACD_DEA' in df.columns:
        ax_macd.plot(x, df['MACD_DEA'], color='#2196F3', linewidth=1, label='DEA')
    ax_macd.axhline(y=0, color='gray', linewidth=0.5, linestyle='--')
    ax_macd.legend(loc='upper left', fontsize=7)
    ax_macd.set_ylabel('MACD', fontsize=9)
    ax_macd.grid(True, alpha=0.3)
    ax_macd.set_title('MACD', fontsize=10, loc='left')

    # ---- PSY图 ----
    if 'PSY' in df.columns:
        ax_psy.plot(x, df['PSY'], color='#9C27B0', linewidth=1.2, label='PSY(12)')
        ax_psy.axhline(y=75, color='#ef5350', linewidth=0.8, linestyle='--', alpha=0.7, label='超买(75)')
        ax_psy.axhline(y=25, color='#26a69a', linewidth=0.8, linestyle='--', alpha=0.7, label='超卖(25)')
        ax_psy.fill_between(x, 25, 75, alpha=0.05, color='gray')
    ax_psy.legend(loc='upper left', fontsize=7)
    ax_psy.set_ylabel('PSY', fontsize=9)
    ax_psy.set_xlabel('交易日期', fontsize=9)
    ax_psy.grid(True, alpha=0.3)
    ax_psy.set_title('PSY心理线', fontsize=10, loc='left')

    tick_positions = np.linspace(0, len(df) - 1, min(8, len(df))).astype(int)
    tick_labels = [dates.iloc[i].strftime('%Y-%m-%d') if i < len(dates) else '' for i in tick_positions]
    ax_psy.set_xticks(tick_positions)
    ax_psy.set_xticklabels(tick_labels, rotation=30, fontsize=8)

    plt.tight_layout(rect=[0, 0, 1, 0.96])

    buf = BytesIO()
    fig.savefig(buf, format='png', dpi=150, bbox_inches='tight')
    plt.close(fig)
    buf.seek(0)
    return buf
