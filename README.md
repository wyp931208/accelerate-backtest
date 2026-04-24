# XPK加速策略 - Streamlit回测与信号提醒系统

基于XPK加速策略的A股回测与信号提醒Web应用，使用Streamlit构建。

## 功能

- **策略回测**: 对加速策略进行历史回测，支持参数调整，展示回测结果并支持下载
- **信号提醒**: 检测前一日加速策略信号，实时提醒潜在交易机会
- **K线图表**: 对信号股票展示近40个交易日的日K/周K、成交量、MACD、PSY图表，支持筛选下载PDF

## 部署

### 本地运行

```bash
pip install -r requirements.txt
streamlit run app.py
```

### Streamlit Cloud部署

1. 将代码上传到GitHub仓库
2. 在[Streamlit Cloud](https://streamlit.io/cloud)创建应用
3. 在Secrets中配置 `TUSHARE_TOKEN`

## 配置

在 `.streamlit/secrets.toml` 中配置你的Tushare API Token：

```toml
TUSHARE_TOKEN = "你的token"
```

## Tushare接口使用

本应用使用以下Tushare Pro接口（需要10000+积分）：

- `pro.daily()` - 日线行情
- `pro.weekly()` - 周线行情
- `pro.stock_basic()` - 股票列表
- `pro.trade_cal()` - 交易日历
- `pro.adj_factor()` - 复权因子
- `pro.daily_basic()` - 每日指标（量比等）
- `pro.stk_limit()` - 每日涨跌停价格
- `pro.suspend_d()` - 停复牌信息
- `pro.stock_st()` - ST股票列表
