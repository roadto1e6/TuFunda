"""
个股基本面数据获取核心模块

包含：
- 接口配置 & 字段映射（常量）
- RateLimiter    限流器
- MarketFetcher  大盘/行业数据获取
- StockFetcher   个股数据批量获取
"""

import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from pathlib import Path
from typing import Union

import pandas as pd
import tushare as ts

# ==================== 公共常量 ====================

OUTPUT_DIR = Path("./output")

MAX_WORKERS = 8  # 并发线程上限，避免大量股票时线程爆炸

# ==================== 个股接口配置 ====================

INTERFACES = [
    ("stock_company",    "01_公司信息",   "ts_code,com_name,chairman,manager,reg_capital,setup_date,province,city,employees,main_business", "simple"),
    ("stk_rewards",      "02_管理层",     "ts_code,ann_date,end_date,name,title,reward,hold_vol", "simple"),
    ("daily",            "03_日线行情",   "ts_code,trade_date,open,high,low,close,pre_close,change,pct_chg,vol,amount", "date"),
    ("adj_factor",       "03_日线行情",   "ts_code,trade_date,adj_factor", "date"),
    ("daily_basic",      "04_每日指标",   "ts_code,trade_date,turnover_rate,pe,pe_ttm,pb,ps,total_mv,circ_mv", "date"),
    ("income",           "05_利润表",     "ts_code,ann_date,end_date,basic_eps,total_revenue,revenue,operate_profit,n_income", "date"),
    ("balancesheet",     "06_资产负债表", "ts_code,ann_date,end_date,money_cap,total_assets,total_liab,total_hldr_eqy_exc_min_int", "date"),
    ("cashflow",         "07_现金流量表", "ts_code,ann_date,end_date,net_profit,n_cashflow_act,n_cashflow_inv_act,n_cash_flows_fnc_act", "date"),
    ("fina_indicator",   "08_财务指标",   "ts_code,ann_date,end_date,eps,bps,roe,roa,netprofit_margin,grossprofit_margin,debt_to_assets,or_yoy,netprofit_yoy", "date"),
    ("forecast",         "09_业绩预告",   "ts_code,ann_date,end_date,type,p_change_min,p_change_max,net_profit_min,net_profit_max", "date"),
    ("dividend",         "10_分红送股",   "ts_code,end_date,ann_date,stk_div,cash_div,record_date,ex_date", "simple"),
    ("fina_mainbz",      "11_主营构成",   "ts_code,end_date,bz_item,bz_sales,bz_profit", "date"),
    ("top10_holders",    "12_十大股东",   "ts_code,ann_date,end_date,holder_name,hold_amount,hold_ratio", "date"),
    ("stk_holdernumber", "13_股东户数",   "ts_code,ann_date,end_date,holder_num", "date"),
    ("pledge_stat",      "14_股权质押",   "ts_code,end_date,pledge_count,unrest_pledge,pledge_ratio", "simple"),
    ("stk_holdertrade",  "15_股东增减持", "ts_code,ann_date,holder_name,in_de,change_vol,change_ratio", "date"),
    ("block_trade",      "16_大宗交易",   "ts_code,trade_date,price,vol,amount,buyer,seller", "date"),
    ("moneyflow_hsgt",   "17_北向资金",   "trade_date,hgt,sgt,north_money", "market"),
    ("moneyflow_ths",    "18_资金流向",   "trade_date,ts_code,net_amount,buy_lg_amount,buy_md_amount,buy_sm_amount", "date"),
    ("margin_detail",    "19_融资融券",   "trade_date,ts_code,rzye,rqye,rzrqye", "date"),
    ("cyq_perf",         "20_筹码分布",   "ts_code,trade_date,cost_5pct,cost_50pct,cost_95pct,winner_rate", "date"),
    ("stk_factor_pro",   "21_技术因子",   "ts_code,trade_date,ma_qfq_5,ma_qfq_20,ma_qfq_60,macd_qfq,kdj_k_qfq,rsi_qfq_6", "date"),
    ("report_rc",        "22_券商预测",   "ts_code,report_date,org_name,quarter,eps,pe,rating", "date"),
    ("stk_surv",         "23_机构调研",   "ts_code,surv_date,fund_visitors,rece_org", "date"),
    ("limit_list_d",     "24_涨跌停",     "trade_date,ts_code,close,pct_chg,limit_times,limit", "date"),
]

FIELD_MAP = {
    "ts_code": "股票代码", "trade_date": "交易日期", "ann_date": "公告日期", "end_date": "报告期",
    "com_name": "公司名称", "chairman": "董事长", "manager": "总经理", "reg_capital": "注册资本(万)",
    "setup_date": "成立日期", "province": "省份", "city": "城市", "employees": "员工人数",
    "main_business": "主营业务", "name": "姓名", "title": "职务", "reward": "薪酬(万)",
    "hold_vol": "持股数", "open": "开盘价", "high": "最高价", "low": "最低价", "close": "收盘价",
    "pre_close": "昨收价", "change": "涨跌额", "pct_chg": "涨跌幅(%)", "vol": "成交量(手)",
    "amount": "成交额(千元)", "adj_factor": "复权因子", "turnover_rate": "换手率(%)",
    "pe": "市盈率", "pe_ttm": "市盈率TTM", "pb": "市净率", "ps": "市销率",
    "total_mv": "总市值(万)", "circ_mv": "流通市值(万)", "basic_eps": "每股收益",
    "total_revenue": "营业总收入", "revenue": "营业收入", "operate_profit": "营业利润",
    "n_income": "净利润", "money_cap": "货币资金", "total_assets": "总资产",
    "total_liab": "总负债", "total_hldr_eqy_exc_min_int": "股东权益", "net_profit": "净利润",
    "n_cashflow_act": "经营现金流", "n_cashflow_inv_act": "投资现金流", "n_cash_flows_fnc_act": "筹资现金流",
    "eps": "每股收益", "bps": "每股净资产", "roe": "ROE(%)", "roa": "ROA(%)",
    "netprofit_margin": "净利率(%)", "grossprofit_margin": "毛利率(%)", "debt_to_assets": "负债率(%)",
    "or_yoy": "营收增长(%)", "netprofit_yoy": "净利增长(%)", "type": "预告类型",
    "p_change_min": "变动下限(%)", "p_change_max": "变动上限(%)", "net_profit_min": "净利下限(万)",
    "net_profit_max": "净利上限(万)", "stk_div": "每股送转", "cash_div": "每股分红",
    "record_date": "股权登记日", "ex_date": "除权除息日", "bz_item": "业务名称",
    "bz_sales": "营业收入(元)", "bz_profit": "营业利润(元)", "holder_name": "股东名称",
    "hold_amount": "持股数", "hold_ratio": "持股比例(%)", "holder_num": "股东户数",
    "pledge_count": "质押次数", "unrest_pledge": "无限售质押(万)", "pledge_ratio": "质押比例(%)",
    "in_de": "增减持", "change_vol": "变动数量", "change_ratio": "变动比例(%)",
    "price": "成交价", "buyer": "买方", "seller": "卖方", "hgt": "沪股通(百万)",
    "sgt": "深股通(百万)", "north_money": "北向资金(百万)", "net_amount": "净流入(万)",
    "buy_lg_amount": "大单买入(万)", "buy_md_amount": "中单买入(万)", "buy_sm_amount": "小单买入(万)",
    "rzye": "融资余额", "rqye": "融券余额", "rzrqye": "两融余额",
    "cost_5pct": "5%成本", "cost_50pct": "50%成本", "cost_95pct": "95%成本", "winner_rate": "胜率(%)",
    "ma_qfq_5": "MA5", "ma_qfq_20": "MA20", "ma_qfq_60": "MA60", "macd_qfq": "MACD",
    "kdj_k_qfq": "KDJ_K", "rsi_qfq_6": "RSI6", "report_date": "报告日期", "org_name": "机构",
    "quarter": "季度", "rating": "评级", "surv_date": "调研日期", "fund_visitors": "参与机构",
    "rece_org": "接待机构", "limit_times": "连板数", "limit": "涨跌停标识",
}

# ==================== 大盘配置 ====================

CSI300 = "000300.SH"

MARKET_FIELD_MAP = {
    "ts_code": "指数代码", "trade_date": "交易日期",
    "open": "开盘点位", "high": "最高点位", "low": "最低点位", "close": "收盘点位",
    "pre_close": "昨收点位", "change": "涨跌点", "pct_chg": "涨跌幅(%)",
    "vol": "成交量(手)", "amount": "成交额(千元)",
    "total_mv": "总市值(元)", "float_mv": "流通市值(元)",
    "total_share": "总股本(股)", "float_share": "流通股本(股)",
    "turnover_rate": "换手率(%)", "turnover_rate_f": "自由流通换手率(%)",
    "pe": "市盈率", "pe_ttm": "市盈率TTM", "pb": "市净率",
}

SW_FIELD_MAP = {
    "ts_code": "行业代码", "trade_date": "交易日期", "name": "行业名称",
    "open": "开盘点位", "close": "收盘点位", "high": "最高点位", "low": "最低点位",
    "change": "涨跌点", "pct_change": "涨跌幅(%)",
    "vol": "成交量(万股)", "amount": "成交额(万元)",
    "pe": "市盈率", "pb": "市净率",
    "float_mv": "流通市值(万元)", "total_mv": "总市值(万元)",
}


# ==================== 工具类 ====================

class RateLimiter:
    """全局 API 限流器"""

    def __init__(self, interval: float = 0.35):
        self.interval = interval
        self._lock = threading.Lock()
        self._last = 0.0

    def wait(self):
        with self._lock:
            elapsed = time.time() - self._last
            if elapsed < self.interval:
                time.sleep(self.interval - elapsed)
            self._last = time.time()


# ==================== 大盘数据获取 ====================

class MarketFetcher:
    """
    大盘背景数据获取器。

    共三张表，沪深300数据所有个股共享（只拉一次），申万行业按个股精准匹配：
      index_daily      → 25_大盘日线
      index_dailybasic → 26_大盘估值
      sw_daily         → 27_申万行业
    """

    SHEET_NAMES = {
        "index_daily":      "25_大盘日线",
        "index_dailybasic": "26_大盘估值",
        "sw_daily":         "27_申万行业",
    }

    _shared_cache: dict[str, pd.DataFrame] = {}
    _cache_lock = threading.Lock()

    def __init__(self, pro, limiter: RateLimiter, log: logging.Logger):
        self.pro = pro
        self.limiter = limiter
        self.log = log

    # ---------- 对外接口 ----------

    def fetch_shared(self, start_date: str, end_date: str) -> None:
        """预拉沪深300数据并缓存，所有个股共享，只执行一次"""
        with self._cache_lock:
            if self._shared_cache:
                return
            df = self._fetch_index_daily(start_date, end_date)
            if not df.empty:
                self._shared_cache["index_daily"] = self._sort(self._rename(df, MARKET_FIELD_MAP))
                self.log.info(f"  ✓ 大盘日线: {len(df):,} 条")

            df = self._fetch_index_dailybasic(start_date, end_date)
            if not df.empty:
                self._shared_cache["index_dailybasic"] = self._sort(self._rename(df, MARKET_FIELD_MAP))
                self.log.info(f"  ✓ 大盘估值: {len(df):,} 条")

    def get_sheets(self, stock_code: str, start_date: str, end_date: str) -> dict[str, pd.DataFrame]:
        """返回该个股完整的大盘背景数据"""
        result = dict(self._shared_cache)

        l1_code, l1_name = self._get_sw_l1(stock_code)
        if l1_code:
            df = self._fetch_sw_daily(l1_code, start_date, end_date)
            if not df.empty:
                result["sw_daily"] = self._sort(self._rename(df, SW_FIELD_MAP))
                self.log.info(f"  ✓ {stock_code} 申万行业: {l1_name}({l1_code}) | {len(df):,} 条")
        else:
            self.log.warning(f"  - {stock_code} 未找到申万一级行业，跳过")

        return result

    @classmethod
    def clear_cache(cls):
        """清空共享缓存（新任务开始前调用）"""
        with cls._cache_lock:
            cls._shared_cache.clear()

    # ---------- 申万行业查询 ----------

    def _get_sw_l1(self, stock_code: str) -> tuple:
        self.limiter.wait()
        try:
            df = self.pro.index_member_all(
                ts_code=stock_code, is_new="Y",
                fields="l1_code,l1_name",
            )
            if df is not None and not df.empty:
                row = df.iloc[0]
                return row["l1_code"], row["l1_name"]
        except Exception as e:
            self.log.warning(f"  ✗ index_member_all({stock_code}): {str(e)[:80]}")
        return None, None

    # ---------- 各接口拉取 ----------

    def _fetch_index_daily(self, start: str, end: str) -> pd.DataFrame:
        return self._call(
            "index_daily",
            ts_code=CSI300, start_date=start, end_date=end,
            fields="ts_code,trade_date,open,high,low,close,pre_close,change,pct_chg,vol,amount",
        )

    def _fetch_index_dailybasic(self, start: str, end: str) -> pd.DataFrame:
        return self._call(
            "index_dailybasic",
            ts_code=CSI300, start_date=start, end_date=end,
            fields="ts_code,trade_date,total_mv,float_mv,total_share,float_share,turnover_rate,turnover_rate_f,pe,pe_ttm,pb",
        )

    def _fetch_sw_daily(self, l1_code: str, start: str, end: str) -> pd.DataFrame:
        return self._call(
            "sw_daily",
            ts_code=l1_code, start_date=start, end_date=end,
            fields="ts_code,trade_date,name,open,close,high,low,change,pct_change,vol,amount,pe,pb,float_mv,total_mv",
        )

    # ---------- 工具 ----------

    def _call(self, api_name: str, **kwargs) -> pd.DataFrame:
        self.limiter.wait()
        try:
            result = getattr(self.pro, api_name)(**kwargs)
            return result if result is not None else pd.DataFrame()
        except Exception as e:
            self.log.warning(f"  ✗ {api_name}: {str(e)[:100]}")
            return pd.DataFrame()

    @staticmethod
    def _rename(df: pd.DataFrame, mapping: dict) -> pd.DataFrame:
        df = df.copy()
        df.columns = [mapping.get(c, c) for c in df.columns]
        return df

    @staticmethod
    def _sort(df: pd.DataFrame) -> pd.DataFrame:
        if "交易日期" in df.columns:
            df = df.sort_values("交易日期", ascending=False).reset_index(drop=True)
        return df


# ==================== 个股数据获取 ====================

class StockFetcher:
    """个股基本面批量获取器"""

    def __init__(self, token: str, log: logging.Logger | None = None):
        self.pro = ts.pro_api(token)
        self.limiter = RateLimiter()
        self._lock = threading.Lock()
        self.done = 0
        self.market_fetcher: MarketFetcher | None = None
        self.save_dir: Path | None = None  # 本次任务的实际输出目录

        if log is not None:
            self.log = log
        else:
            logging.basicConfig(
                format="%(asctime)s | %(message)s",
                datefmt="%H:%M:%S",
                level=logging.INFO,
            )
            self.log = logging.getLogger("stock_fetcher")

    def fetch(
        self,
        codes: Union[str, list[str]],
        start_date: str | None = None,
        end_date: str | None = None,
        save_path: str = str(OUTPUT_DIR),
        years: int = 3,
    ) -> dict:
        if isinstance(codes, str):
            codes = [c.strip() for c in codes.split(",") if c.strip()]

        # 一次性捕获当前时间，避免跨午夜不一致
        now = datetime.now()
        end_date = end_date or now.strftime("%Y%m%d")
        start_date = start_date or (now - timedelta(days=365 * years)).strftime("%Y%m%d")
        today = now.strftime("%Y%m%d")

        self.save_dir = Path(save_path) / today
        self.save_dir.mkdir(parents=True, exist_ok=True)

        total = len(codes)
        days = (datetime.strptime(end_date, "%Y%m%d") - datetime.strptime(start_date, "%Y%m%d")).days
        self.log.info("=" * 60)
        self.log.info(f"股票: {total}只 | 并发: {min(total, MAX_WORKERS)}线程 | 周期: {days}天")
        self.log.info(f"数据: {start_date} ~ {end_date}")
        self.log.info(f"保存: {self.save_dir}/")
        self.log.info("=" * 60)

        if not codes:
            return {}

        # ① 预拉沪深300共享数据（只拉一次）
        self.market_fetcher = MarketFetcher(self.pro, self.limiter, self.log)
        self.market_fetcher.fetch_shared(start_date, end_date)

        # ② 并发拉个股（线程数上限 MAX_WORKERS）
        self.done = 0
        results: dict = {}
        t0 = time.time()
        ok, fail = 0, 0

        with ThreadPoolExecutor(max_workers=min(total, MAX_WORKERS)) as ex:
            futures = {
                ex.submit(self._fetch_one, c, start_date, end_date, self.save_dir, today, total): c
                for c in codes
            }
            for f in as_completed(futures):
                code = futures[f]
                try:
                    data, cnt, info = f.result()
                    results[code] = data
                    ok += 1
                    self.log.info(f"✓ {code} | {cnt:,}条 | {info}")
                except Exception as e:
                    fail += 1
                    self.log.error(f"✗ {code} | {str(e)[:80]}")

        self.log.info("=" * 60)
        self.log.info(f"完成! 成功:{ok} 失败:{fail} 耗时:{time.time() - t0:.1f}秒")
        self.log.info("=" * 60)
        return results

    def _fetch_one(self, code: str, start: str, end: str,
                   save_dir: Path, today: str, total: int) -> tuple:
        data: dict = {}
        info: list[str] = []

        for name, sheet, fields, typ in INTERFACES:
            df = self._api(name, code, start, end, fields, typ)
            if df is not None and not df.empty:
                df.columns = [FIELD_MAP.get(c, c) for c in df.columns]
                data[name] = df
                info.append(f"{sheet[3:]}:{len(df)}")

        market_sheets = self.market_fetcher.get_sheets(code, start, end)

        if data or market_sheets:
            try:
                self._save(code, data, market_sheets, save_dir, today)
            except Exception as e:
                self.log.error(f"  {code} 保存失败: {e}")

        with self._lock:
            self.done += 1
            prog = f"[{self.done}/{total}]"

        cnt = sum(len(d) for d in data.values())
        detail = (
            f'{prog} {" ".join(info[:5])}{"..." if len(info) > 5 else ""}'
            if info else f"{prog} 无数据"
        )
        return data, cnt, detail

    def _api(self, name: str, code: str, start: str, end: str,
             fields: str, typ: str) -> pd.DataFrame:
        self.limiter.wait()
        try:
            fn = getattr(self.pro, name)
            if typ == "simple":
                return fn(ts_code=code, fields=fields)
            elif typ == "date":
                return fn(ts_code=code, start_date=start, end_date=end, fields=fields)
            elif typ == "market":
                return fn(start_date=start, end_date=end, fields=fields)
        except Exception as e:
            self.log.warning(f"  {code} {name}: {e}")
        return pd.DataFrame()

    def _save(self, code: str, data: dict, market_sheets: dict,
              save_dir: Path, today: str):
        sheets: dict[str, list] = {}
        for name, sheet, _, _ in INTERFACES:
            if name in data:
                sheets.setdefault(sheet, []).append(data[name])

        save_dir.mkdir(parents=True, exist_ok=True)
        filename = f'{code.replace(".", "_")}_{today}.xlsx'
        with pd.ExcelWriter(save_dir / filename, engine="openpyxl") as w:
            for sheet, dfs in sheets.items():
                if len(dfs) == 1:
                    df = dfs[0]
                elif "日线" in sheet:
                    df = dfs[0]
                    for d in dfs[1:]:
                        df = df.merge(d, on=["股票代码", "交易日期"], how="outer")
                else:
                    df = pd.concat(dfs, ignore_index=True)

                for c in ["交易日期", "公告日期", "报告期"]:
                    if c in df.columns:
                        df = df.sort_values(c, ascending=False)
                        break

                df.to_excel(w, sheet_name=sheet, index=False)

            for api_name, sheet_name in MarketFetcher.SHEET_NAMES.items():
                df = market_sheets.get(api_name)
                if df is not None and not df.empty:
                    df.to_excel(w, sheet_name=sheet_name, index=False)
