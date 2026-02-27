# TuFunda - 个股基本面查询工具

基于 Tushare 的 A 股基本面数据批量获取工具，提供 Web 界面操作，支持实时进度推送和 Excel 导出。

## 功能

- 批量获取 24 类个股基本面数据（日线行情、财务指标、利润表、资产负债表等）
- 自动附加大盘背景数据（沪深 300 日线/估值、申万行业）
- 多线程并发拉取，API 限流保护
- WebSocket 实时日志和进度推送
- 按日期分目录导出 Excel，支持在线下载/删除管理

## 数据覆盖

| 类别 | 内容 |
|------|------|
| 行情 | 日线行情、复权因子、每日指标、涨跌停 |
| 财务 | 利润表、资产负债表、现金流量表、财务指标 |
| 股东 | 十大股东、股东户数、股东增减持、股权质押 |
| 市场 | 大宗交易、融资融券、北向资金、资金流向 |
| 研究 | 业绩预告、券商预测、机构调研 |
| 其他 | 公司信息、管理层、分红送股、主营构成、筹码分布、技术因子 |
| 大盘 | 沪深300日线/估值、申万一级行业 |

## 快速开始

### 环境要求

- Python 3.11+
- [Tushare Pro](https://tushare.pro) Token

### 本地运行

```bash
# 安装依赖
pip install -r requirements.txt

# 启动服务
python run.py
```

浏览器访问 http://localhost:8000

### Docker 运行

```bash
# 构建镜像
docker build -t tufunda .

# 启动容器
docker run -d -p 8000:8000 --name tufunda tufunda
```

## 项目结构

```
TuFunda/
├── app/
│   ├── main.py              # FastAPI 入口，挂载路由和静态文件
│   ├── config.py             # Token 持久化读写
│   ├── models.py             # Pydantic 数据模型
│   ├── routers/
│   │   ├── query.py          # REST API（Token、查询、文件管理）
│   │   └── ws.py             # WebSocket 进度推送
│   └── services/
│       ├── fetcher.py        # 核心数据获取（StockFetcher、MarketFetcher）
│       └── stock_service.py  # Web 集成层（TaskManager、日志队列）
├── static/
│   ├── index.html            # 前端页面
│   ├── style.css             # 样式
│   └── app.js                # 前端逻辑
├── Dockerfile
├── requirements.txt
└── run.py                    # 启动脚本
```

## API

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/api/token` | 查询 Token 配置状态 |
| POST | `/api/token` | 设置 Tushare Token |
| POST | `/api/query` | 启动查询任务 |
| GET | `/api/status` | 获取当前任务状态 |
| GET | `/api/files` | 列出所有导出文件 |
| GET | `/api/download/{path}` | 下载指定文件 |
| DELETE | `/api/files/{path}` | 删除指定文件 |
| WS | `/ws/progress/{task_id}` | 实时日志和进度推送 |

## 技术栈

- **后端**: FastAPI + Uvicorn
- **数据源**: Tushare Pro API
- **数据处理**: Pandas + OpenPyXL
- **前端**: 原生 HTML/CSS/JS
- **实时通信**: WebSocket
- **容器化**: Docker
