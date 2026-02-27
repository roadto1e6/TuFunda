"""FastAPI 入口：挂载路由 + 静态文件"""

from pathlib import Path

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

from .routers import query, ws

app = FastAPI(title="个股基本面查询工具")

# 挂载路由
app.include_router(query.router)
app.include_router(ws.router)

# 静态文件（放在路由之后，兜底 /）
static_dir = Path(__file__).resolve().parent.parent / "static"
app.mount("/", StaticFiles(directory=str(static_dir), html=True), name="static")
