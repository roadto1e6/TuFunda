"""REST 端点"""

from pathlib import Path

from fastapi import APIRouter, HTTPException
from fastapi.responses import FileResponse

from ..config import get_token, save_token, mask_token
from ..models import QueryRequest, QueryResponse, TokenRequest, TokenStatus, TaskStatus
from ..services.fetcher import OUTPUT_DIR
from ..services.stock_service import task_manager

router = APIRouter(prefix="/api")


@router.post("/token")
def set_token(req: TokenRequest) -> dict:
    save_token(req.token)
    return {"ok": True, "masked": mask_token(req.token)}


@router.get("/token")
def check_token() -> TokenStatus:
    token = get_token()
    return TokenStatus(configured=bool(token), masked=mask_token(token))


@router.post("/query")
def start_query(req: QueryRequest) -> QueryResponse:
    token = get_token()
    if not token:
        raise HTTPException(400, "请先配置 Tushare Token")

    try:
        state = task_manager.start_task(
            token=token,
            codes=req.codes,
            start_date=req.start_date,
            end_date=req.end_date,
            years=req.years,
        )
    except (RuntimeError, ValueError) as e:
        raise HTTPException(409, str(e))

    return QueryResponse(task_id=state.task_id, message="查询已启动")


@router.get("/status")
def get_status() -> TaskStatus:
    st = task_manager.current
    if st is None:
        return TaskStatus(state="idle")
    return TaskStatus(
        task_id=st.task_id,
        state=st.state,
        progress=st.progress,
        total=st.total,
        message=st.message,
        files=st.files,
    )


@router.get("/files")
def list_files() -> list[dict]:
    if not OUTPUT_DIR.exists():
        return []
    return [
        {
            "name": f.name,
            "path": str(f.relative_to(Path("."))),
            "size": f.stat().st_size,
        }
        for f in sorted(OUTPUT_DIR.rglob("*.xlsx"), reverse=True)
    ]


@router.get("/download/{path:path}")
def download_file(path: str):
    allowed_dir = OUTPUT_DIR.resolve()
    fp = Path(path).resolve()
    if not fp.is_relative_to(allowed_dir) or not fp.exists() or fp.suffix != ".xlsx":
        raise HTTPException(404, "文件不存在")
    return FileResponse(
        fp, filename=fp.name,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )


@router.delete("/files/{path:path}")
def delete_file(path: str):
    allowed_dir = OUTPUT_DIR.resolve()
    fp = Path(path).resolve()
    if not fp.is_relative_to(allowed_dir) or not fp.exists() or fp.suffix != ".xlsx":
        raise HTTPException(404, "文件不存在")
    fp.unlink()
    # 如果父目录为空，也一并删除
    parent = fp.parent
    if parent != allowed_dir and parent.exists() and not any(parent.iterdir()):
        parent.rmdir()
    return {"ok": True}
