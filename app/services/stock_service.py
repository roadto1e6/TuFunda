"""Web 集成层：WebStockFetcher + TaskManager + QueueLogHandler"""

import logging
import queue
import threading
import uuid
from dataclasses import dataclass, field
from pathlib import Path

from .fetcher import StockFetcher, MarketFetcher, OUTPUT_DIR


# ==================== 日志 Handler ====================

class QueueLogHandler(logging.Handler):
    """将日志记录推入 queue.Queue，供 WebSocket 消费"""

    def __init__(self, q: queue.Queue):
        super().__init__()
        self.q = q

    def emit(self, record: logging.LogRecord):
        try:
            self.q.put_nowait({"type": "log", "text": self.format(record)})
        except queue.Full:
            pass


# ==================== WebStockFetcher ====================

_WEB_LOGGER_NAME = "web_stock_fetcher"


def _make_queue_logger(log_queue: queue.Queue) -> logging.Logger:
    """创建/复用一个将日志推入队列的 logger"""
    log = logging.getLogger(_WEB_LOGGER_NAME)
    log.handlers.clear()
    log.propagate = False
    handler = QueueLogHandler(log_queue)
    handler.setFormatter(logging.Formatter("%(asctime)s | %(message)s", datefmt="%H:%M:%S"))
    log.addHandler(handler)
    log.setLevel(logging.INFO)
    return log


class WebStockFetcher(StockFetcher):
    """继承 StockFetcher，添加进度回调和日志队列"""

    def __init__(self, token: str, log_queue: queue.Queue, progress_cb=None):
        MarketFetcher.clear_cache()
        log = _make_queue_logger(log_queue)
        super().__init__(token, log=log)
        self._progress_cb = progress_cb

    def _fetch_one(self, code, start, end, save_dir, today, total):
        result = super()._fetch_one(code, start, end, save_dir, today, total)
        if self._progress_cb:
            self._progress_cb(self.done, total)
        return result


# ==================== TaskState ====================

@dataclass
class TaskState:
    task_id: str
    state: str = "running"  # running / completed / error
    progress: int = 0
    total: int = 0
    message: str = ""
    log_queue: queue.Queue = field(default_factory=lambda: queue.Queue(maxsize=5000))
    files: list[str] = field(default_factory=list)


# ==================== TaskManager ====================

class TaskManager:
    """管理后台查询任务，单任务限制"""

    def __init__(self):
        self._current: TaskState | None = None
        self._lock = threading.Lock()

    @property
    def current(self) -> TaskState | None:
        return self._current

    def is_running(self) -> bool:
        with self._lock:
            return self._current is not None and self._current.state == "running"

    def start_task(self, token: str, codes: str, start_date: str | None,
                   end_date: str | None, years: int) -> TaskState:
        with self._lock:
            if self._current is not None and self._current.state == "running":
                raise RuntimeError("已有任务正在运行，请等待完成")

            task_id = uuid.uuid4().hex[:8]
            state = TaskState(task_id=task_id)

            code_list = [c.strip() for c in codes.split(",") if c.strip()]
            if not code_list:
                raise ValueError("股票代码列表为空")

            state.total = len(code_list)
            self._current = state

        def progress_cb(done: int, total: int):
            state.progress = done
            state.total = total
            try:
                state.log_queue.put_nowait({
                    "type": "status",
                    "progress": done,
                    "total": total,
                })
            except queue.Full:
                pass

        def run():
            try:
                fetcher = WebStockFetcher(token, state.log_queue, progress_cb)
                kwargs = {"years": years}
                if start_date:
                    kwargs["start_date"] = start_date
                if end_date:
                    kwargs["end_date"] = end_date

                fetcher.fetch(code_list, **kwargs)

                # 精准收集本次任务输出目录的文件
                state.files = _collect_files(fetcher.save_dir)
                state.state = "completed"
                state.message = "查询完成"
                state.log_queue.put_nowait({"type": "complete"})
            except Exception as e:
                state.state = "error"
                state.message = str(e)
                try:
                    state.log_queue.put_nowait({"type": "error", "text": str(e)})
                except queue.Full:
                    pass

        threading.Thread(target=run, daemon=True).start()
        return state


def _collect_files(save_dir: Path | None) -> list[str]:
    """收集指定目录下的所有 xlsx 文件路径"""
    if save_dir is None or not save_dir.exists():
        return []
    return [
        str(f.relative_to(Path(".")))
        for f in sorted(save_dir.glob("*.xlsx"))
    ]


# 全局单例
task_manager = TaskManager()
