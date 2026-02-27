"""WebSocket 进度推送"""

import asyncio
import queue

from fastapi import APIRouter, WebSocket, WebSocketDisconnect

from ..services.stock_service import task_manager

router = APIRouter()


@router.websocket("/ws/progress/{task_id}")
async def ws_progress(websocket: WebSocket, task_id: str):
    await websocket.accept()

    state = task_manager.current
    if state is None or state.task_id != task_id:
        await websocket.send_json({"type": "error", "text": "任务不存在"})
        await websocket.close()
        return

    try:
        while True:
            # 快照当前状态，避免竞态
            current_state = state.state

            # 批量读取队列消息（最多 50 条/轮）
            messages = []
            try:
                while len(messages) < 50:
                    messages.append(state.log_queue.get_nowait())
            except queue.Empty:
                pass

            # 发送消息，遇到终态立即结束
            for msg in messages:
                await websocket.send_json(msg)
                if msg["type"] in ("complete", "error"):
                    await websocket.close()
                    return

            # 任务已结束且队列已排空
            if current_state != "running" and not messages:
                final_type = "complete" if current_state == "completed" else "error"
                await websocket.send_json({
                    "type": final_type,
                    "text": state.message,
                })
                await websocket.close()
                return

            await asyncio.sleep(0.3)

    except WebSocketDisconnect:
        pass
    except Exception:
        try:
            await websocket.close()
        except Exception:
            pass
