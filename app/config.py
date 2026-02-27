"""Token 持久化：读写 ~/.stock_fetcher_config.json"""

import json
from pathlib import Path

CONFIG_PATH = Path.home() / ".stock_fetcher_config.json"


def _read_config() -> dict:
    if CONFIG_PATH.exists():
        try:
            return json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            pass
    return {}


def _write_config(cfg: dict) -> None:
    CONFIG_PATH.write_text(json.dumps(cfg, ensure_ascii=False, indent=2), encoding="utf-8")


def get_token() -> str | None:
    return _read_config().get("token")


def save_token(token: str) -> None:
    cfg = _read_config()
    cfg["token"] = token
    _write_config(cfg)


def mask_token(token: str | None) -> str:
    """脱敏显示 token：前4后4，中间用 * 代替"""
    if not token:
        return ""
    if len(token) <= 8:
        return "****"
    return token[:4] + "*" * (len(token) - 8) + token[-4:]
