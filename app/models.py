"""Pydantic 请求/响应模型"""

from datetime import datetime

from pydantic import BaseModel, Field, field_validator, model_validator


class QueryRequest(BaseModel):
    codes: str = Field(..., description="股票代码，逗号分隔", max_length=5000)
    start_date: str | None = Field(None, description="起始日期 YYYYMMDD")
    end_date: str | None = Field(None, description="结束日期 YYYYMMDD")
    years: int = Field(3, ge=1, le=30, description="默认回溯年数")

    @field_validator("start_date", "end_date")
    @classmethod
    def validate_date(cls, v: str | None) -> str | None:
        if v is None:
            return v
        try:
            datetime.strptime(v, "%Y%m%d")
        except ValueError:
            raise ValueError("日期格式应为 YYYYMMDD 且必须是合法日期")
        return v

    @model_validator(mode="after")
    def validate_date_range(self):
        if self.start_date and self.end_date and self.start_date > self.end_date:
            raise ValueError("起始日期不能晚于结束日期")
        return self


class QueryResponse(BaseModel):
    task_id: str
    message: str


class TokenRequest(BaseModel):
    token: str = Field(..., min_length=1, max_length=200)


class TokenStatus(BaseModel):
    configured: bool
    masked: str


class TaskStatus(BaseModel):
    task_id: str | None = None
    state: str  # idle / running / completed / error
    progress: int = 0
    total: int = 0
    message: str = ""
    files: list[str] = []
