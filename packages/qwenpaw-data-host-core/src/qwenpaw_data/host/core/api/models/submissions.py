# -*- coding: utf-8 -*-
from __future__ import annotations

import hashlib
import json
from typing import Any, Literal

from pydantic import Field, field_validator

from qwenpaw_data.host.core.api.models.common import ApiModel


class SubmitRunRequest(ApiModel):
    protocol_version: Literal[1] = 1
    submission_id: str = Field(
        min_length=1, max_length=128, pattern=r"^[A-Za-z0-9_-]+$"
    )
    text: str = Field(min_length=1)
    datasource_id: str = Field(min_length=1)
    agent_id: str = "default"

    @field_validator("text", "datasource_id")
    @classmethod
    def nonblank(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("must not be blank")
        return value

    def request_digest(self) -> str:
        encoded = json.dumps(
            self.model_dump(exclude={"submission_id"}),
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
        ).encode("utf-8")
        return hashlib.sha256(encoded).hexdigest()


class SubmissionRunSchema(ApiModel):
    session_id: str
    run_id: str
    status: Literal[
        "running",
        "reconciling",
        "succeeded",
        "failed",
        "cancelled",
        "interrupted",
        "unknown",
    ]
    last_sequence_number: int | None = None
    error: dict[str, Any] | None = None
    reason: str | None = None


class SubmissionLookupSchema(ApiModel):
    protocol_version: Literal[1] = 1
    submission_id: str
    state: Literal["accepted", "not_found"]
    run: SubmissionRunSchema | None = None
