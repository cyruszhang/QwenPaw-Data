# -*- coding: utf-8 -*-
from __future__ import annotations

import hashlib
import json
from typing import Any, Literal

from pydantic import Field, field_validator, model_validator

from qwenpaw_data.host.core.api.models.chat import AskUserQuestionAnswerSchema
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


class SubmissionCommandRequest(ApiModel):
    protocol_version: Literal[1] = 1
    command_id: str = Field(min_length=1, max_length=128, pattern=r"^[A-Za-z0-9_-]+$")
    kind: Literal["answer", "cancel"]
    request_id: str | None = Field(default=None, min_length=1, max_length=256)
    answers: list[AskUserQuestionAnswerSchema] | None = None
    reason: str | None = Field(default=None, max_length=2000)

    @model_validator(mode="after")
    def validate_payload(self) -> SubmissionCommandRequest:
        if self.kind == "answer":
            if not self.request_id or not self.answers or self.reason is not None:
                raise ValueError("answer requires request_id and answers")
        elif self.request_id is not None or self.answers is not None:
            raise ValueError("cancel accepts only an optional reason")
        return self

    def request_digest(self) -> str:
        encoded = json.dumps(
            self.model_dump(exclude={"command_id"}),
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
        ).encode("utf-8")
        return hashlib.sha256(encoded).hexdigest()


class SubmissionCommandSchema(ApiModel):
    protocol_version: Literal[1] = 1
    submission_id: str
    command_id: str
    kind: Literal["answer", "cancel"] | None = None
    state: Literal["accepted", "rejected", "unknown", "not_found"]
    reason: str | None = None
