# -*- coding: utf-8 -*-
"""Independent, idempotent runs for the PawApp backend adapter."""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

from fastapi import APIRouter, Depends, Header, Query, Request
from fastapi.responses import StreamingResponse

from qwenpaw_data.host.core.api.deps import ServiceState, get_identity, get_state
from qwenpaw_data.host.core.api.errors import map_domain_error, raise_api
from qwenpaw_data.host.core.api.models.submissions import (
    SubmissionLookupSchema,
    SubmissionRunSchema,
    SubmitRunRequest,
)
from qwenpaw_data.host.core.api.routers.events import chat_events
from qwenpaw_data.host.core.domain.identity import Identity
from qwenpaw_data.host.core.runtime.chat_runtime import ChatRuntime

if TYPE_CHECKING:
    from qwenpaw_data.host.core.store.submissions import (
        SQLSubmissionStore,
        SubmissionRecord,
    )

router = APIRouter(tags=["submissions"])


def _store(state: ServiceState) -> SQLSubmissionStore:
    if state.submissions is None:
        raise_api(
            "VALIDATION",
            "Durable submissions require the SQL store",
            status=501,
            details={"reason": "durable_submissions_unsupported"},
        )
    return state.submissions


async def _lookup_response(
    store: SQLSubmissionStore,
    record: SubmissionRecord,
) -> SubmissionLookupSchema:
    return SubmissionLookupSchema(
        submission_id=record.submission_id,
        state="accepted",
        run=SubmissionRunSchema(
            session_id=record.session_id,
            run_id=record.run_id,
            **await store.run_snapshot(record),
        ),
    )


@router.get("/capabilities/submissions")
async def submission_capabilities(
    state: ServiceState = Depends(get_state),
) -> dict:
    supported = state.submissions is not None
    return {
        "protocol_version": 1 if supported else None,
        "durable_submissions": supported,
        "event_replay": supported,
    }


async def _accept_and_start(
    body: SubmitRunRequest,
    identity: Identity,
    state: ServiceState,
) -> SubmissionRecord:
    record, created = await _store(state).submit(
        identity=identity,
        submission_id=body.submission_id,
        request_digest=body.request_digest(),
        text=body.text,
        datasource_id=body.datasource_id,
        agent_id=body.agent_id,
    )
    if created:
        runtime = ChatRuntime(
            chats=state.chats,
            events=state.events,
            hosts=state.hosts,
            prefs=state.prefs,
            sessions=state.sessions,
            settlement=state.settlement,
        )
        state.track(asyncio.create_task(runtime.run(record.run_id, identity=identity)))
    return record


@router.post("/submissions", response_model=SubmissionLookupSchema, status_code=202)
async def submit_run(
    body: SubmitRunRequest,
    identity: Identity = Depends(get_identity),
    state: ServiceState = Depends(get_state),
) -> SubmissionLookupSchema:
    store = _store(state)
    # A disconnected HTTP caller must not cancel acceptance between commit and
    # scheduling. A process crash in that window is reconciled at startup.
    task = asyncio.create_task(_accept_and_start(body, identity, state))
    state.track(task)
    try:
        record = await asyncio.shield(task)
    except Exception as exc:
        http = map_domain_error(exc)
        if http:
            raise http from exc
        raise
    return await _lookup_response(store, record)


@router.get("/submissions/{submission_id}", response_model=SubmissionLookupSchema)
async def lookup_submission(
    submission_id: str,
    identity: Identity = Depends(get_identity),
    state: ServiceState = Depends(get_state),
) -> SubmissionLookupSchema:
    store = _store(state)
    record = await store.lookup(identity.user_id, submission_id)
    if record is None:
        return SubmissionLookupSchema(submission_id=submission_id, state="not_found")
    return await _lookup_response(store, record)


@router.get("/submissions/{submission_id}/events")
async def submission_events(
    request: Request,
    submission_id: str,
    identity: Identity = Depends(get_identity),
    state: ServiceState = Depends(get_state),
    after_sequence_number: int = Query(-1, ge=-1),
    last_event_id: str | None = Header(default=None, alias="Last-Event-ID"),
) -> StreamingResponse:
    store = _store(state)
    record = await store.lookup(identity.user_id, submission_id)
    if record is None:
        raise_api("NOT_FOUND", "Submission not found", status=404)
    snapshot = await store.run_snapshot(record)
    if snapshot["status"] == "unknown":
        raise_api(
            "NOT_FOUND",
            "Accepted submission's run is unavailable",
            status=410,
            details={"reason": "run_missing"},
        )
    return await chat_events(
        request,
        record.session_id,
        record.run_id,
        identity,
        state,
        after_sequence_number,
        last_event_id,
    )
