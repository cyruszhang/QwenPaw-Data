# -*- coding: utf-8 -*-
"""Artifact stream registration remains available without BizTrace."""

from __future__ import annotations

import hashlib
from types import SimpleNamespace

from qwenpaw_data.host.core.domain.identity import Identity
from qwenpaw_data.host.core.runtime.chat_runtime import ChatRuntime
from qwenpaw_data.host.core.runtime.context import RunContext


class _Stream:
    def __init__(self):
        self.items = []

    async def artifact_registered(self, **fields):
        self.items.append(fields)


async def test_registers_digest_metadata_when_biztrace_is_disabled(tmp_path):
    runtime = ChatRuntime(chats=object(), events=object(), hosts=object())
    stream = _Stream()
    runtime._envelope = SimpleNamespace(stream=stream)
    runtime._run_context = RunContext(
        session_id="session-1",
        chat_id="chat-1",
        workspace=tmp_path,
        paths=SimpleNamespace(artifact_dir=tmp_path),
        identity=Identity(user_id="alice"),
    )
    content = b"# report\n"
    (tmp_path / "report.md").write_bytes(content)

    await runtime._register_new_files()

    assert len(stream.items) == 1
    artifact = stream.items[0]
    assert artifact["path"] == "report.md"
    assert artifact["media_type"] == "text/markdown"
    assert artifact["size_bytes"] == len(content)
    assert artifact["digest"] == (
        "sha256:" + hashlib.sha256(content).hexdigest()
    )
