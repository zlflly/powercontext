# Copyright (c) 2026 OceanBase.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Branch-aware Pi v3 session history capture."""

# Contract validation errors are ValueError by design for malformed provider input.
# ruff: noqa: TRY003, TRY004

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any
from urllib.parse import quote

from pydantic import BaseModel, ConfigDict, StrictBytes, field_validator, model_validator

from powercontext.errors import InvalidConnectorRunError
from powercontext.limits import MAX_SOURCE_ID_LENGTH
from powercontext.sources.connectors import (
    ConnectorRunCompletion,
    ConnectorRunSession,
    ConnectorRunStatus,
)
from powercontext.sources.definitions import AdapterSourceDefinition
from powercontext.sources.models import Source, SourceMaterialization
from powercontext.sources.projections import TEXT_EVIDENCE_PROJECTION_KEY, TextEvidence

PI_EVENT_SOURCE_NAME = "pi-event"
PI_EVENT_SOURCE_VERSION = "1"
PI_SESSION_CONNECTOR_NAME = "pi-session-history"


class PiSessionInput(BaseModel):
    """A complete Pi v3 JSONL session and an optional selected branch leaf."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    content: StrictBytes
    leaf_id: str | None = None


class PiEventInput(BaseModel):
    """One visible event on a selected root-to-leaf history path."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    session_id: str
    event_id: str
    parent_id: str | None
    event_type: str
    role: str
    content: str
    record: dict[str, Any]

    @field_validator("session_id", "event_id", "event_type", "role")
    @classmethod
    def validate_identity_text(cls, value: str) -> str:
        if not value:
            raise ValueError("Pi event identity fields must be non-empty")
        return value


class PiEventSource(Source):
    """An immutable captured Pi event with its complete provider record."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    source_id: str
    session_id: str
    event_id: str
    parent_id: str | None
    event_type: str
    role: str
    content: str
    record: dict[str, Any]

    @model_validator(mode="after")
    def validate_source_identity(self) -> PiEventSource:
        expected_id = pi_event_source_id(self.session_id, self.event_id)
        if self.name != expected_id or self.source_id != expected_id:
            raise ValueError("Pi Source name and source_id must equal its event identity")
        if self.definition_version != PI_EVENT_SOURCE_VERSION:
            raise ValueError("Pi Source definition version must be 1")
        return self


class PiEventSourceAdapter:
    input_class = PiEventInput
    name = PI_EVENT_SOURCE_NAME
    source_class = PiEventSource

    async def resolve(self, value: PiEventInput, /) -> PiEventSource:
        source_id = pi_event_source_id(value.session_id, value.event_id)
        return PiEventSource(
            name=source_id,
            source_id=source_id,
            definition_version=PI_EVENT_SOURCE_VERSION,
            materialization=SourceMaterialization.CAPTURED,
            session_id=value.session_id,
            event_id=value.event_id,
            parent_id=value.parent_id,
            event_type=value.event_type,
            role=value.role,
            content=value.content,
            record=value.record,
        )

    async def read(self, source: PiEventSource, /) -> PiEventInput:
        return PiEventInput(
            session_id=source.session_id,
            event_id=source.event_id,
            parent_id=source.parent_id,
            event_type=source.event_type,
            role=source.role,
            content=source.content,
            record=source.record,
        )


class PiEventTextEvidenceProjection:
    name = TEXT_EVIDENCE_PROJECTION_KEY.name
    version = TEXT_EVIDENCE_PROJECTION_KEY.version
    source_class = PiEventSource
    output_class: type[BaseModel] = TextEvidence

    def project(self, source: PiEventSource, /) -> TextEvidence:
        return TextEvidence(
            source_type=PI_EVENT_SOURCE_NAME,
            source_id=source.source_id,
            content=source.content,
            metadata={
                "session_id": source.session_id,
                "event_id": source.event_id,
                "parent_id": source.parent_id,
                "event_type": source.event_type,
                "role": source.role,
            },
        )


PI_EVENT_SOURCE_ADAPTER = PiEventSourceAdapter()
PI_EVENT_SOURCE_DEFINITION = AdapterSourceDefinition(
    PI_EVENT_SOURCE_ADAPTER,
    version=PI_EVENT_SOURCE_VERSION,
    projections=(PiEventTextEvidenceProjection(),),
)


class PiSessionConnector:
    """Import each newly visible event from the selected Pi history branch."""

    name = PI_SESSION_CONNECTOR_NAME
    version = "1"
    source_definitions = frozenset({PI_EVENT_SOURCE_NAME})

    def __init__(self, path: str | Path, leaf_id: str | None = None) -> None:
        self._path = Path(path)
        self._leaf_id = leaf_id

    async def run(self, session: ConnectorRunSession, /) -> ConnectorRunCompletion:
        content = self._path.read_bytes()
        parsed = parse_pi_session(PiSessionInput(content=content, leaf_id=self._leaf_id))
        checkpoint = _read_checkpoint(session.checkpoint)
        fingerprints = {
            pi_event_source_id(item.session_id, item.event_id): _fingerprint(item.record)
            for item in parsed
        }
        fingerprints.update(_session_fingerprints(content, set(checkpoint)))

        # Check all previously imported records before submitting any new item.
        for source_id, fingerprint in fingerprints.items():
            previous = checkpoint.get(source_id)
            if previous is not None and previous != fingerprint:
                raise ValueError(f"previously imported Pi event was rewritten: {source_id}")

        next_checkpoint = dict(checkpoint)
        for item in parsed:
            source_id = pi_event_source_id(item.session_id, item.event_id)
            fingerprint = fingerprints[source_id]
            if source_id in checkpoint:
                continue
            outcome = await session.submit(source_id, PI_EVENT_SOURCE_NAME, item)
            if outcome.status.value != "accepted":
                return ConnectorRunCompletion(status=ConnectorRunStatus.INCOMPLETE, checkpoint=session.checkpoint)
            next_checkpoint[source_id] = fingerprint
        return ConnectorRunCompletion(status=ConnectorRunStatus.COMPLETE, checkpoint={"events": next_checkpoint})


def pi_event_source_id(session_id: str, event_id: str) -> str:
    if not isinstance(session_id, str) or not session_id or not isinstance(event_id, str) or not event_id:
        raise ValueError("session_id and event_id must be non-empty strings")
    source_id = f"pi:{quote(session_id, safe='')}:{quote(event_id, safe='')}"
    if len(source_id) > MAX_SOURCE_ID_LENGTH:
        raise ValueError(f"Pi event source_id must not exceed {MAX_SOURCE_ID_LENGTH} characters")
    return source_id


def parse_pi_session(value: PiSessionInput, /) -> tuple[PiEventInput, ...]:  # noqa: C901
    """Parse a complete Pi v3 JSONL file and return visible selected-branch events."""

    if not isinstance(value, PiSessionInput):
        raise TypeError("value must be a PiSessionInput")
    try:
        text = value.content.decode("utf-8-sig", errors="strict")
    except UnicodeDecodeError as error:
        raise ValueError("Pi session must be valid UTF-8") from error

    records: list[dict[str, Any]] = []
    for line in text.splitlines():
        if not line.strip():
            continue
        try:
            record = json.loads(line)
        except json.JSONDecodeError as error:
            raise ValueError("Pi session contains invalid JSON") from error
        if not isinstance(record, dict):
            raise ValueError("Pi session records must be JSON objects")
        records.append(record)
    if not records:
        raise ValueError("Pi session is empty")
    header = records[0]
    if header.get("type") != "session" or header.get("version") != 3:
        raise ValueError("Pi session must begin with a v3 session header")
    session_id = header.get("id")
    if not isinstance(session_id, str) or not session_id:
        raise ValueError("Pi session header id must be non-empty")

    nodes: dict[str, tuple[str | None, dict[str, Any]]] = {}
    order: list[str] = []
    for record in records[1:]:
        event_id = record.get("id")
        if "parentId" not in record:
            raise ValueError("Pi events must include parentId")
        parent_id = record["parentId"]
        if not isinstance(event_id, str) or not event_id:
            raise ValueError("Pi event id must be a non-empty string")
        if event_id in nodes:
            raise ValueError("Pi event ids must be unique")
        if parent_id == "":
            parent_id = None
        if parent_id is not None and not isinstance(parent_id, str):
            raise ValueError("Pi parentId must be a string or null")
        if parent_id is not None and parent_id not in nodes:
            raise ValueError("Pi event parent must refer to an earlier event")
        nodes[event_id] = (parent_id, record)
        order.append(event_id)
    if not nodes:
        if value.leaf_id is not None:
            raise ValueError("selected Pi leaf does not exist")
        return ()
    leaf_id = value.leaf_id if value.leaf_id is not None else order[-1]
    if leaf_id not in nodes:
        raise ValueError("selected Pi leaf does not exist")

    path_ids: list[str] = []
    seen: set[str] = set()
    current: str | None = leaf_id
    while current is not None:
        if current in seen:
            raise ValueError("Pi event parent graph contains a cycle")
        seen.add(current)
        path_ids.append(current)
        current = nodes[current][0]
    path_ids.reverse()

    events: list[PiEventInput] = []
    for event_id in path_ids:
        parent_id, record = nodes[event_id]
        event_type = record.get("type")
        if not isinstance(event_type, str) or not event_type:
            raise ValueError("Pi event type must be a non-empty string")
        rendered = _render_event(record)
        if rendered is None:
            continue
        role, body = rendered
        events.append(PiEventInput(
            session_id=session_id,
            event_id=event_id,
            parent_id=parent_id,
            event_type=event_type,
            role=role,
            content=f"{role}: {body}",
            record=record,
        ))
    return tuple(events)


def _render_event(record: dict[str, Any]) -> tuple[str, str] | None:
    event_type = record["type"]
    if event_type in {"message", "custom_message"}:
        message = record.get("message", record)
        if not isinstance(message, dict):
            raise ValueError("Pi message must be an object")
        role_value = message.get("role", "custom" if event_type == "custom_message" else "")
        role = "custom" if event_type == "custom_message" else role_value
        if not isinstance(role, str) or not role:
            raise ValueError("Pi message role must be a non-empty string")
        blocks = message.get("content", "")
        body = _render_content_blocks(blocks)
        if role == "toolResult":
            tool_name = message.get("toolName", message.get("name", "unknown"))
            tool_call_id = message.get("toolCallId", message.get("id", "unknown"))
            body = f"[tool_result {tool_name} {tool_call_id}] {body}"
        return (role, body) if body else None
    if event_type in {"compaction", "branch_summary"}:
        summary = record.get("summary")
        if not isinstance(summary, str) or not summary:
            return None
        return event_type, summary
    if event_type == "bashExecution":
        command = record.get("command", "")
        output = record.get("output", "")
        if not isinstance(command, str) or not isinstance(output, str):
            raise ValueError("Pi bashExecution command and output must be strings")
        body = f"$ {command}\n{output}".rstrip()
        return (event_type, body) if body else None
    return None


def _render_content_blocks(value: Any) -> str:
    if isinstance(value, str):
        return value
    if not isinstance(value, list):
        raise ValueError("Pi message content must be a string or block list")
    rendered: list[str] = []
    for block in value:
        if not isinstance(block, dict):
            continue
        block_type = block.get("type")
        if block_type == "text" and isinstance(block.get("text"), str):
            rendered.append(block["text"])
        elif block_type == "toolCall":
            name = block.get("name", "unknown")
            call_id = block.get("id", "unknown")
            arguments = block.get("arguments", {})
            try:
                encoded_arguments = json.dumps(arguments, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
            except (TypeError, ValueError) as error:
                raise ValueError("Pi tool arguments must be JSON values") from error
            rendered.append(f"[tool_call {name} {call_id}] {encoded_arguments}")
    return "\n".join(rendered)


def _read_checkpoint(value: Any) -> dict[str, str]:
    if value is None:
        return {}
    if not isinstance(value, dict) or set(value) != {"events"} or not isinstance(value["events"], dict):
        raise InvalidConnectorRunError("checkpoint", "Pi checkpoint has invalid shape")
    events = value["events"]
    if not all(isinstance(key, str) and isinstance(fingerprint, str) for key, fingerprint in events.items()):
        raise InvalidConnectorRunError("checkpoint", "Pi checkpoint entries must be string fingerprints")
    return dict(events)


def _fingerprint(record: dict[str, Any]) -> str:
    wire = json.dumps(record, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(wire).hexdigest()


def _session_fingerprints(content: bytes, known_source_ids: set[str]) -> dict[str, str]:
    text = content.decode("utf-8-sig", errors="strict")
    records = [json.loads(line) for line in text.splitlines() if line.strip()]
    session_id = records[0]["id"]
    fingerprints: dict[str, str] = {}
    for record in records[1:]:
        event_id = record.get("id")
        if not isinstance(event_id, str) or not event_id:
            continue
        source_id = f"pi:{quote(session_id, safe='')}:{quote(event_id, safe='')}"
        if source_id in known_source_ids:
            fingerprints[source_id] = _fingerprint(record)
    return fingerprints


__all__ = [
    "PI_EVENT_SOURCE_ADAPTER",
    "PI_EVENT_SOURCE_DEFINITION",
    "PI_EVENT_SOURCE_NAME",
    "PI_EVENT_SOURCE_VERSION",
    "PI_SESSION_CONNECTOR_NAME",
    "PiEventInput",
    "PiEventSource",
    "PiEventSourceAdapter",
    "PiEventTextEvidenceProjection",
    "PiSessionConnector",
    "PiSessionInput",
    "parse_pi_session",
    "pi_event_source_id",
]
