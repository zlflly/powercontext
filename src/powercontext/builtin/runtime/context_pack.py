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

"""Portable full and incremental wire formats for prepared context text."""

# Protocol validation is exposed as ContextPackError(ValueError) for every malformed wire input.
# ruff: noqa: TRY003

from __future__ import annotations

import base64
import binascii
import hashlib
import json
from dataclasses import dataclass
from typing import Any

_PACK_SCHEMA = "powercontext.context-pack.v1"
_DELTA_SCHEMA = "powercontext.context-pack-delta.v1"


class ContextPackError(ValueError):
    """Invalid context pack, delta, or reconstruction input."""


@dataclass(frozen=True, slots=True)
class ContextPack:
    """A validated immutable sequence of UTF-8 chunks."""

    items: tuple[bytes, ...]
    digest: str
    byte_count: int

    @classmethod
    def build(cls, items: tuple[bytes, ...]) -> ContextPack:
        normalized = _validate_items(items)
        digest = _pack_digest(normalized)
        byte_count = 0
        pack = cls(items=normalized, digest=digest, byte_count=byte_count)
        # byte_count includes itself. Its decimal width converges in at most a few steps.
        for _ in range(8):
            current_count = len(pack.to_bytes())
            if current_count == pack.byte_count:
                return pack
            pack = cls(items=normalized, digest=digest, byte_count=current_count)
        raise ContextPackError("could not stabilize context pack byte count")

    @classmethod
    def from_content(cls, content: str, *, chunk_bytes: int = 512) -> ContextPack:  # noqa: C901
        if not isinstance(content, str):
            raise ContextPackError("content must be text")
        if isinstance(chunk_bytes, bool) or not isinstance(chunk_bytes, int) or chunk_bytes <= 0:
            raise ContextPackError("chunk_bytes must be a positive integer")
        chunks: list[bytes] = []
        current = bytearray()
        for line in content.splitlines(keepends=True):
            encoded_line = line.encode("utf-8")
            if len(encoded_line) <= chunk_bytes:
                if current and len(current) + len(encoded_line) > chunk_bytes:
                    chunks.append(bytes(current))
                    current.clear()
                current.extend(encoded_line)
                continue
            if current:
                chunks.append(bytes(current))
                current.clear()
            for character in line:
                encoded = character.encode("utf-8")
                if len(encoded) > chunk_bytes:
                    raise ContextPackError("chunk_bytes cannot fit a UTF-8 character")
                if current and len(current) + len(encoded) > chunk_bytes:
                    chunks.append(bytes(current))
                    current.clear()
                current.extend(encoded)
            if current:
                chunks.append(bytes(current))
                current.clear()
        if current:
            chunks.append(bytes(current))
        return cls.build(tuple(chunks))

    def to_bytes(self) -> bytes:
        payload = {
            "schema": _PACK_SCHEMA,
            "items": [_encode_item(item) for item in self.items],
            "digest": self.digest,
            "byte_count": self.byte_count,
        }
        return _canonical_json(payload)

    @classmethod
    def from_bytes(cls, value: bytes) -> ContextPack:
        payload = _decode_json(value)
        if not isinstance(payload, dict) or set(payload) != {"schema", "items", "digest", "byte_count"}:
            raise ContextPackError("pack must contain exactly schema, items, digest, and byte_count")
        if payload["schema"] != _PACK_SCHEMA:
            raise ContextPackError("unsupported context pack schema")
        raw_items = payload["items"]
        if not isinstance(raw_items, list):
            raise ContextPackError("pack items must be an array")
        items = tuple(_decode_item(item) for item in raw_items)
        items = _validate_items(items)
        digest = payload["digest"]
        byte_count = payload["byte_count"]
        if not isinstance(digest, str) or digest != _pack_digest(items):
            raise ContextPackError("context pack digest mismatch")
        if isinstance(byte_count, bool) or not isinstance(byte_count, int) or byte_count != len(value):
            raise ContextPackError("context pack byte_count mismatch")
        return cls(items=items, digest=digest, byte_count=byte_count)


@dataclass(frozen=True, slots=True)
class ContextPackDelta:
    """A base-bound prefix reuse and appended suffix."""

    base_digest: str
    target_digest: str
    prefix_items: int
    appended: tuple[bytes, ...]

    @classmethod
    def between(cls, base: ContextPack, target: ContextPack) -> ContextPackDelta:
        prefix = 0
        for base_item, target_item in zip(base.items, target.items, strict=False):
            if base_item != target_item:
                break
            prefix += 1
        return cls(
            base_digest=base.digest,
            target_digest=target.digest,
            prefix_items=prefix,
            appended=target.items[prefix:],
        )

    @property
    def byte_count(self) -> int:
        return len(self.to_bytes())

    def to_bytes(self) -> bytes:
        _validate_delta(self)
        return _canonical_json({
            "schema": _DELTA_SCHEMA,
            "base_digest": self.base_digest,
            "target_digest": self.target_digest,
            "prefix_items": self.prefix_items,
            "appended": [_encode_item(item) for item in self.appended],
        })

    @classmethod
    def from_bytes(cls, value: bytes) -> ContextPackDelta:
        payload = _decode_json(value)
        expected = {"schema", "base_digest", "target_digest", "prefix_items", "appended"}
        if not isinstance(payload, dict) or set(payload) != expected:
            raise ContextPackError("delta has missing or extra fields")
        if payload["schema"] != _DELTA_SCHEMA:
            raise ContextPackError("unsupported context pack delta schema")
        base_digest = payload["base_digest"]
        target_digest = payload["target_digest"]
        if not _valid_digest(base_digest) or not _valid_digest(target_digest):
            raise ContextPackError("delta digests must be sha256 digests")
        prefix_items = payload["prefix_items"]
        if isinstance(prefix_items, bool) or not isinstance(prefix_items, int) or prefix_items < 0:
            raise ContextPackError("delta prefix_items must be a non-negative integer")
        raw_appended = payload["appended"]
        if not isinstance(raw_appended, list):
            raise ContextPackError("delta appended must be an array")
        appended = _validate_items(tuple(_decode_item(item) for item in raw_appended))
        return cls(base_digest, target_digest, prefix_items, appended)


def apply_context_pack_delta(base: ContextPack, delta: ContextPackDelta) -> ContextPack:
    """Validate a delta against its base and return its reconstructed target."""

    _validate_delta(delta)
    if not isinstance(base, ContextPack):
        raise ContextPackError("base must be a ContextPack")
    validated_base = ContextPack.from_bytes(base.to_bytes())
    if delta.base_digest != validated_base.digest:
        raise ContextPackError("delta base digest mismatch")
    if delta.prefix_items > len(validated_base.items):
        raise ContextPackError("delta prefix exceeds base item count")
    target = ContextPack.build(validated_base.items[: delta.prefix_items] + delta.appended)
    if target.digest != delta.target_digest:
        raise ContextPackError("delta target digest mismatch")
    return target


def _validate_delta(delta: ContextPackDelta) -> None:
    if not _valid_digest(delta.base_digest) or not _valid_digest(delta.target_digest):
        raise ContextPackError("delta digests must be sha256 digests")
    if isinstance(delta.prefix_items, bool) or not isinstance(delta.prefix_items, int) or delta.prefix_items < 0:
        raise ContextPackError("delta prefix_items must be a non-negative integer")
    _validate_items(delta.appended)


def _validate_items(items: Any) -> tuple[bytes, ...]:
    if not isinstance(items, tuple):
        raise ContextPackError("items must be a tuple of bytes")
    for item in items:
        if not isinstance(item, bytes):
            raise ContextPackError("each item must be bytes")
        try:
            item.decode("utf-8", errors="strict")
        except UnicodeDecodeError as error:
            raise ContextPackError("each item must contain valid UTF-8") from error
    return items


def _pack_digest(items: tuple[bytes, ...]) -> str:
    hash_payload = {"schema": _PACK_SCHEMA, "items": [_encode_item(item) for item in items]}
    return "sha256:" + hashlib.sha256(_canonical_json(hash_payload)).hexdigest()


def _valid_digest(value: Any) -> bool:
    if not isinstance(value, str) or not value.startswith("sha256:") or len(value) != 71:
        return False
    try:
        int(value[7:], 16)
    except ValueError:
        return False
    return True


def _encode_item(item: bytes) -> str:
    return base64.b64encode(item).decode("ascii")


def _decode_item(value: Any) -> bytes:
    if not isinstance(value, str):
        raise ContextPackError("wire chunks must be Base64 strings")
    try:
        return base64.b64decode(value.encode("ascii"), validate=True)
    except (UnicodeEncodeError, binascii.Error, ValueError) as error:
        raise ContextPackError("wire chunk contains invalid Base64") from error


def _canonical_json(value: Any) -> bytes:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")


def _decode_json(value: bytes) -> Any:
    if not isinstance(value, bytes):
        raise ContextPackError("wire value must be bytes")
    try:
        return json.loads(value.decode("utf-8", errors="strict"), object_pairs_hook=_unique_object)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ContextPackError("wire value must be valid UTF-8 JSON") from error


def _unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ContextPackError("wire JSON object contains a duplicate field")
        result[key] = value
    return result


__all__ = ["ContextPack", "ContextPackDelta", "ContextPackError", "apply_context_pack_delta"]
