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

"""Coalesce concurrent embedding work and cache successful vectors."""

# ValueError is part of the public input/result contract; batch failures are propagated via futures.
# ruff: noqa: TRY003, TRY301

from __future__ import annotations

import asyncio
from collections import OrderedDict
from dataclasses import dataclass
from typing import Literal

from powercontext.builtin.inference.models import EmbeddingResult, EmbeddingVector, InferenceUsage
from powercontext.builtin.inference.protocols import EmbeddingModel, QueryEmbeddingModel

_InputType = Literal["document", "query"]
_Key = tuple[_InputType, str]


@dataclass(frozen=True, slots=True)
class _ResolvedVector:
    vector: EmbeddingVector
    batch_id: int
    usage: InferenceUsage


class CoalescingEmbeddingModel:
    """Share concurrent identical texts and cache completed embeddings with an LRU."""

    def __init__(
        self,
        model: EmbeddingModel,
        *,
        max_batch_size: int = 64,
        coalesce_ms: float = 5.0,
        cache_size: int = 1024,
    ) -> None:
        if isinstance(max_batch_size, bool) or not isinstance(max_batch_size, int) or max_batch_size < 1:
            raise ValueError("max_batch_size must be a positive integer")
        if isinstance(coalesce_ms, bool) or not isinstance(coalesce_ms, (int, float)) or coalesce_ms < 0:
            raise ValueError("coalesce_ms must be a non-negative number")
        if isinstance(cache_size, bool) or not isinstance(cache_size, int) or cache_size < 0:
            raise ValueError("cache_size must be a non-negative integer")
        self._model = model
        self.profile = model.profile
        self._max_batch_size = max_batch_size
        self._coalesce_seconds = float(coalesce_ms) / 1_000
        self._cache_size = cache_size
        self._lock = asyncio.Lock()
        self._cache: OrderedDict[_Key, EmbeddingVector] = OrderedDict()
        self._requests: dict[_Key, asyncio.Future[_ResolvedVector]] = {}
        self._pending: dict[_Key, asyncio.Future[_ResolvedVector]] = {}
        self._dispatcher: asyncio.Task[None] | None = None
        self._next_batch_id = 0

    async def embed(self, texts: tuple[str, ...], /) -> EmbeddingResult:
        """Embed document texts, preserving duplicates and the caller's order."""

        return await self._embed(texts, input_type="document")

    async def embed_query(self, texts: tuple[str, ...], /) -> EmbeddingResult:
        """Embed query texts when the wrapped provider exposes a query operation."""

        if not isinstance(self._model, QueryEmbeddingModel):
            return await self._embed(texts, input_type="document")
        return await self._embed(texts, input_type="query")

    async def _embed(self, texts: tuple[str, ...], *, input_type: _InputType) -> EmbeddingResult:
        if not texts:
            return EmbeddingResult(vectors=(), usage=InferenceUsage(requests=0))
        unique = tuple(dict.fromkeys(texts))
        immediate: dict[str, EmbeddingVector] = {}
        futures: dict[str, asyncio.Future[_ResolvedVector]] = {}
        async with self._lock:
            for text in unique:
                key = (input_type, text)
                if self._cache_size and key in self._cache:
                    immediate[text] = self._cache[key]
                    self._cache.move_to_end(key)
                    continue
                future = self._requests.get(key)
                if future is None:
                    future = asyncio.get_running_loop().create_future()
                    future.add_done_callback(_consume_unobserved_exception)
                    self._requests[key] = future
                    self._pending[key] = future
                futures[text] = future
            if self._pending and (self._dispatcher is None or self._dispatcher.done()):
                self._dispatcher = asyncio.create_task(self._dispatch())

        resolved: dict[str, _ResolvedVector] = {}
        if futures:
            values = await asyncio.gather(*(asyncio.shield(futures[text]) for text in futures))
            resolved.update(zip(futures, values, strict=True))

        batch_usage: dict[int, InferenceUsage] = {}
        vectors_by_text = dict(immediate)
        for text, value in resolved.items():
            vectors_by_text[text] = value.vector
            batch_usage[value.batch_id] = value.usage
        vectors = tuple(vectors_by_text[text] for text in texts)
        usage = _sum_usage(tuple(batch_usage.values()))
        return EmbeddingResult(vectors=vectors, usage=usage)

    async def _dispatch(self) -> None:
        if self._coalesce_seconds:
            await asyncio.sleep(self._coalesce_seconds)
        async with self._lock:
            queued = tuple(self._pending.items())
            self._pending.clear()
            self._dispatcher = None
        grouped: dict[_InputType, list[tuple[_Key, asyncio.Future[_ResolvedVector]]]] = {
            "document": [],
            "query": [],
        }
        for item in queued:
            grouped[item[0][0]].append(item)
        batches = tuple(
            tuple(items[start : start + self._max_batch_size])
            for items in grouped.values()
            for start in range(0, len(items), self._max_batch_size)
        )
        await asyncio.gather(*(self._run_batch(batch) for batch in batches))

    async def _run_batch(self, batch: tuple[tuple[_Key, asyncio.Future[_ResolvedVector]], ...]) -> None:  # noqa: C901
        keys = tuple(key for key, _ in batch)
        texts = tuple(key[1] for key in keys)
        input_type = keys[0][0]
        try:
            if input_type == "query" and isinstance(self._model, QueryEmbeddingModel):
                result = await self._model.embed_query(texts)
            else:
                result = await self._model.embed(texts)
            if len(result.vectors) != len(texts):
                raise ValueError("embedding model returned the wrong vector count")
            vectors = tuple(tuple(float(value) for value in vector) for vector in result.vectors)
            usage = result.usage
        except BaseException as error:
            async with self._lock:
                for key, future in batch:
                    if self._requests.get(key) is future:
                        del self._requests[key]
                    if not future.done():
                        future.set_exception(error)
            if isinstance(error, asyncio.CancelledError):
                raise
            return

        async with self._lock:
            self._next_batch_id += 1
            batch_id = self._next_batch_id
            if self._cache_size:
                for key, vector in zip(keys, vectors, strict=True):
                    self._cache[key] = vector
                    self._cache.move_to_end(key)
                while len(self._cache) > self._cache_size:
                    self._cache.popitem(last=False)
            for (key, future), vector in zip(batch, vectors, strict=True):
                if self._requests.get(key) is future:
                    del self._requests[key]
                if not future.done():
                    future.set_result(_ResolvedVector(vector, batch_id, usage))


def _sum_usage(usages: tuple[InferenceUsage, ...]) -> InferenceUsage:
    return InferenceUsage(
        requests=sum(usage.requests for usage in usages),
        input_tokens=_sum_optional(usage.input_tokens for usage in usages),
        output_tokens=_sum_optional(usage.output_tokens for usage in usages),
    )


def _sum_optional(values) -> int | None:
    observed = tuple(values)
    if not observed or any(value is None for value in observed):
        return None
    return sum(value for value in observed if value is not None)


def _consume_unobserved_exception(future: asyncio.Future[_ResolvedVector]) -> None:
    if not future.cancelled():
        future.exception()


__all__ = ["CoalescingEmbeddingModel"]
