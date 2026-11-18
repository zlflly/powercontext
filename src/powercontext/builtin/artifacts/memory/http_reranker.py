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

"""HTTP-backed scoring for identity-preserving Memory reranking."""

# The offline protocol requires schema failures to surface as ValueError.
# ruff: noqa: TRY003, TRY004

from __future__ import annotations

import math
from typing import Any

import httpx

from powercontext.builtin.artifacts.memory.models import MemoryHit
from powercontext.builtin.artifacts.memory.reranking import (
    MemoryRerankDecision,
    _validate_bounds,
)
from powercontext.builtin.inference import InferenceUsage


class HTTPMemoryReranker:
    """Score candidates in bounded HTTP batches and select by global score."""

    def __init__(
        self,
        endpoint: str,
        *,
        client: httpx.AsyncClient,
        batch_size: int = 32,
        policy_id: str = "http-cross-encoder-v1",
    ) -> None:
        if not isinstance(endpoint, str) or not endpoint:
            raise ValueError("endpoint must be a non-empty string")
        if isinstance(batch_size, bool) or not isinstance(batch_size, int) or batch_size <= 0:
            raise ValueError("batch_size must be greater than zero")
        if not isinstance(policy_id, str) or not policy_id:
            raise ValueError("policy_id must be a non-empty string")
        if not isinstance(client, httpx.AsyncClient):
            raise ValueError("client must be an httpx.AsyncClient")
        self._endpoint = endpoint
        self._client = client
        self._batch_size = batch_size
        self.policy_id = policy_id

    async def rerank(
        self,
        query: str,
        candidates: tuple[MemoryHit, ...],
        limit: int,
        /,
    ) -> MemoryRerankDecision:
        """Return top-scoring global ranks while leaving Memory hits untouched."""

        candidate_count = len(candidates)
        _validate_bounds(candidate_count, limit)

        scores: dict[int, float] = {}
        requests = 0
        for start in range(0, candidate_count, self._batch_size):
            batch = candidates[start : start + self._batch_size]
            first_rank = start + 1
            response = await self._client.post(
                self._endpoint,
                json={
                    "query": query,
                    "candidates": [
                        {"rank": first_rank + offset, "text": candidate.text}
                        for offset, candidate in enumerate(batch)
                    ],
                },
            )
            requests += 1
            response.raise_for_status()
            batch_scores = _parse_batch_scores(response, set(range(first_rank, first_rank + len(batch))))
            scores.update(batch_scores)

        selected_ranks = tuple(
            rank
            for rank, _ in sorted(scores.items(), key=lambda item: (-item[1], item[0]))[:limit]
        )
        return MemoryRerankDecision(
            selected_ranks=selected_ranks,
            usage=InferenceUsage(requests=requests),
        )


def _parse_batch_scores(response: httpx.Response, expected_ranks: set[int]) -> dict[int, float]:  # noqa: C901
    try:
        payload: Any = response.json()
    except (ValueError, UnicodeDecodeError) as error:
        raise ValueError("rerank service returned invalid JSON") from error

    if not isinstance(payload, dict) or set(payload) != {"scores"}:
        raise ValueError("rerank response must contain exactly the scores field")
    raw_scores = payload["scores"]
    if not isinstance(raw_scores, list):
        raise ValueError("rerank scores must be a list")

    parsed: dict[int, float] = {}
    for item in raw_scores:
        if not isinstance(item, dict) or set(item) != {"rank", "score"}:
            raise ValueError("each rerank score must contain exactly rank and score")
        rank = item["rank"]
        score = item["score"]
        if isinstance(rank, bool) or not isinstance(rank, int):
            raise ValueError("rerank rank must be an integer")
        if rank not in expected_ranks or rank in parsed:
            raise ValueError("rerank scores must cover this batch's ranks exactly once")
        if isinstance(score, bool) or not isinstance(score, (int, float)):
            raise ValueError("rerank score must be numeric")
        try:
            numeric_score = float(score)
        except (OverflowError, ValueError) as error:
            raise ValueError("rerank score must be finite") from error
        if not math.isfinite(numeric_score):
            raise ValueError("rerank score must be finite")
        parsed[rank] = numeric_score

    if parsed.keys() != expected_ranks:
        raise ValueError("rerank scores must cover this batch's ranks exactly once")
    return parsed


__all__ = ["HTTPMemoryReranker"]
