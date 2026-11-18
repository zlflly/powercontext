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

"""Line-preserving CSV Sources suitable for exact record citations."""

# Contract validation errors are ValueError by design and malformed-input parsers stay cohesive.
# ruff: noqa: TRY003

from __future__ import annotations

import csv
import io
import json
from urllib.parse import quote

from pydantic import BaseModel, ConfigDict, StrictBytes, field_validator, model_validator

from powercontext.limits import MAX_SOURCE_ID_LENGTH
from powercontext.sources.definitions import AdapterSourceDefinition
from powercontext.sources.models import Source, SourceMaterialization
from powercontext.sources.projections import TEXT_EVIDENCE_PROJECTION_KEY, TextEvidence

CSV_ROW_SOURCE_NAME = "csv-row"
CSV_ROW_SOURCE_VERSION = "1"


class CSVDocumentInput(BaseModel):
    """A complete offline CSV document and its caller-supplied identity."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    document_id: str
    primary_key: str
    content: StrictBytes

    @field_validator("document_id")
    @classmethod
    def validate_document_id(cls, value: str) -> str:
        if not value or not value.strip() or value != value.strip():
            raise ValueError("document_id must be non-empty and have no surrounding whitespace")
        return value

    @field_validator("primary_key")
    @classmethod
    def validate_primary_key(cls, value: str) -> str:
        if not value or not value.strip():
            raise ValueError("primary_key must be non-empty")
        return value


class CSVRowInput(BaseModel):
    """One parsed row with the source document's exact header and values."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    document_id: str
    primary_key: str
    primary_key_value: str
    columns: tuple[str, ...]
    values: tuple[str, ...]

    @model_validator(mode="after")
    def validate_row(self) -> CSVRowInput:
        if not self.document_id or self.document_id != self.document_id.strip():
            raise ValueError("document_id must be non-empty and have no surrounding whitespace")
        if not self.primary_key:
            raise ValueError("primary_key must be non-empty")
        if len(self.columns) != len(self.values):
            raise ValueError("columns and values must have the same length")
        if not self.columns or len(set(self.columns)) != len(self.columns):
            raise ValueError("columns must be non-empty and unique")
        if any(not column or not column.strip() for column in self.columns):
            raise ValueError("columns must not be empty or whitespace-only")
        if self.primary_key not in self.columns:
            raise ValueError("primary_key must exactly match a column")
        key_value = self.values[self.columns.index(self.primary_key)]
        if not key_value or not key_value.strip() or key_value != self.primary_key_value:
            raise ValueError("primary_key_value must match a non-blank row value")
        return self


class CSVRowSource(Source):
    """A captured, immutable CSV row whose ID is stable across row order."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    source_id: str
    document_id: str
    primary_key: str
    primary_key_value: str
    columns: tuple[str, ...]
    values: tuple[str, ...]

    @model_validator(mode="after")
    def validate_source_identity(self) -> CSVRowSource:
        expected_id = csv_row_source_id(self.document_id, self.primary_key_value)
        if self.name != expected_id or self.source_id != expected_id:
            raise ValueError("name and source_id must equal the CSV row identity")
        if self.definition_version != CSV_ROW_SOURCE_VERSION:
            raise ValueError("definition_version must be 1")
        CSVRowInput(
            document_id=self.document_id,
            primary_key=self.primary_key,
            primary_key_value=self.primary_key_value,
            columns=self.columns,
            values=self.values,
        )
        return self


def csv_row_source_id(document_id: str, primary_key_value: str) -> str:
    """Return the collision-safe, length-bounded row identity."""

    if not isinstance(document_id, str) or not document_id or document_id != document_id.strip():
        raise ValueError("document_id must be non-empty and have no surrounding whitespace")
    if not isinstance(primary_key_value, str) or not primary_key_value.strip():
        raise ValueError("primary_key_value must not be blank")
    source_id = f"csv:{quote(document_id, safe='')}:{quote(primary_key_value, safe='')}"
    if len(source_id) > MAX_SOURCE_ID_LENGTH:
        raise ValueError(f"CSV row source_id must not exceed {MAX_SOURCE_ID_LENGTH} characters")
    return source_id


def parse_csv_document(value: CSVDocumentInput, /) -> tuple[CSVRowInput, ...]:  # noqa: C901
    """Parse and validate a complete CSV document before returning any rows."""

    if not isinstance(value, CSVDocumentInput):
        raise TypeError("value must be a CSVDocumentInput")
    try:
        text = value.content.decode("utf-8-sig", errors="strict")
    except UnicodeDecodeError as error:
        raise ValueError("CSV content must be valid UTF-8") from error

    reader = csv.reader(io.StringIO(text, newline=""), strict=True)
    try:
        header = next(reader, None)
        if not header:
            raise ValueError("CSV document must start with a non-empty header")
        if any(not column or not column.strip() for column in header):
            raise ValueError("CSV header fields must not be empty or whitespace-only")
        if len(set(header)) != len(header):
            raise ValueError("CSV header fields must be unique")
        if value.primary_key not in header:
            raise ValueError("primary_key must exactly match a CSV header field")

        key_index = header.index(value.primary_key)
        seen_primary_keys: set[str] = set()
        rows: list[CSVRowInput] = []
        for fields in reader:
            # csv.reader emits [] for a completely blank physical record.
            if fields == []:
                continue
            if len(fields) != len(header):
                raise ValueError("CSV row field count must match the header")
            key_value = fields[key_index]
            if not key_value.strip():
                raise ValueError("CSV primary key values must not be blank")
            if key_value in seen_primary_keys:
                raise ValueError("CSV primary key values must be unique within a document")
            _ = csv_row_source_id(value.document_id, key_value)
            seen_primary_keys.add(key_value)
            rows.append(CSVRowInput(
                document_id=value.document_id,
                primary_key=value.primary_key,
                primary_key_value=key_value,
                columns=tuple(header),
                values=tuple(fields),
            ))
    except csv.Error as error:
        raise ValueError("CSV content is malformed") from error
    return tuple(rows)


class CSVRowSourceAdapter:
    """Resolve and read captured CSV rows through the Source registry."""

    input_class = CSVRowInput
    name = CSV_ROW_SOURCE_NAME
    source_class = CSVRowSource

    async def resolve(self, value: CSVRowInput, /) -> CSVRowSource:
        source_id = csv_row_source_id(value.document_id, value.primary_key_value)
        return CSVRowSource(
            name=source_id,
            source_id=source_id,
            definition_version=CSV_ROW_SOURCE_VERSION,
            materialization=SourceMaterialization.CAPTURED,
            document_id=value.document_id,
            primary_key=value.primary_key,
            primary_key_value=value.primary_key_value,
            columns=value.columns,
            values=value.values,
        )

    async def read(self, source: CSVRowSource, /) -> CSVRowInput:
        return CSVRowInput(
            document_id=source.document_id,
            primary_key=source.primary_key,
            primary_key_value=source.primary_key_value,
            columns=source.columns,
            values=source.values,
        )


class CSVRowTextEvidenceProjection:
    """Project each row as ordered JSON text and complete row metadata."""

    name = TEXT_EVIDENCE_PROJECTION_KEY.name
    version = TEXT_EVIDENCE_PROJECTION_KEY.version
    source_class = CSVRowSource
    output_class: type[BaseModel] = TextEvidence

    def project(self, source: CSVRowSource, /) -> TextEvidence:
        row = dict(zip(source.columns, source.values, strict=True))
        return TextEvidence(
            source_type=CSV_ROW_SOURCE_NAME,
            source_id=source.source_id,
            content=json.dumps(row, ensure_ascii=False, separators=(",", ":")),
            metadata={
                "document_id": source.document_id,
                "primary_key": source.primary_key,
                "primary_key_value": source.primary_key_value,
                "columns": list(source.columns),
                "row": row,
            },
        )


CSV_ROW_SOURCE_ADAPTER = CSVRowSourceAdapter()
CSV_ROW_SOURCE_DEFINITION = AdapterSourceDefinition(
    CSV_ROW_SOURCE_ADAPTER,
    version=CSV_ROW_SOURCE_VERSION,
    projections=(CSVRowTextEvidenceProjection(),),
)


__all__ = [
    "CSV_ROW_SOURCE_ADAPTER",
    "CSV_ROW_SOURCE_DEFINITION",
    "CSV_ROW_SOURCE_NAME",
    "CSV_ROW_SOURCE_VERSION",
    "CSVDocumentInput",
    "CSVRowInput",
    "CSVRowSource",
    "CSVRowSourceAdapter",
    "CSVRowTextEvidenceProjection",
    "csv_row_source_id",
    "parse_csv_document",
]
