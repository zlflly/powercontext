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

"""Page-preserving PDF Sources and citation retrieval helpers."""

from __future__ import annotations

import hashlib
import io
from urllib.parse import quote

from pydantic import BaseModel, ConfigDict, StrictBytes, field_validator, model_validator

from powercontext.artifacts import MemoryCitation
from powercontext.builtin.runtime.models import GetMemoryEntryRequest, SearchMemoryRequest
from powercontext.limits import MAX_SOURCE_ID_LENGTH
from powercontext.sources.definitions import AdapterSourceDefinition
from powercontext.sources.models import Source, SourceMaterialization
from powercontext.sources.projections import TEXT_EVIDENCE_PROJECTION_KEY, TextEvidence

PDF_PAGE_SOURCE_NAME = "pdf-page"
PDF_PAGE_SOURCE_VERSION = "1"


class PDFDocumentInput(BaseModel):
    """A complete PDF byte sequence and its caller-supplied document identity."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    document_id: str
    content: StrictBytes

    @field_validator("document_id")
    @classmethod
    def validate_document_id(cls, value: str) -> str:
        if not value or not value.strip():
            raise ValueError("document_id must be non-empty")
        return value


class PDFPageInput(BaseModel):
    """The extractable text of one physical PDF page and its whole-file digest."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    document_id: str
    document_sha256: str
    page_number: int
    text: str

    @model_validator(mode="after")
    def validate_page(self) -> PDFPageInput:
        if not self.document_id or not self.document_id.strip():
            raise ValueError("document_id must be non-empty")
        if len(self.document_sha256) != 64 or any(c not in "0123456789abcdef" for c in self.document_sha256):
            raise ValueError("document_sha256 must be lowercase SHA-256 hex")
        if isinstance(self.page_number, bool) or self.page_number < 1:
            raise ValueError("page_number must be a positive integer")
        return self


class PDFPageSource(Source):
    """A captured, immutable PDF page whose identity includes the PDF revision."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    source_id: str
    document_id: str
    document_sha256: str
    page_number: int
    text: str

    @model_validator(mode="after")
    def validate_source_identity(self) -> PDFPageSource:
        expected_id = pdf_page_source_id(self.document_id, self.document_sha256, self.page_number)
        if self.name != expected_id or self.source_id != expected_id:
            raise ValueError("PDF page Source identity does not match its document revision and page")
        if self.definition_version != PDF_PAGE_SOURCE_VERSION:
            raise ValueError("PDF page Source definition version must be 1")
        return self


class PDFPageSourceAdapter:
    input_class = PDFPageInput
    name = PDF_PAGE_SOURCE_NAME
    source_class = PDFPageSource

    async def resolve(self, value: PDFPageInput, /) -> PDFPageSource:
        source_id = pdf_page_source_id(value.document_id, value.document_sha256, value.page_number)
        return PDFPageSource(
            name=source_id,
            source_id=source_id,
            definition_version=PDF_PAGE_SOURCE_VERSION,
            materialization=SourceMaterialization.CAPTURED,
            document_id=value.document_id,
            document_sha256=value.document_sha256,
            page_number=value.page_number,
            text=value.text,
        )

    async def read(self, source: PDFPageSource, /) -> PDFPageInput:
        return PDFPageInput(
            document_id=source.document_id,
            document_sha256=source.document_sha256,
            page_number=source.page_number,
            text=source.text,
        )


class PDFPageTextEvidenceProjection:
    name = TEXT_EVIDENCE_PROJECTION_KEY.name
    version = TEXT_EVIDENCE_PROJECTION_KEY.version
    source_class = PDFPageSource
    output_class: type[BaseModel] = TextEvidence

    def project(self, source: PDFPageSource, /) -> TextEvidence:
        return TextEvidence(
            source_type=PDF_PAGE_SOURCE_NAME,
            source_id=source.source_id,
            content=f"{source.document_id}\n{source.text}",
            metadata={
                "document_id": source.document_id,
                "document_sha256": source.document_sha256,
                "page_number": source.page_number,
            },
        )


PDF_PAGE_SOURCE_ADAPTER = PDFPageSourceAdapter()
PDF_PAGE_SOURCE_DEFINITION = AdapterSourceDefinition(
    PDF_PAGE_SOURCE_ADAPTER,
    version=PDF_PAGE_SOURCE_VERSION,
    projections=(PDFPageTextEvidenceProjection(),),
)


def pdf_page_source_id(document_id: str, document_sha256: str, page_number: int) -> str:
    if not document_id or not document_id.strip():
        raise ValueError("document_id must be non-empty")
    if len(document_sha256) != 64 or any(c not in "0123456789abcdef" for c in document_sha256):
        raise ValueError("document_sha256 must be lowercase SHA-256 hex")
    if isinstance(page_number, bool) or not isinstance(page_number, int) or page_number < 1:
        raise ValueError("page_number must be a positive integer")
    source_id = f"pdf:{quote(document_id, safe='')}:{document_sha256}:{page_number}"
    if len(source_id) > MAX_SOURCE_ID_LENGTH:
        raise ValueError(f"PDF page source_id must not exceed {MAX_SOURCE_ID_LENGTH} characters")
    return source_id


def parse_pdf_document(value: PDFDocumentInput, /) -> tuple[PDFPageInput, ...]:
    """Extract every physical page from a complete PDF atomically."""

    if not isinstance(value, PDFDocumentInput):
        raise TypeError("value must be a PDFDocumentInput")
    if not value.content:
        raise ValueError("PDF content must not be empty")
    try:
        from pypdf import PdfReader

        reader = PdfReader(io.BytesIO(value.content), strict=True)
        if reader.is_encrypted and not reader.decrypt(""):
            raise ValueError("PDF requires a password")
        page_count = len(reader.pages)
        if page_count < 1:
            raise ValueError("PDF must contain at least one physical page")
        digest = hashlib.sha256(value.content).hexdigest()
        pages: list[PDFPageInput] = []
        for page_number, page in enumerate(reader.pages, start=1):
            extracted = page.extract_text() or ""
            if not isinstance(extracted, str):
                raise ValueError("PDF page text extraction returned a non-string value")
            pages.append(PDFPageInput(
                document_id=value.document_id,
                document_sha256=digest,
                page_number=page_number,
                text=extracted,
            ))
        return tuple(pages)
    except ValueError:
        raise
    except Exception as error:
        raise ValueError("PDF is malformed or cannot be completely parsed") from error


async def search_pdf_pages(memory, query: str, limit: int = 20) -> tuple[MemoryCitation, ...]:
    """Search a scoped Memory and return distinct PDF page citations that read back exactly."""

    if not isinstance(query, str) or not query.strip():
        raise ValueError("query must be non-empty")
    if isinstance(limit, bool) or not isinstance(limit, int) or limit < 1:
        raise ValueError("limit must be a positive integer")
    page = await memory.search(SearchMemoryRequest(query=query, limit=min(limit * 4, 100), mode="auto"))
    selected: list[MemoryCitation] = []
    seen_pages: set[tuple[str, str, int]] = set()
    for hit in page.hits:
        citation = MemoryCitation(
            memory_ref=hit.memory_ref,
            entry_id=hit.entry_id,
            entry_version_id=hit.entry_version_id,
        )
        record = await memory.get(GetMemoryEntryRequest(citation=citation))
        for source_ref in record.entry.sources:
            if source_ref.source_type != PDF_PAGE_SOURCE_NAME:
                continue
            identity = parse_pdf_page_source_id(source_ref.source_id)
            if identity in seen_pages:
                continue
            seen_pages.add(identity)
            selected.append(citation)
            if len(selected) == limit:
                return tuple(selected)
    return tuple(selected)


def parse_pdf_page_source_id(source_id: str) -> tuple[str, str, int]:
    """Decode a PDF page identity for stable page deduplication."""

    parts = source_id.split(":")
    if len(parts) != 4 or parts[0] != "pdf" or not parts[3].isdigit():
        raise ValueError("invalid PDF page Source identity")
    from urllib.parse import unquote

    return unquote(parts[1]), parts[2], int(parts[3])


__all__ = [
    "PDF_PAGE_SOURCE_ADAPTER",
    "PDF_PAGE_SOURCE_DEFINITION",
    "PDF_PAGE_SOURCE_NAME",
    "PDF_PAGE_SOURCE_VERSION",
    "PDFDocumentInput",
    "PDFPageInput",
    "PDFPageSource",
    "PDFPageSourceAdapter",
    "PDFPageTextEvidenceProjection",
    "parse_pdf_document",
    "parse_pdf_page_source_id",
    "pdf_page_source_id",
    "search_pdf_pages",
]
