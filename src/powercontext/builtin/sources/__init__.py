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

"""Built-in Source components."""

from powercontext.builtin.sources.content import (
    CONTENT_SOURCE_ADAPTER,
    CONTENT_SOURCE_DEFINITION,
    CONTENT_SOURCE_NAME,
    ContentCapture,
    ContentSource,
    ContentSourceAdapter,
    ContentSourceInternal,
    ContentSourceTarget,
    ContentTextEvidenceProjection,
)
from powercontext.builtin.sources.external_skill import (
    EXTERNAL_SKILL_SNAPSHOT_SOURCE_ADAPTER,
    EXTERNAL_SKILL_SNAPSHOT_SOURCE_DEFINITION,
    EXTERNAL_SKILL_SNAPSHOT_SOURCE_NAME,
    ExternalSkillImportMode,
    ExternalSkillSnapshotCapture,
    ExternalSkillSnapshotSource,
    ExternalSkillSnapshotSourceAdapter,
)
from powercontext.builtin.sources.journal import (
    SourceCursor,
    SourceJournal,
    SourceJournalEntry,
    validate_scope_id,
)
from powercontext.builtin.sources.skill_package import (
    SKILL_PACKAGE_UPLOAD_SOURCE_ADAPTER,
    SKILL_PACKAGE_UPLOAD_SOURCE_DEFINITION,
    SKILL_PACKAGE_UPLOAD_SOURCE_NAME,
    SkillPackageUploadCapture,
    SkillPackageUploadSource,
    SkillPackageUploadSourceAdapter,
)
from powercontext.builtin.sources.skill_usage import (
    SKILL_USAGE_SOURCE_ADAPTER,
    SKILL_USAGE_SOURCE_DEFINITION,
    SKILL_USAGE_SOURCE_NAME,
    ObservedInvocation,
    ObservedOutcome,
    ObservedValidation,
    SkillUsageCapture,
    SkillUsageSource,
    SkillUsageSourceAdapter,
)
from powercontext.sources import TEXT_EVIDENCE_PROJECTION_KEY, SourceDefinitionRegistry, TextEvidence
from powercontext.sources.csv_rows import (
    CSV_ROW_SOURCE_ADAPTER,
    CSV_ROW_SOURCE_DEFINITION,
    CSV_ROW_SOURCE_NAME,
    CSV_ROW_SOURCE_VERSION,
    CSVDocumentInput,
    CSVRowInput,
    CSVRowSource,
    CSVRowSourceAdapter,
    CSVRowTextEvidenceProjection,
    csv_row_source_id,
    parse_csv_document,
)
from powercontext.sources.pi_session import (
    PI_EVENT_SOURCE_ADAPTER,
    PI_EVENT_SOURCE_DEFINITION,
    PI_EVENT_SOURCE_NAME,
    PI_EVENT_SOURCE_VERSION,
    PI_SESSION_CONNECTOR_NAME,
    PiEventInput,
    PiEventSource,
    PiEventSourceAdapter,
    PiEventTextEvidenceProjection,
    PiSessionConnector,
    PiSessionInput,
    parse_pi_session,
    pi_event_source_id,
)
from powercontext.sources.pdf_pages import (
    PDF_PAGE_SOURCE_ADAPTER,
    PDF_PAGE_SOURCE_DEFINITION,
    PDF_PAGE_SOURCE_NAME,
    PDF_PAGE_SOURCE_VERSION,
    PDFDocumentInput,
    PDFPageInput,
    PDFPageSource,
    PDFPageSourceAdapter,
    PDFPageTextEvidenceProjection,
    parse_pdf_document,
    parse_pdf_page_source_id,
    pdf_page_source_id,
    search_pdf_pages,
)

BUILTIN_SOURCE_REGISTRY = SourceDefinitionRegistry((
    CONTENT_SOURCE_DEFINITION,
    CSV_ROW_SOURCE_DEFINITION,
    PI_EVENT_SOURCE_DEFINITION,
    PDF_PAGE_SOURCE_DEFINITION,
    EXTERNAL_SKILL_SNAPSHOT_SOURCE_DEFINITION,
    SKILL_PACKAGE_UPLOAD_SOURCE_DEFINITION,
    SKILL_USAGE_SOURCE_DEFINITION,
))

__all__ = [
    "BUILTIN_SOURCE_REGISTRY",
    "CONTENT_SOURCE_ADAPTER",
    "CONTENT_SOURCE_DEFINITION",
    "CONTENT_SOURCE_NAME",
    "CSV_ROW_SOURCE_ADAPTER",
    "CSV_ROW_SOURCE_DEFINITION",
    "CSV_ROW_SOURCE_NAME",
    "CSV_ROW_SOURCE_VERSION",
    "EXTERNAL_SKILL_SNAPSHOT_SOURCE_ADAPTER",
    "EXTERNAL_SKILL_SNAPSHOT_SOURCE_DEFINITION",
    "EXTERNAL_SKILL_SNAPSHOT_SOURCE_NAME",
    "PI_EVENT_SOURCE_ADAPTER",
    "PI_EVENT_SOURCE_DEFINITION",
    "PI_EVENT_SOURCE_NAME",
    "PI_EVENT_SOURCE_VERSION",
    "PI_SESSION_CONNECTOR_NAME",
    "PDF_PAGE_SOURCE_ADAPTER",
    "PDF_PAGE_SOURCE_DEFINITION",
    "PDF_PAGE_SOURCE_NAME",
    "PDF_PAGE_SOURCE_VERSION",
    "SKILL_PACKAGE_UPLOAD_SOURCE_ADAPTER",
    "SKILL_PACKAGE_UPLOAD_SOURCE_DEFINITION",
    "SKILL_PACKAGE_UPLOAD_SOURCE_NAME",
    "SKILL_USAGE_SOURCE_ADAPTER",
    "SKILL_USAGE_SOURCE_DEFINITION",
    "SKILL_USAGE_SOURCE_NAME",
    "TEXT_EVIDENCE_PROJECTION_KEY",
    "CSVDocumentInput",
    "CSVRowInput",
    "CSVRowSource",
    "CSVRowSourceAdapter",
    "CSVRowTextEvidenceProjection",
    "ContentCapture",
    "ContentSource",
    "ContentSourceAdapter",
    "ContentSourceInternal",
    "ContentSourceTarget",
    "ContentTextEvidenceProjection",
    "ExternalSkillImportMode",
    "ExternalSkillSnapshotCapture",
    "ExternalSkillSnapshotSource",
    "ExternalSkillSnapshotSourceAdapter",
    "ObservedInvocation",
    "ObservedOutcome",
    "ObservedValidation",
    "PiEventInput",
    "PiEventSource",
    "PiEventSourceAdapter",
    "PiEventTextEvidenceProjection",
    "PiSessionConnector",
    "PiSessionInput",
    "PDFDocumentInput",
    "PDFPageInput",
    "PDFPageSource",
    "PDFPageSourceAdapter",
    "PDFPageTextEvidenceProjection",
    "SkillPackageUploadCapture",
    "SkillPackageUploadSource",
    "SkillPackageUploadSourceAdapter",
    "SkillUsageCapture",
    "SkillUsageSource",
    "SkillUsageSourceAdapter",
    "SourceCursor",
    "SourceJournal",
    "SourceJournalEntry",
    "TextEvidence",
    "csv_row_source_id",
    "parse_csv_document",
    "parse_pi_session",
    "pi_event_source_id",
    "parse_pdf_document",
    "parse_pdf_page_source_id",
    "pdf_page_source_id",
    "search_pdf_pages",
    "validate_scope_id",
]
