"""Offline tests for ``cdsci.lake.publish.release`` (cdsci-lake#96, M0).

Two roles share the same fixture files:

* "producer" -- builds a ``ReleaseManifest`` from ``DATASET_CONTRACT`` +
  ``golden_manifest.json``'s release/run metadata and checks it matches the
  committed golden JSON byte-for-byte.
* "DuckDock-style validator" -- loads ``golden_manifest.json`` cold (no
  producer code) and checks ``schema_version``, that every path is
  release-relative, and that every table names a temporal model.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from fixtures.contracts import dataset as fx

from cdsci.lake.contracts import Materialization, TemporalModel
from cdsci.lake.publish.release import (
    AcceptanceCheck,
    AcceptanceReport,
    ArtifactEntry,
    ArtifactStatus,
    FileEntry,
    ManifestTable,
    PublicationReceipt,
    PublicPathError,
    ReleaseCandidate,
    ReleaseManifest,
    SourceAssetVersion,
    TableFileIndex,
)

FIXTURES = Path(__file__).parent / "fixtures" / "contracts"
GOLDEN_MANIFEST_PATH = FIXTURES / "golden_manifest.json"


def _build_manifest_from_contract() -> ReleaseManifest:
    """The "producer" role: build a manifest from DATASET_CONTRACT, not from the golden JSON."""
    events_table = ManifestTable.from_contract(
        fx.EVENTS_TABLE,
        schema_path="tables/demo.events/schema.json",
        files_path="tables/demo.events/files.json",
        schema_digest="sha256:0000000000000000000000000000000000000000000000000000000000e1",
        row_count=3,
    )
    entities_table = ManifestTable.from_contract(
        fx.ENTITIES_TABLE,
        schema_path="tables/demo.entities/schema.json",
        files_path="tables/demo.entities/files.json",
        schema_digest="sha256:0000000000000000000000000000000000000000000000000000000000e2",
        row_count=3,
    )
    return ReleaseManifest(
        dataset="demo-catalog",
        release="R1",
        status=ArtifactStatus.PUBLISHED,
        run_id="01927c9e-0000-7000-8000-000000000001",
        tables=(events_table, entities_table),
        published_at="2026-09-22T00:00:00Z",
        source_asset_versions=(
            SourceAssetVersion(ref="ducklake://lake/demo/events", version="snapshot:1"),
            SourceAssetVersion(ref="ducklake://lake/demo/entities", version="snapshot:1"),
        ),
        artifacts={
            "parquet": ArtifactEntry(
                status=ArtifactStatus.VERIFIED, required=True, location="tables/"
            ),
            "ducklake": ArtifactEntry(
                status=ArtifactStatus.VERIFIED, required=True, location="catalog.ducklake"
            ),
        },
    )


def test_producer_builds_manifest_matching_golden_fixture():
    manifest = _build_manifest_from_contract()
    golden = json.loads(GOLDEN_MANIFEST_PATH.read_text())
    assert manifest.to_dict() == golden


def test_producer_manifest_round_trips_through_json():
    manifest = _build_manifest_from_contract()
    assert ReleaseManifest.from_json(manifest.to_json()) == manifest


def test_duckdock_validator_loads_golden_manifest_cold():
    """No producer code involved -- just JSON parsing + this module's validator."""
    manifest = ReleaseManifest.from_json(GOLDEN_MANIFEST_PATH.read_text())
    assert manifest.schema_version == "1.0"
    assert manifest.dataset == "demo-catalog"
    for table in manifest.tables:
        assert isinstance(table.temporal_model, TemporalModel)
        assert not table.schema_path.startswith("/")
        assert not table.files_path.startswith("/")
        assert "://" not in table.schema_path
        assert "://" not in table.files_path
    assert not manifest.provenance.startswith("/")
    assert not manifest.lineage.startswith("/")


def test_manifest_rejects_absolute_schema_path():
    with pytest.raises(PublicPathError, match="absolute path"):
        ManifestTable.from_contract(
            fx.EVENTS_TABLE,
            schema_path="/etc/secrets/schema.json",
            files_path="tables/demo.events/files.json",
            schema_digest="sha256:x",
        )


def test_manifest_rejects_s3_uri():
    with pytest.raises(PublicPathError, match="private storage scheme"):
        FileEntry(
            uri="s3://private-bucket/part-0.parquet",
            size=1,
            sha256="x",
            content_type="application/x-parquet",
        )


def test_manifest_rejects_credential_bearing_url():
    with pytest.raises(PublicPathError, match="credential-bearing"):
        ManifestTable.from_contract(
            fx.EVENTS_TABLE,
            schema_path="https://user:pass@example.org/schema.json",
            files_path="tables/demo.events/files.json",
            schema_digest="sha256:x",
        )


def test_manifest_rejects_secret_shaped_artifact_key():
    with pytest.raises(PublicPathError, match="secret-shaped key"):
        ReleaseManifest(
            dataset="d",
            release="R1",
            status=ArtifactStatus.STAGED,
            run_id="r1",
            tables=(),
            artifacts={
                "aws_secret_key": ArtifactEntry(status=ArtifactStatus.STAGED, required=False)
            },
        )


def test_file_index_round_trips():
    idx = TableFileIndex(
        table="demo.entities",
        release="R1",
        materialization=Materialization.RELEASE_SNAPSHOT,
        files=(
            FileEntry(
                uri="data/part-00000.parquet",
                size=123,
                sha256="abc",
                content_type="application/x-parquet",
                rows=3,
            ),
        ),
    )
    assert TableFileIndex.from_json(idx.to_json()) == idx


def test_acceptance_report_required_check_gates_promotion():
    report = AcceptanceReport(
        dataset="demo-catalog",
        release="R1",
        run_id="r1",
        checked_at="2026-09-22T00:00:00Z",
        checks=(
            AcceptanceCheck("manifest_schema_valid", passed=True, required=True),
            AcceptanceCheck("optional_iceberg_parity", passed=False, required=False),
        ),
    )
    assert report.passed is True
    failed = AcceptanceReport(
        dataset="demo-catalog",
        release="R1",
        run_id="r1",
        checked_at="2026-09-22T00:00:00Z",
        checks=(AcceptanceCheck("manifest_schema_valid", passed=False, required=True),),
    )
    assert failed.passed is False
    assert AcceptanceReport.from_json(report.to_json()) == report


def test_release_candidate_and_publication_receipt_may_reference_private_locations():
    """Internal types (design §6.3/§6.6) -- unlike public manifest/file-index/acceptance types."""
    candidate = ReleaseCandidate(
        dataset="demo-catalog",
        release="R1",
        run_id="r1",
        built_at="2026-09-22T00:00:00Z",
        destination="s3://internal-staging-bucket/demo-catalog/R1/",
        tables=("demo.events", "demo.entities"),
        status=ArtifactStatus.STAGED,
    )
    assert ReleaseCandidate.from_json(candidate.to_json()) == candidate

    receipt = PublicationReceipt(
        dataset="demo-catalog",
        release="R1",
        format="parquet",
        destination="s3://internal-staging-bucket/demo-catalog/R1/",
        schema_digest="sha256:x",
        run_id="r1",
        status=ArtifactStatus.PUBLISHED,
        row_counts={"demo.events": 3, "demo.entities": 3},
    )
    assert PublicationReceipt.from_json(receipt.to_json()) == receipt
