"""CLI: ``python -m cdsci.lake.sources.ensembl`` — land an Ensembl GTF."""

from __future__ import annotations

import typer

from .._cli import build_app
from .ingest import ingest, ingest_release

app = build_app(
    "ensembl", ingest,
    help="Land an Ensembl per-species GTF into lake.ensembl.gtf (one release/species per run).",
)


@app.command("run-release")
def run_release(
    ensembl_release: int | None = typer.Option(
        None, "--ensembl-release", help="Release number (default: Ensembl's current)."
    ),
) -> None:
    """Land every vertebrate species of a release, skipping species already landed."""
    r = ingest_release(ensembl_release=ensembl_release)
    typer.echo(
        f"ensembl {r['release']}: {r['landed']} landed, {r['already_landed']} already landed"
    )


if __name__ == "__main__":
    app()
