"""CLI: ``python -m cdsci.lake.sources.openalex`` — load OpenAlex into ``openalex.*``.

Subset on a laptop, then the same commands run the full load on a big server:

    python -m cdsci.lake.sources.openalex parts --entity works   # how many parts
    python -m cdsci.lake.sources.openalex entities --max-files 2  # ref entities (subset)
    python -m cdsci.lake.sources.openalex works --max-files 4 --mode merge
    # full run (server): --mode append for the initial bulk, then monthly --mode merge
"""

from __future__ import annotations

import typer

from ... import ops
from ...config import get_settings
from ...connect import LAKE, lake_connect
from .._cli import base_app
from .ingest import ENTITIES, ingest_entity, ingest_works, part_urls

app = base_app(help="Load the OpenAlex snapshot (works + reference entities) into the lake.")


@app.command("parts")
def parts(
    entity: str = typer.Option(
        "works", "--entity", "-e", help="works|institutions|sources|funders|topics"
    ),
    max_files: int = typer.Option(None, "--max-files", help="Cap part count (subset)."),
) -> None:
    """List how many snapshot part files an entity has (and the first URL)."""
    s = get_settings()
    name = "works" if entity == "works" else ENTITIES[entity].name
    con = lake_connect(s)
    try:
        urls = part_urls(con, name, base=s.openalex_s3_base, max_files=max_files)
    finally:
        con.close()
    typer.echo(f"{entity}: {len(urls)} part files")
    if urls:
        typer.echo(f"  first: {urls[0]}")


@app.command("entities")
def entities(
    entity: list[str] = typer.Option(
        None, "--entity", "-e", help="Reference entity/entities; omit for all four."
    ),
    schema: str = typer.Option("openalex", "--schema"),
    version: str = typer.Option(None, "--version", help="Snapshot label (default: YYYY-MM)."),
    mode: str = typer.Option("merge", "--mode", help="merge|append"),
    max_files: int = typer.Option(None, "--max-files", help="Cap part count (subset)."),
) -> None:
    """Load the small reference entities (institutions/sources/funders/topics)."""
    s = get_settings()
    con = lake_connect(s)
    try:
        with ops.run(con, source="openalex", target=f"{LAKE}.{schema}", version=version) as r:
            total = 0
            for name in entity or list(ENTITIES):
                n = ingest_entity(
                    name, schema=schema, version=version, mode=mode,
                    max_files=max_files, settings=s, con=con,
                )
                total += n
                typer.echo(f"{name}: {n:,}")
            r.rows = total
    finally:
        con.close()


@app.command("works")
def works(
    schema: str = typer.Option("openalex", "--schema"),
    version: str = typer.Option(None, "--version", help="Snapshot label (default: YYYY-MM)."),
    mode: str = typer.Option("merge", "--mode", help="merge|append (append for initial bulk)."),
    max_files: int = typer.Option(None, "--max-files", help="Cap part count (subset)."),
    batch_files: int = typer.Option(None, "--batch-files", help="Parts per temp-table batch."),
) -> None:
    """Load the works corpus (pruned by domain, abstract reconstructed) + edge table."""
    summary = ingest_works(
        schema=schema, version=version, mode=mode,
        max_files=max_files, batch_files=batch_files,
    )
    typer.echo(
        f"openalex works: {summary['works']:,} works, "
        f"{summary['references']:,} references, "
        f"{summary['authorships']:,} affiliation edges ({summary['parts']} parts)"
    )


if __name__ == "__main__":
    app()
