"""CLI: ``python -m cdsci.lake.sources.ontology`` — load OBO ontologies → ontology.*."""

from __future__ import annotations

import typer

from .._cli import build_app
from .ingest import RELATIONS, available_ontologies, ingest

app = build_app(
    "ontology", ingest,
    help="Load semantic-sql OBO ontologies into the ontology schema (terms/synonyms/xrefs/edges).",
)


@app.command("tables")
def tables() -> None:
    """List the projected ontology tables and their keys."""
    for name, spec in RELATIONS.items():
        typer.echo(f"  ontology.{name:<10} key={','.join(spec.key)}")


@app.command("list")
def list_ontologies() -> None:
    """List every ontology available in the semsql bucket."""
    onts = available_ontologies()
    typer.echo("\n".join(onts))
    typer.echo(f"\n{len(onts)} ontologies available.")


if __name__ == "__main__":
    app()
