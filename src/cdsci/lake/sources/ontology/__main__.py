"""CLI: ``python -m cdsci.lake.sources.ontology`` — load OBO ontologies → ontology.*."""

from __future__ import annotations

import typer

from .ingest import RELATIONS, available_ontologies, ingest

app = typer.Typer(
    help="Load semantic-sql OBO ontologies into the ontology schema (terms/synonyms/xrefs/edges).",
    add_completion=False,
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


@app.command("run")
def run(
    ontology: list[str] = typer.Option(
        None, "--ontology", "-o", help="Ontology stem(s) to load; omit for ALL in the bucket."
    ),
    schema: str = typer.Option("ontology", "--schema", help="Target lake schema."),
) -> None:
    """Download + MERGE-upsert ontologies into the ontology schema."""
    summary = ingest(ontologies=ontology or None, schema=schema)
    typer.echo(f"ontology loaded: {summary['loaded']} ontologies into {summary['schema']}.*")
    if summary["errors"]:
        typer.echo(f"  {len(summary['errors'])} failed: {', '.join(summary['errors'])}")


if __name__ == "__main__":
    app()
