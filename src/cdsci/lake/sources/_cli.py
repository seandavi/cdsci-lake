"""Shared typer CLI driver for a registered :class:`~cdsci.lake.ops.Source` (issue #52).

Every source's real work already lives in one ``ingest(**kwargs) -> dict``
function that self-connects, self-brackets in :func:`cdsci.lake.ops.run`, and
returns a summary dict (a superset of ``ops.Run.summary()``). What every
``sources/*/__main__.py`` restated by hand was just the shell around that: a
typer app, a ``--log-level`` callback wired to :func:`cdsci.lake.log.configure`,
and a ``run`` command mapping CLI flags 1:1 onto ``ingest()``'s own keyword
parameters, then echoing the summary.

:func:`build_app` generates that shell from ``ingest``'s real signature via
``inspect``, so a source's CLI options fall out of its function signature for
free -- no per-source restating. It also requires ``name`` to be a member of
:data:`ops.SOURCES`: a source absent from the registry (the drift ``ontology``
fell into) structurally cannot get a CLI here.

Sources with more than one entrypoint (``mesh``) or a ``run`` that is genuinely
more than a kwargs pass-through (``scp``'s ``domain=path`` parsing, ``openalex``'s
shared-connection multi-entity loop) keep hand-written commands --
:func:`base_app` gives them just the ``--log-level`` shell to build on, so even
the bespoke ones don't restate *that*.
"""

from __future__ import annotations

import dataclasses
import inspect
import typing
from collections.abc import Callable
from typing import Any

import typer

from .. import ops
from ..log import configure

# Driver-injected concerns, never a CLI option: `settings` is the driver's own
# Settings() default, and no ingest() should take a raw DuckDB connection from
# a CLI user -- if one ever does, it stays hand-written (see openalex).
_NOT_CLI_PARAMS = {"settings", "con"}


def base_app(help: str) -> typer.Typer:  # noqa: A002 - matches typer.Typer's own kwarg name
    """A typer app with the one shared piece every source's CLI needs: ``--log-level``."""
    app = typer.Typer(help=help, add_completion=False)

    @app.callback()
    def _main(log_level: str = typer.Option("INFO", "--log-level", help="loguru level.")) -> None:
        configure(log_level)

    return app


def _run_signature(ingest: Callable[..., dict]) -> inspect.Signature:
    """``ingest``'s own keyword parameters -- typer builds CLI options from these.

    Every ``ingest()`` module has ``from __future__ import annotations``, so
    ``inspect.signature`` alone hands back unevaluated strings (``"str | None"``)
    that typer/click can't turn into a parser. ``get_type_hints`` resolves them
    to real type objects first.
    """
    hints = typing.get_type_hints(ingest)
    params = [
        p.replace(annotation=hints.get(pname, p.annotation))
        for pname, p in inspect.signature(ingest).parameters.items()
        if pname not in _NOT_CLI_PARAMS
    ]
    return inspect.Signature(params)


def _echo_summary(summary: dict[str, Any]) -> None:
    """One generic report line -- every ingest() returns a superset of ``ops.Run.summary()``."""
    target = summary.get("table") or summary.get("schema") or "?"
    rows = summary.get("rows") or 0
    delta = "changed" if summary.get("changed") else "no change (idempotent)"
    typer.echo(f"{target} <- {rows:,} rows (status={summary.get('status')}) — {delta}")


def build_app(name: str, ingest: Callable[..., dict], *, help: str) -> typer.Typer:  # noqa: A002
    """A ready-to-run typer app for a registered source: ``--log-level`` + a generated ``run``.

    ``name`` must be one of :data:`ops.SOURCES`'s names -- an unregistered source
    has no ledger row to attribute a run to, so wiring a CLI for one is refused
    outright rather than silently running unattributed (issue #52). The matching
    :class:`ops.Source` is stamped with ``ingest`` (via ``dataclasses.replace`` --
    ``SOURCES`` itself stays free of ingest-extra imports, see ``ops.Source.ingest``),
    so the source that ends up driving the CLI is the registered one, not just a
    same-named function the caller happened to pass in.
    """
    matches = [s for s in ops.SOURCES if s.name == name]
    if not matches:
        raise ValueError(
            f"{name!r} is not registered in ops.SOURCES -- register it there before "
            "wiring a CLI; a source not in the registry cannot be run"
        )
    source = dataclasses.replace(matches[0], ingest=ingest)
    app = base_app(help)

    def run(**kwargs: Any) -> None:
        _echo_summary(source.ingest(**kwargs))

    run.__signature__ = _run_signature(source.ingest)  # type: ignore[attr-defined]
    app.command("run")(run)
    return app
