"""Read platform credentials from Google Secret Manager (via the gcloud CLI).

The shared lake's credentials live in GSM (project ``cdsci-infra``), so the
Postgres-catalog backend reads them here rather than from ``.env`` — one place
for every project. Local/dev work (the single-file catalog) needs none of this,
so ``gcloud`` is only required when ``lake_backend == "postgres"``.

Values are returned to the caller and never logged.
"""

from __future__ import annotations

import functools
import subprocess


@functools.cache
def get_secret(name: str, project: str) -> str:
    """Return the latest version of secret ``name`` in GSM project ``project``.

    Cached per process so repeated lake connections don't re-shell out. Raises a
    clear error if ``gcloud`` is missing or the secret can't be read.
    """
    try:
        result = subprocess.run(
            [
                "gcloud", "secrets", "versions", "access", "latest",
                f"--secret={name}", f"--project={project}",
            ],
            capture_output=True,
            text=True,
            check=True,
        )
    except FileNotFoundError as exc:  # gcloud not installed
        raise RuntimeError(
            "gcloud CLI not found; it is required to read lake secrets from "
            f"Google Secret Manager (project {project!r})."
        ) from exc
    except subprocess.CalledProcessError as exc:
        raise RuntimeError(
            f"Could not read secret {name!r} from project {project!r}: "
            f"{exc.stderr.strip()}"
        ) from exc
    return result.stdout.strip()
