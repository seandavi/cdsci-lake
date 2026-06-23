"""State Cancer Profiles — county/state cancer burden + risk + demographics.

Bulk-loaded from the maintainer's **monthly GitHub releases** of
``seandavi/state-cancer-profile-scraper`` (a versioned, re-curatable snapshot —
not a live scrape). Four lake tables under the ``scp`` schema:

* ``scp.incidence`` / ``scp.mortality`` — tidy cancer rates (age-adjusted rate,
  CIs, trend) by area x cancer x sex x race x stage x age.
* ``scp.risk`` — behavioral-risk-factor / screening prevalence by area x risk.
* ``scp.demographics`` — a WIDE socio-economic table (sparse, ~44 cols) keyed by
  area x topic x demographic group.

These are institution-neutral *place* facts; the state-level burden bridges into
the grant/literature graph (a geo crosswalk between FIPS/state-name and the
2-letter ``org_state`` used by reporter is a future ``ref`` concern).
"""

from .ingest import DOMAINS, curate, download_assets, ingest, resolve_latest

__all__ = ["DOMAINS", "curate", "download_assets", "ingest", "resolve_latest"]
