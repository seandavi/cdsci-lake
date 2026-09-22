"""``cdsci.lake.publish`` — release builder primitives (design §6.5/§6.6).

``release.py``: manifest/file-index/acceptance/receipt types. ``builder.py``: writes a
release's Parquet + JSON to an ``ObjectStore`` (``build_release``) and publishes its
manifest once accepted (``finalize_release``, ``record_release``). ``verify.py``: the
cold-path ``verify_release`` -- no ``ops`` call, no credentials, the seed of DuckDock's
``verify``.
"""
