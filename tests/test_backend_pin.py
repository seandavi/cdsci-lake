"""tests/conftest.py pins the local lake backend for every test."""
def test_the_backend_is_pinned_local_even_if_the_shell_says_postgres():
    from cdsci.lake.config import Settings

    assert Settings(storage_base_uri="file:///nonexistent").lake_backend == "local"
