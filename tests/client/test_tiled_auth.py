from tiled.client.auth import TiledAuth


def test_no_token_directory():
    auth = TiledAuth(
        refresh_url="https://example.com/refresh",
        csrf_token="placeholder_csrf_token",
        token_directory=None,
    )
    assert auth.sync_get_token("access_token") is None
    assert auth.sync_get_token("refresh_token") is None
    assert auth.sync_get_token("access_token", reload_from_disk=True) is None
    assert auth.sync_get_token("refresh_token", reload_from_disk=True) is None
    auth.sync_set_token("access_token", "placeholder_access_token")
    auth.sync_set_token("refresh_token", "placeholder_refresh_token")
    assert auth.sync_get_token("access_token") == "placeholder_access_token"
    assert auth.sync_get_token("refresh_token") == "placeholder_refresh_token"
