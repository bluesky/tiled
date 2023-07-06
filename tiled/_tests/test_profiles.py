from ..profiles import (
    create_profile,
    delete_profile,
    get_default_profile_name,
    list_profiles,
    load_profiles,
    set_default_profile_name,
)


def test_integration(tmp_profiles_dir):
    URI = "http://example.com/api/v1"
    create_profile(name="test", uri=URI)
    assert "test" in list_profiles()
    _, profile_content = load_profiles()["test"]
    assert profile_content["uri"] == URI
    assert get_default_profile_name() is None
    set_default_profile_name("test")
    assert get_default_profile_name() == "test"
    set_default_profile_name(None)
    assert get_default_profile_name() is None
    delete_profile("test")
    assert "test" not in list_profiles()
