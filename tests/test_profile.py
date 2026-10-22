from pathlib import Path

import pytest
import yaml

from oncall.profile import ProfileError, load_profile

ROOT = Path(__file__).resolve().parents[1]
FIXTURES = Path(__file__).parent / "fixtures" / "profiles"


def write_profile(tmp_path: Path, data: dict) -> Path:
    path = tmp_path / "profile.yaml"
    path.write_text(yaml.safe_dump(data), encoding="utf-8")
    return path


def minimal() -> dict:
    return yaml.safe_load((FIXTURES / "minimal.yaml").read_text(encoding="utf-8"))


@pytest.mark.parametrize("path", sorted((ROOT / "profiles").glob("*.yaml")), ids=lambda p: p.name)
def test_shipped_profiles_load(path: Path) -> None:
    profile = load_profile(path, env={})
    assert profile.name


def test_minimal_profile_defaults() -> None:
    profile = load_profile(FIXTURES / "minimal.yaml")
    assert profile.flags is None
    assert profile.backends.logs is None
    assert profile.backends.metrics.timeout_seconds == 10.0
    assert profile.owners == {}


def test_env_reference_uses_environment(tmp_path: Path) -> None:
    data = minimal()
    data["backends"]["metrics"]["url"] = "${PROM_URL:-http://default:9090}"
    path = write_profile(tmp_path, data)

    assert load_profile(path, env={}).backends.metrics.url == "http://default:9090"
    assert load_profile(path, env={"PROM_URL": "http://x:1"}).backends.metrics.url == "http://x:1"


def test_missing_env_reference_without_default_fails(tmp_path: Path) -> None:
    data = minimal()
    data["flags"] = {"provider": {"type": "flagd_ui", "url": "http://f", "options": {"token": "${FLAG_TOKEN}"}}}
    with pytest.raises(ProfileError, match="FLAG_TOKEN"):
        load_profile(write_profile(tmp_path, data), env={})


def test_unknown_key_is_rejected(tmp_path: Path) -> None:
    data = minimal()
    data["alerts"]["servce_label"] = "typo"
    with pytest.raises(ProfileError, match="servce_label"):
        load_profile(write_profile(tmp_path, data))


def test_missing_required_section_is_rejected(tmp_path: Path) -> None:
    data = minimal()
    del data["alerts"]
    with pytest.raises(ProfileError, match="alerts"):
        load_profile(write_profile(tmp_path, data))


def test_duplicate_allowed_flag_is_rejected(tmp_path: Path) -> None:
    data = minimal()
    data["flags"] = {
        "provider": {"type": "flagd_ui", "url": "http://f"},
        "allowed": [{"name": "a"}, {"name": "a", "values": ["off"]}],
    }
    with pytest.raises(ProfileError, match="duplicate allowed flags: a"):
        load_profile(write_profile(tmp_path, data))


def test_is_allowed(tmp_path: Path) -> None:
    data = minimal()
    data["flags"] = {
        "provider": {"type": "flagd_ui", "url": "http://f"},
        "allowed": [{"name": "restricted", "values": ["off"]}, {"name": "any"}],
    }
    flags = load_profile(write_profile(tmp_path, data)).flags
    assert flags is not None
    assert flags.is_allowed("restricted", "off")
    assert not flags.is_allowed("restricted", "on")
    assert flags.is_allowed("any", "on")
    assert not flags.is_allowed("unlisted", "off")


def test_unreadable_and_malformed_files(tmp_path: Path) -> None:
    with pytest.raises(ProfileError, match="cannot read"):
        load_profile(tmp_path / "missing.yaml")
    bad = tmp_path / "bad.yaml"
    bad.write_text("name: [unclosed", encoding="utf-8")
    with pytest.raises(ProfileError, match="invalid YAML"):
        load_profile(bad)
    scalar = tmp_path / "scalar.yaml"
    scalar.write_text("just a string", encoding="utf-8")
    with pytest.raises(ProfileError, match="mapping"):
        load_profile(scalar)
