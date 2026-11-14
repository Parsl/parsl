import json
from unittest import mock

import pytest

pytest.importorskip("boto3")

from parsl.providers.aws.aws import AWSProvider  # noqa: E402

_SESSION = "parsl.providers.aws.aws.boto3.session.Session"


@pytest.fixture
def state_file(tmp_path):
    # An existing state file stops the provider from creating a VPC.
    path = tmp_path / "awsproviderstate.json"
    path.write_text(json.dumps(
        {"vpcID": "vpc-1", "sgID": "sg-1", "snIDs": ["subnet-1"], "instances": []}
    ))
    return str(path)


def _fake_session(resolved_region):
    """Return a Session stand-in that resolves the region like boto3 does:
    an explicit region_name wins, otherwise resolved_region is used."""
    def make(*args, region_name=None, **kwargs):
        session = mock.MagicMock()
        session.region_name = region_name if region_name is not None else resolved_region
        return session
    return make


def _provider(state_file, **kwargs):
    return AWSProvider("ami-1", "key", profile="p", state_file=state_file, **kwargs)


@pytest.mark.local
def test_region_resolved_from_profile(state_file):
    with mock.patch(_SESSION, side_effect=_fake_session("eu-west-1")) as session:
        provider = _provider(state_file)

    assert session.call_args.kwargs["region_name"] is None
    assert provider.region == "eu-west-1"
    assert "region='eu-west-1'" in repr(provider)


@pytest.mark.local
def test_region_falls_back_to_us_east_2(state_file):
    with mock.patch(_SESSION, side_effect=_fake_session(None)):
        provider = _provider(state_file)

    assert provider.region == "us-east-2"
    assert provider.session.region_name == "us-east-2"


@pytest.mark.local
def test_explicit_region_passed_through(state_file):
    with mock.patch(_SESSION, side_effect=_fake_session("eu-west-1")) as session:
        provider = _provider(state_file, region="ap-south-1")

    session.assert_called_once_with(profile_name="p", region_name="ap-south-1")
    assert provider.region == "ap-south-1"


@pytest.mark.local
def test_default_instance_type(state_file):
    with mock.patch(_SESSION, side_effect=_fake_session("eu-west-1")):
        provider = _provider(state_file)

    assert provider.instance_type == "t3.small"


@pytest.mark.local
def test_region_from_aws_config_file_with_real_boto3(state_file, tmp_path, monkeypatch):
    # Uses real boto3 with no network access: the region comes from a
    # scratch AWS config file.
    config = tmp_path / "config"
    config.write_text("[profile p]\nregion = eu-west-1\n")
    for var in ("AWS_REGION", "AWS_DEFAULT_REGION", "AWS_PROFILE"):
        monkeypatch.delenv(var, raising=False)
    monkeypatch.setenv("AWS_CONFIG_FILE", str(config))
    monkeypatch.setenv("AWS_SHARED_CREDENTIALS_FILE", str(tmp_path / "credentials"))
    monkeypatch.setenv("AWS_EC2_METADATA_DISABLED", "true")

    provider = _provider(state_file)

    assert provider.region == "eu-west-1"
    assert provider.client.meta.region_name == "eu-west-1"
