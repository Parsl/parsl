import json
import os
from unittest import mock

import pytest

from parsl.errors import ConfigurationError
from parsl.providers import AWSProvider

pytest.importorskip("boto3")

_MOCK_SESSION = "parsl.providers.aws.aws.boto3.session.Session"


@pytest.fixture
def state_file(tmp_path):
    """A state file describing existing infrastructure, so that the provider
    does not try to create a VPC."""
    path = tmp_path / "awsproviderstate.json"
    path.write_text(json.dumps({
        "vpcID": "vpc-1",
        "sgID": "sg-1",
        "snIDs": ["subnet-1"],
        "instances": [],
    }))
    return str(path)


@pytest.fixture(autouse=True)
def clear_aws_env(monkeypatch):
    for var in list(os.environ):
        if var.startswith("AWS_"):
            monkeypatch.delenv(var)


def _make_provider(state_file, **kwargs):
    return AWSProvider(image_id="ami-1", key_name="key", state_file=state_file, **kwargs)


@pytest.mark.local
def test_default_credential_chain(state_file):
    """With no profile, key_file or access key environment variables, the
    provider should defer to boto3's default credential chain (for example
    AWS_PROFILE, ~/.aws/config, SSO or an instance role)."""
    with mock.patch(_MOCK_SESSION) as mock_session:
        mock_session.return_value.get_credentials.return_value = mock.Mock()
        p = _make_provider(state_file)

    mock_session.assert_called_once_with(region_name="us-east-2")
    assert p.vpc_id == "vpc-1"


@pytest.mark.local
def test_no_credentials_found(state_file):
    with mock.patch(_MOCK_SESSION) as mock_session:
        mock_session.return_value.get_credentials.return_value = None
        with pytest.raises(ConfigurationError, match="No AWS credentials found"):
            _make_provider(state_file)


@pytest.mark.local
def test_explicit_profile(state_file):
    with mock.patch(_MOCK_SESSION) as mock_session:
        _make_provider(state_file, profile="myprofile")

    mock_session.assert_called_once_with(profile_name="myprofile", region_name="us-east-2")


def _write_key_file(tmp_path, creds):
    path = tmp_path / "creds.json"
    path.write_text(json.dumps(creds))
    return str(path)


@pytest.mark.local
def test_key_file_documented_keys(state_file, tmp_path):
    key_file = _write_key_file(tmp_path, {
        "AWSAccessKeyId": "AKIA1",
        "AWSSecretKey": "secret",
        "AWSSessionToken": "token",
    })
    with mock.patch(_MOCK_SESSION) as mock_session:
        _make_provider(state_file, key_file=key_file)

    mock_session.assert_called_once_with(region_name="us-east-2",
                                         aws_access_key_id="AKIA1",
                                         aws_secret_access_key="secret",
                                         aws_session_token="token")


@pytest.mark.local
def test_key_file_boto3_keys(state_file, tmp_path):
    key_file = _write_key_file(tmp_path, {
        "aws_access_key_id": "AKIA1",
        "aws_secret_access_key": "secret",
    })
    with mock.patch(_MOCK_SESSION) as mock_session:
        _make_provider(state_file, key_file=key_file)

    mock_session.assert_called_once_with(region_name="us-east-2",
                                         aws_access_key_id="AKIA1",
                                         aws_secret_access_key="secret")


@pytest.mark.local
def test_key_file_unknown_key(state_file, tmp_path):
    key_file = _write_key_file(tmp_path, {
        "AWSAccessKeyId": "AKIA1",
        "AWSSecretKey": "secret",
        "Bogus": "x",
    })
    with mock.patch(_MOCK_SESSION) as mock_session:
        with pytest.raises(ConfigurationError, match="AWSAccessKeyId"):
            _make_provider(state_file, key_file=key_file)

    mock_session.assert_not_called()


@pytest.mark.local
def test_missing_profile(state_file):
    from botocore.exceptions import ProfileNotFound

    with mock.patch(_MOCK_SESSION, side_effect=ProfileNotFound(profile="nosuchprofile")):
        with pytest.raises(ConfigurationError, match="nosuchprofile"):
            _make_provider(state_file, profile="nosuchprofile")
