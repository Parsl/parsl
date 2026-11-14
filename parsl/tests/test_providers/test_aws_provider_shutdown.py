import json
import logging
from unittest import mock

import pytest

pytest.importorskip("boto3")

from parsl.providers import AWSProvider  # noqa: E402


@pytest.fixture
def make_provider(tmp_path):
    """Build an AWSProvider which talks to a mock boto3 session, and which
    re-uses infrastructure from a state file instead of creating a VPC."""
    state_file = tmp_path / "awsproviderstate.json"
    state_file.write_text(json.dumps(
        {"vpcID": "vpc-1", "sgID": "sg-1", "snIDs": ["subnet-1"], "instances": []}
    ))

    with mock.patch("parsl.providers.aws.aws.boto3.session.Session") as session:
        def _make(**kwargs):
            p = AWSProvider(image_id="ami-1", key_name="k", profile="p",
                            state_file=str(state_file), **kwargs)
            # The state file must be used: no infrastructure is created.
            assert not p.ec2.create_vpc.called
            assert p.vpc_id == "vpc-1"
            return p
        yield _make, state_file, session


@pytest.mark.local
def test_teardown_on_shutdown_defaults_off(make_provider):
    make, _, _ = make_provider
    assert make().teardown_on_shutdown is False


@pytest.mark.local
def test_shutdown_tears_down_when_enabled(make_provider):
    make, _, _ = make_provider
    p = make(teardown_on_shutdown=True)
    with mock.patch.object(p, "teardown") as teardown:
        p.shutdown()
    teardown.assert_called_once_with()


@pytest.mark.local
def test_shutdown_keeps_infrastructure_by_default(make_provider, caplog):
    make, state_file, _ = make_provider
    p = make()
    with mock.patch.object(p, "teardown") as teardown, caplog.at_level(logging.INFO):
        p.shutdown()
    assert not teardown.called
    assert str(state_file) in caplog.text
    assert "teardown()" in caplog.text
    assert "vpc-1" in caplog.text


@pytest.mark.local
def test_shutdown_skips_teardown_when_lingering(make_provider, caplog):
    make, _, _ = make_provider
    p = make(teardown_on_shutdown=True, linger=True)
    with mock.patch.object(p, "teardown") as teardown, caplog.at_level(logging.WARNING):
        p.shutdown()
    assert not teardown.called
    assert any(r.levelno == logging.WARNING and "linger" in r.getMessage() for r in caplog.records)


@pytest.mark.local
def test_shutdown_logs_teardown_failure(make_provider, caplog):
    make, _, _ = make_provider
    p = make(teardown_on_shutdown=True)
    with mock.patch.object(p, "teardown", side_effect=RuntimeError("boom")) as teardown, \
            caplog.at_level(logging.ERROR):
        p.shutdown()  # must not raise
    teardown.assert_called_once_with()
    assert any(r.levelno == logging.ERROR and r.exc_info and "boom" in str(r.exc_info[1])
               for r in caplog.records)
