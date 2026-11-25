import json
from unittest import mock

import pytest

from parsl.jobs.states import JobState

pytest.importorskip("boto3")

from parsl.providers.aws import aws  # noqa: E402


@pytest.fixture
def provider(tmp_path):
    """An AWSProvider wired to a mock boto3 session, reusing existing state
    so that no infrastructure is created."""
    state_file = tmp_path / "awsproviderstate.json"
    state_file.write_text(json.dumps(
        {"vpcID": "vpc-1", "sgID": "sg-1", "snIDs": ["subnet-1"], "instances": []}
    ))
    with mock.patch.object(aws.boto3.session, "Session") as session:
        p = aws.AWSProvider(image_id="ami-1", key_name="key", profile="p", state_file=str(state_file))
    assert p.client is session.return_value.client.return_value
    return p


def _pages(*instances):
    """describe_instances paginator output for (instance_id, state) pairs."""
    return [{"Reservations": [{"Instances": [
        {"InstanceId": iid, "State": {"Name": state}} for iid, state in instances
    ]}]}]


@pytest.mark.local
def test_status_no_jobs_does_not_describe_account(provider):
    """With no jobs, status must not call describe_instances: an empty
    InstanceIds list describes every instance in the region (#3764)."""
    assert provider.status([]) == []
    provider.client.describe_instances.assert_not_called()
    provider.client.get_paginator.assert_not_called()


@pytest.mark.local
def test_status_is_in_job_id_order_and_scoped(provider):
    provider.resources = {"i-a": {}, "i-b": {}}
    provider.client.get_paginator.return_value.paginate.return_value = _pages(
        ("i-b", "running"), ("i-a", "pending"), ("i-other", "stopped")
    )

    states = [s.state for s in provider.status(["i-a", "i-b"])]

    assert states == [JobState.PENDING, JobState.RUNNING]
    provider.client.get_paginator.return_value.paginate.assert_called_once_with(
        Filters=[{"Name": "instance-id", "Values": ["i-a", "i-b"]}]
    )
    assert provider.resources["i-b"]["status"].state == JobState.RUNNING


@pytest.mark.local
def test_status_vanished_instance_is_completed(provider):
    """Terminated instances eventually disappear from describe_instances."""
    provider.resources = {"i-a": {}, "i-gone": {}}
    provider.client.get_paginator.return_value.paginate.return_value = _pages(("i-a", "running"))

    states = [s.state for s in provider.status(["i-a", "i-gone"])]

    assert states == [JobState.RUNNING, JobState.COMPLETED]
