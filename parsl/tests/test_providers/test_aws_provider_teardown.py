import json
from unittest import mock

import pytest

pytest.importorskip("boto3")

from botocore.exceptions import ClientError  # noqa: E402

from parsl.providers import AWSProvider  # noqa: E402

TEARDOWN_CALLS = {
    "terminate_instances",
    "get_waiter().wait",
    "delete_security_group",
    "detach_internet_gateway",
    "delete_internet_gateway",
    "delete_subnet",
    "delete_route_table",
    "delete_vpc",
}


def _write_state(tmp_path, **extra):
    state = {"vpcID": "vpc-1", "sgID": "sg-1", "snIDs": ["subnet-1", "subnet-2"],
             "instances": ["i-stale"]}
    state.update(extra)
    path = tmp_path / "awsproviderstate.json"
    path.write_text(json.dumps(state))
    return path


def _make_provider(state_file):
    with mock.patch("parsl.providers.aws.aws.boto3.session.Session"):
        provider = AWSProvider(image_id="ami-1", key_name="k", profile="p",
                               state_file=str(state_file))
    provider.client.get_paginator.return_value.paginate.return_value = [
        {"Reservations": [{"Instances": [{"InstanceId": "i-1"}, {"InstanceId": "i-2"}]}]}
    ]
    provider.client.describe_internet_gateways.return_value = {
        "InternetGateways": [{"InternetGatewayId": "igw-found"}]
    }
    provider.client.describe_route_tables.return_value = {
        "RouteTables": [
            {"RouteTableId": "rtb-main", "Associations": [{"Main": True}]},
            {"RouteTableId": "rtb-found", "Associations": [{"Main": False, "SubnetId": "subnet-1"}]},
        ]
    }
    return provider


def _teardown_calls(client):
    return [(name, kwargs) for name, _, kwargs in client.mock_calls if name in TEARDOWN_CALLS]


@pytest.mark.local
def test_teardown_order(tmp_path):
    state_file = _write_state(tmp_path, igwID="igw-1", rtbID="rtb-1")
    provider = _make_provider(state_file)

    provider.teardown()

    client = provider.client
    assert _teardown_calls(client) == [
        ("terminate_instances", {"InstanceIds": ["i-1", "i-2"]}),
        ("get_waiter().wait", {"InstanceIds": ["i-1", "i-2"]}),
        ("delete_security_group", {"GroupId": "sg-1"}),
        ("detach_internet_gateway", {"InternetGatewayId": "igw-1", "VpcId": "vpc-1"}),
        ("delete_internet_gateway", {"InternetGatewayId": "igw-1"}),
        ("delete_subnet", {"SubnetId": "subnet-1"}),
        ("delete_subnet", {"SubnetId": "subnet-2"}),
        ("delete_route_table", {"RouteTableId": "rtb-1"}),
        ("delete_vpc", {"VpcId": "vpc-1"}),
    ]
    client.get_waiter.assert_called_once_with("instance_terminated")
    client.describe_internet_gateways.assert_not_called()
    client.describe_route_tables.assert_not_called()
    assert not state_file.exists()


@pytest.mark.local
def test_old_state_file_discovers_igw_and_route_table(tmp_path):
    # State files written before igwID/rtbID were persisted
    state_file = _write_state(tmp_path)
    provider = _make_provider(state_file)
    assert provider.internet_gateway is None
    assert provider.route_table is None

    provider.teardown()

    calls = _teardown_calls(provider.client)
    assert ("detach_internet_gateway", {"InternetGatewayId": "igw-found", "VpcId": "vpc-1"}) in calls
    assert ("delete_internet_gateway", {"InternetGatewayId": "igw-found"}) in calls
    rtb_deletes = [kw for name, kw in calls if name == "delete_route_table"]
    assert rtb_deletes == [{"RouteTableId": "rtb-found"}]
    assert not state_file.exists()


@pytest.mark.local
def test_stale_instances_not_restored(tmp_path):
    state_file = _write_state(tmp_path)
    provider = _make_provider(state_file)
    assert provider.instances == []

    # cancelling an id this provider does not know about must not raise
    assert provider.cancel(["i-unknown"]) == [True]


@pytest.mark.local
def test_no_instances_skips_terminate_and_waiter(tmp_path):
    state_file = _write_state(tmp_path, igwID="igw-1", rtbID="rtb-1")
    provider = _make_provider(state_file)
    provider.client.get_paginator.return_value.paginate.return_value = [{"Reservations": []}]

    provider.teardown()

    provider.client.terminate_instances.assert_not_called()
    provider.client.get_waiter.assert_not_called()
    provider.client.delete_vpc.assert_called_once_with(VpcId="vpc-1")


@pytest.mark.local
def test_teardown_idempotent_on_not_found(tmp_path):
    state_file = _write_state(tmp_path, igwID="igw-1", rtbID="rtb-1")
    provider = _make_provider(state_file)
    client = provider.client
    client.get_paginator.return_value.paginate.return_value = [{"Reservations": []}]

    def gone(code, op):
        return ClientError({"Error": {"Code": code, "Message": "gone"}}, op)

    client.delete_security_group.side_effect = gone("InvalidGroup.NotFound", "DeleteSecurityGroup")
    client.detach_internet_gateway.side_effect = gone("Gateway.NotAttached", "DetachInternetGateway")
    client.delete_internet_gateway.side_effect = gone("InvalidInternetGatewayID.NotFound",
                                                      "DeleteInternetGateway")
    client.delete_subnet.side_effect = gone("InvalidSubnetID.NotFound", "DeleteSubnet")
    client.delete_route_table.side_effect = gone("InvalidRouteTableID.NotFound", "DeleteRouteTable")
    client.delete_vpc.side_effect = gone("InvalidVpcID.NotFound", "DeleteVpc")

    provider.teardown()

    assert client.delete_subnet.call_count == 2
    client.delete_vpc.assert_called_once_with(VpcId="vpc-1")
    assert not state_file.exists()

    # A second call on the same provider is a no-op
    provider.teardown()
    client.delete_vpc.assert_called_once_with(VpcId="vpc-1")


@pytest.mark.local
def test_teardown_raises_and_keeps_state_file_on_other_errors(tmp_path):
    state_file = _write_state(tmp_path, igwID="igw-1", rtbID="rtb-1")
    provider = _make_provider(state_file)
    client = provider.client
    client.delete_subnet.side_effect = ClientError(
        {"Error": {"Code": "DependencyViolation", "Message": "in use"}}, "DeleteSubnet")

    with pytest.raises(ClientError):
        provider.teardown()

    client.delete_vpc.assert_not_called()
    assert state_file.exists()


@pytest.mark.local
def test_state_file_round_trip(tmp_path):
    state_file = _write_state(tmp_path, igwID="igw-1", rtbID="rtb-1")
    provider = _make_provider(state_file)
    provider.write_state_file()

    state = json.loads(state_file.read_text())
    assert state["igwID"] == "igw-1"
    assert state["rtbID"] == "rtb-1"

    reloaded = _make_provider(state_file)
    assert reloaded.internet_gateway == "igw-1"
    assert reloaded.route_table == "rtb-1"
