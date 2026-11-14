import json
import logging
from unittest import mock

import pytest

pytest.importorskip("boto3")

from botocore.exceptions import ClientError  # noqa: E402

from parsl.providers import AWSProvider  # noqa: E402

INFRA_ID = "parsl.aws.1"

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
DELETE_CALLS = TEARDOWN_CALLS - {"terminate_instances", "get_waiter().wait"}

DESCRIBE = {
    "describe_vpcs": "Vpcs",
    "describe_security_groups": "SecurityGroups",
    "describe_internet_gateways": "InternetGateways",
    "describe_subnets": "Subnets",
    "describe_route_tables": "RouteTables",
}


def _tags(infra_id):
    tags = [{"Key": "Name", "Value": "x"}]
    if infra_id:
        tags += [{"Key": "parsl:provider", "Value": "AWSProvider"},
                 {"Key": "parsl:infrastructure", "Value": infra_id}]
    return tags


def _write_state(tmp_path, **extra):
    state = {"vpcID": "vpc-1", "sgID": "sg-1", "snIDs": ["subnet-1", "subnet-2"],
             "igwID": "igw-1", "rtbID": "rtb-1", "infrastructureID": INFRA_ID,
             "instances": ["i-stale"]}
    state.update(extra)
    state = {k: v for k, v in state.items() if v is not None}
    path = tmp_path / "awsproviderstate.json"
    path.write_text(json.dumps(state))
    return path


def _instance_pages(ids):
    return [{"Reservations": [{"Instances": [{"InstanceId": i} for i in ids]}]}]


def _make_provider(state_file, resource_infra_id=INFRA_ID, owned_instances=("i-1", "i-2"),
                   session_instances=(), foreign_instances=()):
    """Build a provider from state_file with a mocked EC2 client.

    Every resource is described with a parsl:infrastructure tag of resource_infra_id
    (untagged if None). Instance queries filtered by the ownership tag return
    owned_instances, queries by instance-id return session_instances, and the
    VPC-wide query (made after termination) returns foreign_instances.
    """
    with mock.patch("parsl.providers.aws.aws.boto3.session.Session"):
        provider = AWSProvider(image_id="ami-1", key_name="k", profile="p",
                               state_file=str(state_file))
    client = provider.client
    for method, key in DESCRIBE.items():
        getattr(client, method).return_value = {key: [{"Tags": _tags(resource_infra_id)}]}

    def paginate(Filters):
        names = {f["Name"] for f in Filters}
        if "tag:parsl:infrastructure" in names:
            return _instance_pages(owned_instances)
        if "instance-id" in names:
            return _instance_pages(session_instances)
        return _instance_pages(foreign_instances)

    client.get_paginator.return_value.paginate.side_effect = paginate
    return provider


def _teardown_calls(client, names=TEARDOWN_CALLS):
    return [(name, kwargs) for name, _, kwargs in client.mock_calls if name in names]


def _not_found(code, op):
    return ClientError({"Error": {"Code": code, "Message": "gone"}}, op)


@pytest.mark.local
def test_teardown_order(tmp_path):
    state_file = _write_state(tmp_path)
    provider = _make_provider(state_file)

    assert provider.teardown() is True

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
    # instances to terminate are selected by the ownership tag within the VPC
    first_query = client.get_paginator.return_value.paginate.call_args_list[0].kwargs["Filters"]
    assert {"Name": "vpc-id", "Values": ["vpc-1"]} in first_query
    assert {"Name": "tag:parsl:infrastructure", "Values": [INFRA_ID]} in first_query
    assert not state_file.exists()


@pytest.mark.local
def test_foreign_instance_left_running(tmp_path, caplog):
    state_file = _write_state(tmp_path)
    provider = _make_provider(state_file, foreign_instances=["i-foreign"])

    with caplog.at_level(logging.WARNING):
        assert provider.teardown() is False

    client = provider.client
    client.terminate_instances.assert_called_once_with(InstanceIds=["i-1", "i-2"])
    assert _teardown_calls(client, DELETE_CALLS) == []
    assert "i-foreign" in caplog.text
    assert state_file.exists()


@pytest.mark.local
def test_legacy_state_file_deletes_nothing(tmp_path, caplog):
    # State files written by released Parsl: no infrastructure ID, IGW or
    # route table, and the resources carry no ownership tags.
    state_file = _write_state(tmp_path, infrastructureID=None, igwID=None, rtbID=None)
    provider = _make_provider(state_file, resource_infra_id=None, owned_instances=["i-wrong"],
                              session_instances=["i-session"])
    assert provider.infrastructure_id is None
    assert provider.internet_gateway is None
    assert provider.route_table is None
    provider.instances = ["i-session"]

    with caplog.at_level(logging.WARNING):
        assert provider.teardown() is False

    client = provider.client
    # Only the instance launched by this provider object is terminated
    client.terminate_instances.assert_called_once_with(InstanceIds=["i-session"])
    assert _teardown_calls(client, DELETE_CALLS) == []
    client.describe_internet_gateways.assert_not_called()
    client.describe_route_tables.assert_not_called()
    for resource_id in ["vpc-1", "sg-1", "subnet-1", "subnet-2"]:
        assert resource_id in caplog.text
    assert "manually" in caplog.text
    assert state_file.exists()


@pytest.mark.local
@pytest.mark.parametrize("vpc_infra_id", [None, "parsl.aws.other"])
def test_state_file_pointing_at_foreign_vpc_deletes_nothing(tmp_path, caplog, vpc_infra_id):
    state_file = _write_state(tmp_path)
    provider = _make_provider(state_file, owned_instances=[])
    provider.client.describe_vpcs.return_value = {"Vpcs": [{"Tags": _tags(vpc_infra_id)}]}

    with caplog.at_level(logging.WARNING):
        assert provider.teardown() is False

    assert _teardown_calls(provider.client, DELETE_CALLS) == []
    assert "vpc-1" in caplog.text
    assert state_file.exists()


@pytest.mark.local
def test_foreign_resource_in_owned_vpc_is_kept(tmp_path, caplog):
    state_file = _write_state(tmp_path)
    provider = _make_provider(state_file, owned_instances=[])
    client = provider.client
    client.describe_subnets.side_effect = lambda SubnetIds: {
        "Subnets": [{"Tags": _tags(INFRA_ID if SubnetIds == ["subnet-1"] else None)}]}

    with caplog.at_level(logging.WARNING):
        assert provider.teardown() is False

    deleted_subnets = [kw for name, kw in _teardown_calls(client) if name == "delete_subnet"]
    assert deleted_subnets == [{"SubnetId": "subnet-1"}]
    client.delete_security_group.assert_called_once_with(GroupId="sg-1")
    client.delete_route_table.assert_called_once_with(RouteTableId="rtb-1")
    client.delete_vpc.assert_not_called()
    assert "subnet-2" in caplog.text
    assert state_file.exists()


@pytest.mark.local
def test_stale_instances_not_restored(tmp_path):
    state_file = _write_state(tmp_path)
    provider = _make_provider(state_file)
    assert provider.instances == []

    # cancelling an id this provider does not know about must not raise
    assert provider.cancel(["i-unknown"]) == [True]


@pytest.mark.local
def test_no_instances_skips_terminate_and_waiter(tmp_path):
    state_file = _write_state(tmp_path)
    provider = _make_provider(state_file, owned_instances=[])

    assert provider.teardown() is True

    provider.client.terminate_instances.assert_not_called()
    provider.client.get_waiter.assert_not_called()
    provider.client.delete_vpc.assert_called_once_with(VpcId="vpc-1")


@pytest.mark.local
def test_teardown_idempotent_on_not_found(tmp_path):
    state_file = _write_state(tmp_path)
    provider = _make_provider(state_file, owned_instances=[])
    client = provider.client

    # The resources are gone by the time they are deleted
    client.delete_security_group.side_effect = _not_found("InvalidGroup.NotFound", "DeleteSecurityGroup")
    client.detach_internet_gateway.side_effect = _not_found("Gateway.NotAttached", "DetachInternetGateway")
    client.delete_internet_gateway.side_effect = _not_found("InvalidInternetGatewayID.NotFound",
                                                            "DeleteInternetGateway")
    client.delete_subnet.side_effect = _not_found("InvalidSubnetID.NotFound", "DeleteSubnet")
    client.delete_route_table.side_effect = _not_found("InvalidRouteTableID.NotFound", "DeleteRouteTable")
    client.delete_vpc.side_effect = _not_found("InvalidVpcID.NotFound", "DeleteVpc")

    assert provider.teardown() is True

    assert client.delete_subnet.call_count == 2
    client.delete_vpc.assert_called_once_with(VpcId="vpc-1")
    assert not state_file.exists()

    # A second call on the same provider is a no-op
    assert provider.teardown() is True
    client.delete_vpc.assert_called_once_with(VpcId="vpc-1")


@pytest.mark.local
def test_teardown_rerun_after_resources_deleted(tmp_path):
    state_file = _write_state(tmp_path)
    provider = _make_provider(state_file, owned_instances=[])
    client = provider.client
    client.describe_security_groups.side_effect = _not_found("InvalidGroup.NotFound", "DescribeSecurityGroups")
    client.describe_subnets.side_effect = _not_found("InvalidSubnetID.NotFound", "DescribeSubnets")

    assert provider.teardown() is True

    client.delete_security_group.assert_not_called()
    client.delete_subnet.assert_not_called()
    client.delete_internet_gateway.assert_called_once_with(InternetGatewayId="igw-1")
    client.delete_vpc.assert_called_once_with(VpcId="vpc-1")
    assert not state_file.exists()


@pytest.mark.local
def test_teardown_vpc_already_gone(tmp_path):
    state_file = _write_state(tmp_path, infrastructureID=None)
    provider = _make_provider(state_file, owned_instances=[])
    provider.client.describe_vpcs.side_effect = _not_found("InvalidVpcID.NotFound", "DescribeVpcs")

    assert provider.teardown() is True

    assert _teardown_calls(provider.client, DELETE_CALLS) == []
    assert not state_file.exists()


@pytest.mark.local
def test_teardown_raises_and_keeps_state_file_on_other_errors(tmp_path):
    state_file = _write_state(tmp_path)
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
    state_file = _write_state(tmp_path)
    provider = _make_provider(state_file)
    provider.write_state_file()

    state = json.loads(state_file.read_text())
    assert state["igwID"] == "igw-1"
    assert state["rtbID"] == "rtb-1"
    assert state["infrastructureID"] == INFRA_ID

    reloaded = _make_provider(state_file)
    assert reloaded.internet_gateway == "igw-1"
    assert reloaded.route_table == "rtb-1"
    assert reloaded.infrastructure_id == INFRA_ID
