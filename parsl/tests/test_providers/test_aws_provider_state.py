import json
import logging
import os
from unittest import mock

import pytest

pytest.importorskip("boto3")

from botocore.exceptions import ClientError  # noqa: E402

from parsl.errors import ConfigurationError  # noqa: E402
from parsl.providers import AWSProvider  # noqa: E402

INFRA_ID = "parsl.aws.test"
OWNERSHIP_TAGS = [{"Key": "parsl:provider", "Value": "AWSProvider"},
                  {"Key": "parsl:infrastructure", "Value": INFRA_ID}]


@pytest.fixture(autouse=True)
def fixed_infrastructure_id():
    with mock.patch.object(AWSProvider, "generate_aws_id", return_value=INFRA_ID):
        yield


def _configure_session(Session):
    """Make the mocked boto3 session able to run create_vpc()."""
    session = Session.return_value
    client = session.client.return_value
    ec2 = session.resource.return_value

    vpc = ec2.create_vpc.return_value
    vpc.id = vpc.vpc_id = "vpc-new"
    vpc.security_groups.all.return_value = []
    vpc.create_route_table.return_value.id = "rtb-new"
    vpc.create_subnet.return_value.id = "subnet-new"
    vpc.create_security_group.return_value.id = "sg-new"
    ec2.create_internet_gateway.return_value.id = "igw-new"
    client.describe_availability_zones.return_value = {
        "AvailabilityZones": [{"State": "available", "ZoneName": "us-east-2a"}]
    }
    client.get_paginator.return_value.paginate.return_value = [{"Reservations": []}]
    # Everything created in the test is described with this provider's tags
    for method, key in [("describe_vpcs", "Vpcs"), ("describe_security_groups", "SecurityGroups"),
                        ("describe_internet_gateways", "InternetGateways"),
                        ("describe_subnets", "Subnets"), ("describe_route_tables", "RouteTables")]:
        getattr(client, method).return_value = {key: [{"Tags": OWNERSHIP_TAGS}]}
    return client, ec2, vpc


def _provider(state_file):
    return AWSProvider(image_id="ami-1", key_name="k", profile="p", state_file=str(state_file))


@pytest.mark.local
@pytest.mark.parametrize("content", [
    "{not json",
    json.dumps({"sgID": "sg-1", "snIDs": []}),
    json.dumps({"vpcID": "", "sgID": "sg-1", "snIDs": []}),
    json.dumps({"vpcID": "vpc-1", "sgID": "sg-1", "snIDs": "subnet-1"}),
    json.dumps(["vpc-1"]),
])
def test_invalid_state_file_raises_without_creating_vpc(tmp_path, content):
    state_file = tmp_path / "state.json"
    state_file.write_text(content)

    with mock.patch("parsl.providers.aws.aws.boto3.session.Session") as Session:
        _, ec2, _ = _configure_session(Session)
        with pytest.raises(ConfigurationError, match=str(state_file)):
            _provider(state_file)

    ec2.create_vpc.assert_not_called()
    assert state_file.read_text() == content


@pytest.mark.local
def test_missing_state_file_creates_and_records_infrastructure(tmp_path, caplog):
    state_file = tmp_path / "state.json"

    with mock.patch("parsl.providers.aws.aws.boto3.session.Session") as Session:
        _, ec2, _ = _configure_session(Session)
        with caplog.at_level(logging.INFO, logger="parsl.providers.aws.aws"):
            _provider(state_file)

    ec2.create_vpc.assert_called_once()
    state = json.loads(state_file.read_text())
    assert state["vpcID"] == "vpc-new"
    assert state["sgID"] == "sg-new"
    assert state["snIDs"] == ["subnet-new"]
    assert state["igwID"] == "igw-new"
    assert state["rtbID"] == "rtb-new"
    assert state["infrastructureID"] == INFRA_ID
    assert str(state_file) in caplog.text


def _assert_tagged(create_call, resource_type):
    [tag_spec] = create_call.kwargs["TagSpecifications"]
    assert tag_spec["ResourceType"] == resource_type
    for tag in OWNERSHIP_TAGS:
        assert tag in tag_spec["Tags"]


@pytest.mark.local
def test_created_resources_carry_ownership_tags(tmp_path):
    state_file = tmp_path / "state.json"

    with mock.patch("parsl.providers.aws.aws.boto3.session.Session") as Session:
        client, ec2, vpc = _configure_session(Session)
        provider = _provider(state_file)

    assert provider.infrastructure_id == INFRA_ID
    _assert_tagged(ec2.create_vpc.call_args, "vpc")
    _assert_tagged(ec2.create_internet_gateway.call_args, "internet-gateway")
    _assert_tagged(vpc.create_route_table.call_args, "route-table")
    _assert_tagged(vpc.create_subnet.call_args, "subnet")
    _assert_tagged(vpc.create_security_group.call_args, "security-group")
    # Tags are applied on creation, never afterwards with create_tags
    ec2.create_tags.assert_not_called()
    client.create_tags.assert_not_called()

    # Instances are tagged through create_name_tag_spec() in spin_up_instance()
    client.describe_instance_type_offerings.return_value = {
        "InstanceTypeOfferings": [{"Location": "us-east-2a"}]}
    client.describe_subnets.return_value = {
        "Subnets": [{"SubnetId": "subnet-new", "AvailabilityZone": "us-east-2a"}]}
    provider.spin_up_instance("true", "job")
    _assert_tagged(ec2.create_instances.call_args, "instance")


@pytest.mark.local
def test_relative_state_file_logs_absolute_path(tmp_path, caplog, monkeypatch):
    monkeypatch.chdir(tmp_path)
    (tmp_path / "rel.json").write_text(json.dumps({"vpcID": "vpc-1", "sgID": "sg-1", "snIDs": []}))

    with mock.patch("parsl.providers.aws.aws.boto3.session.Session"):
        with caplog.at_level(logging.INFO, logger="parsl.providers.aws.aws"):
            provider = _provider("rel.json")

    assert provider.state_file == "rel.json"
    assert os.path.join(str(tmp_path), "rel.json") in caplog.text


@pytest.mark.local
def test_partial_creation_failure_rolls_back(tmp_path):
    state_file = tmp_path / "state.json"

    with mock.patch("parsl.providers.aws.aws.boto3.session.Session") as Session:
        client, _, vpc = _configure_session(Session)
        vpc.create_security_group.return_value.authorize_ingress.side_effect = ClientError(
            {"Error": {"Code": "UnauthorizedOperation", "Message": "no"}}, "AuthorizeSecurityGroupIngress")

        with pytest.raises(ClientError):
            _provider(state_file)

    client.delete_security_group.assert_called_once_with(GroupId="sg-new")
    client.detach_internet_gateway.assert_called_once_with(InternetGatewayId="igw-new", VpcId="vpc-new")
    client.delete_internet_gateway.assert_called_once_with(InternetGatewayId="igw-new")
    client.delete_subnet.assert_called_once_with(SubnetId="subnet-new")
    client.delete_route_table.assert_called_once_with(RouteTableId="rtb-new")
    client.delete_vpc.assert_called_once_with(VpcId="vpc-new")
    assert not state_file.exists()


@pytest.mark.local
def test_failed_rollback_is_logged_and_original_error_raised(tmp_path, caplog):
    state_file = tmp_path / "state.json"

    with mock.patch("parsl.providers.aws.aws.boto3.session.Session") as Session:
        client, _, vpc = _configure_session(Session)
        vpc.create_subnet.side_effect = ClientError(
            {"Error": {"Code": "SubnetLimitExceeded", "Message": "no"}}, "CreateSubnet")
        client.delete_vpc.side_effect = ClientError(
            {"Error": {"Code": "UnauthorizedOperation", "Message": "no"}}, "DeleteVpc")

        with pytest.raises(ClientError, match="SubnetLimitExceeded"):
            _provider(state_file)

    assert "must be removed manually" in caplog.text
    assert "vpc-new" in caplog.text
