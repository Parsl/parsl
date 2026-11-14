"""Tests for the security group that AWSProvider creates.

These tests never contact AWS: boto3 sessions are replaced with mocks, and
the ingress rules passed to ``authorize_ingress`` are inspected.
"""
import json
from unittest import mock

import pytest

from parsl.errors import ConfigurationError

pytest.importorskip("boto3")

from parsl.providers import AWSProvider  # noqa: E402

VPC_CIDR = '10.0.0.0/16'


@pytest.fixture
def mock_session():
    with mock.patch('parsl.providers.aws.aws.boto3.session.Session') as session_cls:
        session = session_cls.return_value
        client = mock.MagicMock()
        client.describe_availability_zones.return_value = {
            'AvailabilityZones': [{'ZoneName': 'us-east-2a', 'State': 'available'}]
        }
        session.client.return_value = client

        # write_state_file() JSON-encodes these IDs, so they must be strings
        vpc = session.resource.return_value.create_vpc.return_value
        vpc.id = vpc.vpc_id = 'vpc-0123'
        vpc.create_subnet.return_value.id = 'subnet-0123'
        vpc.create_security_group.return_value.id = 'sg-0123'
        yield session_cls


def _make_provider(tmp_path, **kwargs):
    state_file = tmp_path / 'awsproviderstate.json'
    assert not state_file.exists()
    return AWSProvider(
        image_id='ami-00000000',
        key_name='test-key',
        profile='test-profile',
        state_file=str(state_file),
        **kwargs,
    )


def _ingress_rules(session_cls):
    ec2 = session_cls.return_value.resource.return_value
    vpc = ec2.create_vpc.return_value
    sg = vpc.create_security_group.return_value
    sg.authorize_ingress.assert_called_once()
    return sg.authorize_ingress.call_args.kwargs['IpPermissions']


def _cidrs(rule):
    return [r['CidrIp'] for r in rule['IpRanges']]


# The only rule open to the internet: ICMP "fragmentation needed", for path MTU discovery.
PMTU_RULE = {'IpProtocol': 'ICMP', 'FromPort': 3, 'ToPort': 4, 'IpRanges': [{'CidrIp': '0.0.0.0/0'}]}


def _rules_from(rules, cidr):
    return [r for r in rules if cidr in _cidrs(r)]


def _is_ssh_rule(rule):
    return rule['IpProtocol'].lower() == 'tcp' and rule['FromPort'] <= 22 <= rule['ToPort']


@pytest.mark.local
def test_default_has_no_public_ingress(mock_session, tmp_path):
    _make_provider(tmp_path)
    rules = _ingress_rules(mock_session)

    assert _rules_from(rules, '0.0.0.0/0') == [PMTU_RULE]
    for rule in rules:
        assert rule.get('Ipv6Ranges', []) == []
        if rule == PMTU_RULE:
            continue
        assert _cidrs(rule) == [VPC_CIDR], rule


@pytest.mark.local
def test_intra_vpc_rules_unchanged(mock_session, tmp_path):
    _make_provider(tmp_path)
    rules = _ingress_rules(mock_session)

    for proto in ('TCP', 'UDP'):
        assert {
            'IpProtocol': proto,
            'FromPort': 0,
            'ToPort': 65535,
            'IpRanges': [{'CidrIp': VPC_CIDR}],
        } in rules


@pytest.mark.local
def test_ssh_cidr_allows_ssh_from_that_cidr_only(mock_session, tmp_path):
    _make_provider(tmp_path, ssh_cidr='203.0.113.0/24')
    rules = _ingress_rules(mock_session)

    assert _rules_from(rules, '0.0.0.0/0') == [PMTU_RULE]

    from_admin = _rules_from(rules, '203.0.113.0/24')
    assert any(r['IpProtocol'].lower() == 'tcp' and r['FromPort'] == 22 and r['ToPort'] == 22
               for r in from_admin)
    # nothing other than SSH and ICMP is opened to the admin CIDR
    for rule in from_admin:
        assert (rule['FromPort'], rule['ToPort']) == (22, 22) or rule['IpProtocol'].lower() == 'icmp', rule

    # SSH is only reachable from the admin CIDR (and from inside the VPC)
    for rule in rules:
        if _is_ssh_rule(rule):
            assert set(_cidrs(rule)) <= {'203.0.113.0/24', VPC_CIDR}, rule


@pytest.mark.local
def test_ssh_cidr_list_and_bare_address(mock_session, tmp_path):
    p = _make_provider(tmp_path, ssh_cidr=['198.51.100.7', '203.0.113.0/24', '198.51.100.7/32'])
    rules = _ingress_rules(mock_session)

    ssh_rules = [r for r in rules if r['FromPort'] == 22 and r['ToPort'] == 22]
    assert len(ssh_rules) == 1
    # bare address normalised to /32, duplicates removed, order kept
    assert _cidrs(ssh_rules[0]) == ['198.51.100.7/32', '203.0.113.0/24']
    assert _rules_from(rules, '0.0.0.0/0') == [PMTU_RULE]

    # RepresentationMixin needs the constructor argument stored as-is
    assert 'ssh_cidr=' in repr(p)


@pytest.mark.local
def test_ssh_cidr_overlapping_vpc_does_not_duplicate_rules(mock_session, tmp_path):
    # AWS rejects duplicate (protocol, ports, cidr) permissions in one call
    _make_provider(tmp_path, ssh_cidr=VPC_CIDR)
    rules = _ingress_rules(mock_session)

    seen = set()
    for rule in rules:
        for cidr in _cidrs(rule):
            key = (rule['IpProtocol'].lower(), rule['FromPort'], rule['ToPort'], cidr)
            assert key not in seen, key
            seen.add(key)


@pytest.mark.local
@pytest.mark.parametrize('bad', [
    'not-a-cidr',
    '10.0.0.1/24',       # host bits set
    '256.0.0.0/8',
    '2001:db8::/32',     # the provider's VPC has no IPv6
    ['203.0.113.0/24', 'bogus'],
    [42],
    42,
])
def test_invalid_ssh_cidr_raises(mock_session, tmp_path, bad):
    with pytest.raises(ConfigurationError):
        _make_provider(tmp_path, ssh_cidr=bad)
    # validation happens before any AWS session is created
    mock_session.assert_not_called()


@pytest.mark.local
def test_reused_state_file_does_not_touch_security_group(mock_session, tmp_path, caplog):
    state_file = tmp_path / 'awsproviderstate.json'
    state_file.write_text(json.dumps(
        {'vpcID': 'vpc-1', 'sgID': 'sg-1', 'snIDs': ['subnet-1'], 'instances': []}
    ))
    AWSProvider(
        image_id='ami-00000000',
        key_name='test-key',
        profile='test-profile',
        state_file=str(state_file),
        ssh_cidr='203.0.113.0/24',
    )
    ec2 = mock_session.return_value.resource.return_value
    ec2.create_vpc.assert_not_called()
    assert 'sg-1' in caplog.text
