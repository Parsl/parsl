import json
import os
import shutil
import subprocess
import sys
from unittest import mock

import pytest

from parsl.jobs.states import JobState, JobStatus
from parsl.providers import AWSProvider
from parsl.providers.errors import SubmitException

pytest.importorskip("boto3")

PYTHON_VERSION = "{}.{}".format(*sys.version_info[:2])


@pytest.fixture
def make_provider(tmp_path):
    """Make AWSProviders which never talk to AWS: boto3 sessions are mocked
    and the state file points at existing (fake) VPC resources, so that the
    constructor does not create any."""
    state_file = tmp_path / "awsproviderstate.json"
    state_file.write_text(json.dumps({"vpcID": "vpc-1", "sgID": "sg-1",
                                      "snIDs": ["subnet-1"], "instances": []}))

    with mock.patch("parsl.providers.aws.aws.boto3.session.Session") as session:
        client = session.return_value.client.return_value
        client.describe_instance_type_offerings.return_value = {
            "InstanceTypeOfferings": [{"Location": "us-east-2a"}]}
        client.describe_subnets.return_value = {
            "Subnets": [{"SubnetId": "subnet-1", "AvailabilityZone": "us-east-2a"}]}

        ec2 = session.return_value.resource.return_value
        launched = []

        def create_instances(**kwargs):
            instance_id = "i-{}".format(len(launched))
            launched.append(instance_id)
            return [mock.Mock(id=instance_id, instance_id=instance_id, state={"Name": "pending"})]
        ec2.create_instances.side_effect = create_instances

        def make(**kwargs):
            kwargs.setdefault("key_name", None)
            return AWSProvider("ami-1", profile="p", state_file=str(state_file), **kwargs)
        make.client = client
        make.ec2 = ec2
        yield make


@pytest.mark.local
def test_user_data_pins_parsl_version(make_provider):
    with mock.patch("parsl.providers.aws.aws.VERSION", "2026.11.09"):
        p = make_provider()
    user_data = p._user_data("process_worker_pool.py", "job")

    assert "pip install parsl==2026.11.09\n" in user_data
    assert "-m venv /opt/parsl-venv" in user_data
    assert "python3-venv" in user_data
    assert 'export PATH="/opt/parsl-venv/bin:$PATH"' in user_data
    assert "python" + PYTHON_VERSION in user_data
    assert "pip3 install" not in user_data
    assert "numpy" not in user_data


@pytest.mark.local
def test_user_data_dev_version(make_provider):
    with mock.patch("parsl.providers.aws.aws.VERSION", "1.3.0-dev"):
        p = make_provider()
    assert "github.com/Parsl/parsl" in p._user_data("cmd", "job")


@pytest.mark.local
def test_user_data_parsl_requirement(make_provider):
    p = make_provider(parsl_requirement="parsl[monitoring] >= 2026.1.1")
    assert "pip install 'parsl[monitoring] >= 2026.1.1'\n" in p._user_data("cmd", "job")


@pytest.mark.local
def test_user_data_order(make_provider):
    user_data = make_provider(worker_init="WORKER_INIT")._user_data("USER_SCRIPT", "job")
    install = user_data.index("pip install")
    assert install < user_data.index("WORKER_INIT") < user_data.index("USER_SCRIPT")


@pytest.mark.local
def test_user_data_shutdown(make_provider):
    user_data = make_provider(walltime="01:30:00")._user_data("cmd", "job")
    assert "shutdown -h +90\n" in user_data
    assert "trap 'shutdown -h now' EXIT" in user_data
    assert "halt" not in user_data
    assert "if ! false\n" in user_data

    assert "if ! true\n" in make_provider(linger=True)._user_data("cmd", "job")


@pytest.mark.local
def test_user_data_syntax(make_provider):
    if shutil.which("bash") is None:
        pytest.skip("requires bash")
    p = make_provider(worker_init="echo 'init $HOME'")
    command = p.launcher("process_worker_pool.py --debug", 1, 1, None)
    subprocess.run(["bash", "-n"], input=p._user_data(command, "job"), text=True, check=True)


STUB = """#!/bin/bash
echo "$(basename "$0")${*:+ $*}" >> "$STUB_LOG"
if [ "$1 $2" = "-m venv" ]
then
    mkdir -p "$3/bin"
    cp "$0" "$3/bin/python"
    cp "$0" "$3/bin/process_worker_pool.py"
fi
"""


def run_user_data(tmp_path, provider, command):
    """Run the rendered user data script with bash, with the commands which
    would change the system (package managers, pip, Python and shutdown) replaced
    by stubs which log their arguments, and return the log lines."""
    if shutil.which("bash") is None:
        pytest.skip("requires bash")

    stub_dir = tmp_path / "stubs"
    stub_dir.mkdir()
    for name in ["apt-get", "dnf", "shutdown", "pip", "pip3", "python3", "python" + PYTHON_VERSION]:
        (stub_dir / name).write_text(STUB)
        (stub_dir / name).chmod(0o755)

    # the script starts with cd ~, so this puts the venv in tmp_path
    script = tmp_path / "user_data.sh"
    command = provider.launcher(command, 1, 1, None)
    script.write_text(provider._user_data(command, "job").replace("/opt/parsl-venv", "./venv"))

    log = tmp_path / "stub.log"
    log.touch()
    env = dict(os.environ, HOME=str(tmp_path), STUB_LOG=str(log),
               PATH="{}:{}".format(stub_dir, os.environ["PATH"]))
    subprocess.run(["bash", str(script)], env=env, timeout=60)
    return log.read_text().splitlines()


@pytest.mark.local
def test_user_data_runs_worker(tmp_path, make_provider):
    log = run_user_data(tmp_path, make_provider(), "process_worker_pool.py --debug")

    assert log[0] == "shutdown -h +60"
    assert "python{} -m venv ./venv".format(PYTHON_VERSION) in log
    assert "python -m pip install parsl" in log[log.index("process_worker_pool.py --debug") - 1]
    assert log[-1] == "shutdown -h now"


@pytest.mark.local
def test_user_data_shuts_down_after_worker_failure(tmp_path, make_provider):
    # the default SingleNodeLauncher wraps this in a script which sets -e
    # and exits with the worker's exit code
    log = run_user_data(tmp_path, make_provider(), "exit 1")
    assert log[-1] == "shutdown -h now"


@pytest.mark.local
def test_user_data_shuts_down_after_worker_init_failure(tmp_path, make_provider):
    log = run_user_data(tmp_path, make_provider(worker_init="exit 2"), "process_worker_pool.py")
    assert "process_worker_pool.py" not in log
    assert log[-1] == "shutdown -h now"


@pytest.mark.local
def test_user_data_linger(tmp_path, make_provider):
    log = run_user_data(tmp_path, make_provider(linger=True), "process_worker_pool.py")
    assert "process_worker_pool.py" in log
    assert not [line for line in log if line.startswith("shutdown")]


@pytest.mark.local
def test_create_instances_defaults(make_provider):
    p = make_provider()
    assert p.submit("process_worker_pool.py", 1) == "i-0"

    kwargs = make_provider.ec2.create_instances.call_args.kwargs
    assert "KeyName" not in kwargs
    assert "IamInstanceProfile" not in kwargs
    assert "InstanceMarketOptions" not in kwargs
    assert kwargs["SubnetId"] == "subnet-1"
    assert kwargs["SecurityGroupIds"] == ["sg-1"]
    assert kwargs["InstanceInitiatedShutdownBehavior"] == "terminate"
    assert "process_worker_pool.py" in kwargs["UserData"]
    assert "pip install" in kwargs["UserData"]


@pytest.mark.local
def test_create_instances_options(make_provider):
    p = make_provider(key_name="k", iam_instance_profile_arn="arn:aws:iam::1:instance-profile/x",
                      spot_max_bid=0.25)
    p.submit("process_worker_pool.py", 1)

    kwargs = make_provider.ec2.create_instances.call_args.kwargs
    assert kwargs["KeyName"] == "k"
    assert kwargs["IamInstanceProfile"] == {"Arn": "arn:aws:iam::1:instance-profile/x"}
    assert kwargs["InstanceMarketOptions"]["MarketType"] == "spot"
    assert kwargs["InstanceMarketOptions"]["SpotOptions"]["MaxPrice"] == "0.25"


@pytest.mark.local
def test_submit_raises_on_ec2_error(make_provider):
    from botocore.exceptions import ClientError

    make_provider.ec2.create_instances.side_effect = ClientError(
        {"Error": {"Code": "InvalidAMIID.NotFound", "Message": "no such AMI"}}, "RunInstances")
    with pytest.raises(SubmitException, match="no such AMI"):
        make_provider().submit("process_worker_pool.py", 1)


@pytest.mark.local
def test_submit_raises_on_other_error(make_provider):
    make_provider.client.describe_subnets.side_effect = RuntimeError("boom")
    with pytest.raises(SubmitException, match="boom"):
        make_provider().submit("process_worker_pool.py", 1)


@pytest.mark.local
def test_submit_raises_without_subnet(make_provider):
    make_provider.client.describe_instance_type_offerings.return_value = {"InstanceTypeOfferings": []}
    with pytest.raises(SubmitException, match="No subnet"):
        make_provider().submit("process_worker_pool.py", 1)
    make_provider.ec2.create_instances.assert_not_called()


@pytest.mark.local
def test_submit_instance_limit(make_provider):
    p = make_provider(max_blocks=2)
    first = p.submit("process_worker_pool.py", 1)
    p.submit("process_worker_pool.py", 1)

    with pytest.raises(SubmitException, match="instance limit"):
        p.submit("process_worker_pool.py", 1)
    assert make_provider.ec2.create_instances.call_count == 2

    # instances which have finished do not count towards the limit
    p.resources[first]["status"] = JobStatus(JobState.COMPLETED)
    assert p.submit("process_worker_pool.py", 1) == "i-2"
