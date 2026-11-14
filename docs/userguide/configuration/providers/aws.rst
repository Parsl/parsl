.. _label-aws:

Amazon Web Services (EC2)
=========================

The :class:`~parsl.providers.AWSProvider` runs each block as an Amazon EC2 instance.
On each instance, a user-data script installs Parsl and starts the
:class:`~parsl.executors.HighThroughputExecutor` worker pool.
Install the AWS dependencies with ``python3 -m pip install 'parsl[aws]'``.

.. contents::
   :local:
   :depth: 1


Credentials and region
----------------------

The provider makes one boto3 session and uses it for everything it does.
It takes credentials from the first of these that is set:

1. ``key_file``: a JSON file containing ``AWSAccessKeyId`` and ``AWSSecretKey``, and optionally
   ``AWSSessionToken``. The boto3 names ``aws_access_key_id``, ``aws_secret_access_key`` and
   ``aws_session_token`` are also accepted.
2. ``profile``: a named profile from ``~/.aws/config``.
3. boto3's `default credential chain
   <https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html>`_. This includes
   ``AWS_PROFILE`` and the other ``AWS_*`` environment variables, SSO profiles, and container and EC2
   instance roles.

A :class:`~parsl.errors.ConfigurationError` is raised if no credentials can be found, or if ``profile``
names a profile that does not exist.

If ``region`` is not set, boto3 resolves it from the ``AWS_DEFAULT_REGION`` environment variable, and
then from the ``region`` of the selected profile in ``~/.aws/config``. boto3 does not read
``AWS_REGION``. If no region can be resolved, the provider uses ``us-east-2`` and logs this at INFO
level. After construction, the provider's ``region`` attribute holds the region actually used.
``image_id`` must be an AMI in that region, with an architecture matching ``instance_type``. The
default ``instance_type`` is ``t3.small``, which is x86_64; an arm64 AMI needs a Graviton type, such as
``t4g.small``.

The credentials need permission to create, describe and delete VPCs, subnets, internet gateways,
route tables, security groups and tags, and to run, describe and terminate instances. Attaching
``iam_instance_profile_arn`` to the workers also requires ``iam:PassRole``. The workers themselves
receive no AWS credentials unless you attach an instance profile with ``iam_instance_profile_arn``.


Infrastructure and the state file
---------------------------------

When the provider is constructed and its state file does not exist, it creates the following,
and records their IDs in the state file:

* a VPC with CIDR ``10.0.0.0/16``;
* an internet gateway and a route table;
* one public subnet in each available availability zone;
* a security group. It allows all TCP, UDP and ICMP traffic from within the VPC. From outside the
  VPC, it allows only ICMP "fragmentation needed" (type 3, code 4), which path MTU discovery needs
  (the VPC MTU is 9001, but traffic through the internet gateway is limited to 1500). That rule
  admits no ping. If ``ssh_cidr`` is set, it also allows SSH (TCP port 22) and ICMP from those IPv4
  ranges. ``ssh_cidr`` takes a CIDR string or a list of them. A bare address is treated as ``/32``,
  and IPv6 networks or CIDRs with host bits set raise :class:`~parsl.errors.ConfigurationError`.

If the state file exists, the recorded infrastructure is reused instead. A reused security group is
not modified, so ``ssh_cidr`` does not apply to it; a warning names the group. Change a reused group's
rules with the AWS console or CLI, or remove the infrastructure and let the provider recreate it.
Note these properties of the state file:

* ``state_file`` defaults to ``awsproviderstate.json``. A relative path is relative to the current
  working directory. A run from another directory therefore creates a second VPC, so prefer an
  absolute path.
* A state file that exists but cannot be read raises :class:`~parsl.errors.ConfigurationError`.
* If creating the infrastructure fails part way, the provider removes what it had created.
* The provider manages the file. Do not write or edit it by hand.

Every resource the provider creates is tagged at creation with ``parsl:provider=AWSProvider`` and
``parsl:infrastructure=<infrastructure ID>``. That covers the VPC, subnets, security group, internet
gateway, route table and instances. The infrastructure ID has the form ``parsl.aws.<timestamp>``, and
it is recorded in the state file as ``infrastructureID``.

The infrastructure is kept after Parsl shuts down, so that later runs can reuse it.
To remove it, either:

* call :meth:`~parsl.providers.AWSProvider.teardown` once you no longer need it; or
* pass ``teardown_on_shutdown=True``, which tears it down when the executor shuts down.

Teardown deletes only resources whose ``parsl:infrastructure`` tag matches the state file:

1. It terminates the instances carrying this infrastructure ID, and any launched by this provider
   object, and waits for them to terminate.
2. It deletes the security group, the internet gateway, the subnets, the route table and the VPC.
3. It removes the state file.

:meth:`~parsl.providers.AWSProvider.teardown` returns ``True`` if everything was removed. It returns
``False`` if anything was left in place; the state file is then kept, and a warning lists what was left
and why. Teardown leaves these in place:

* instances in the VPC that the provider did not launch, for example a submit host. They are left
  running, and they prevent the VPC from being deleted;
* resources that are untagged or carry another infrastructure ID;
* everything recorded in a state file without an ``infrastructureID``, for example one written by an
  older version of Parsl, which did not tag resources. Instances that this provider object launched
  are still terminated.

Resources that no longer exist are skipped, so teardown can be re-run once the cause is removed.

**Teardown at shutdown.** During :meth:`DataFlowKernel.cleanup <parsl.dataflow.dflow.DataFlowKernel.cleanup>`,
Parsl first cancels the remaining blocks. It then shuts down each executor. At the end of its
shutdown, the executor calls the provider's :meth:`~parsl.providers.base.ExecutionProvider.shutdown`
hook. In ``AWSProvider``, that hook behaves as follows:

* With ``teardown_on_shutdown=False`` (the default), it keeps the infrastructure. It logs the VPC ID,
  the state file path, and how to remove the infrastructure.
* With ``teardown_on_shutdown=True``, it calls ``teardown()``. If something is left in place, a warning
  says so.
* With ``linger=True``, it skips teardown, with a warning, because teardown would terminate the
  lingering instances.

Errors in the hook are logged and do not stop the rest of the shutdown. If the executor never
started, the hook is not called, even though the constructor has already created the infrastructure.
In that case, call ``teardown()`` yourself.

AWS accounts allow five VPCs per region by default, so remove infrastructure you no longer use.


Connecting workers to Parsl
---------------------------

The workers connect to the interchange on the submitting host, on the HighThroughputExecutor
worker port (see :ref:`label-networking`). The EC2 instances must therefore be able to reach the
submitting host at ``address``, on ``worker_port``.

The CurveZMQ certificates used by ``encrypted=True`` are created on the submitting host, and the provider
does not copy them to the instances. Set ``encrypted=False``. Messages on the worker port are then
neither encrypted nor authenticated, and the submitting side unpickles them. Make sure that only your
workers can reach that port, and never open it to the internet.

Two arrangements work well.

**A submitting host inside the provider's VPC.** Launch an EC2 instance into one of the provider's
subnets, using the provider's security group (the IDs are in the state file), and run Parsl on it.
Set ``address`` to its private IP. The security group already allows the workers to reach it on any
TCP port:

.. code-block:: python

    from parsl.addresses import address_by_route

    HighThroughputExecutor(
        address=address_by_route(),
        worker_port=54000,
        encrypted=False,
        provider=AWSProvider(...),
    )

A submitting host in another VPC is not reachable over private addresses unless you add VPC peering
and routes yourself. Over public addresses, the traffic comes from the instances' public IPs, which are
assigned at launch, so a security group rule cannot be limited to them.

**A workstation behind NAT.** Bind the interchange to the loopback interface. Then, for each instance,
open an SSH reverse tunnel, so that the workers reach the interchange through their own loopback
interface:

.. code-block:: python

    HighThroughputExecutor(
        address='127.0.0.1',
        worker_port=54000,
        encrypted=False,
        provider=AWSProvider(..., key_name='my-key', ssh_cidr='203.0.113.7/32'),
    )

.. code-block:: bash

    ssh -N -i my-key.pem -o ControlMaster=no -o ControlPath=none \
        -o ExitOnForwardFailure=yes -R 54000:127.0.0.1:54000 ubuntu@<instance public IP>

When setting this up:

* ``ssh_cidr`` must include the workstation's public IPv4 address. It applies only when the security
  group is created, so if the address changes later, update the group's rule by hand.
* Each new instance needs its own tunnel. A script can poll the provider's ``resources`` attribute and
  ``describe_instances`` for the public IPs.
* Keep ``ControlMaster=no``. A ``ControlMaster`` setting in ``~/.ssh/config`` would hand the
  forwarding to another connection, and the tunnel would not work.
* The worker exits if it cannot reach the interchange within ``address_probe_timeout``
  (30 seconds by default), so open the tunnels promptly or raise the timeout.

In both cases, the submitting side starts the interchange as ``interchange.py`` from ``PATH``.
Activate the Python environment that Parsl is installed in, or put its ``bin`` directory on ``PATH``.


Worker environment and version matching
---------------------------------------

The interchange rejects workers whose Python ``major.minor`` version or Parsl version
(:data:`parsl.version.VERSION`) differ from the submitting side's. It reports a
:class:`~parsl.executors.high_throughput.errors.VersionMismatch`.

The user-data script runs as root, once, on first boot. In order, it does the following:

1. Unless ``linger=True``, it arranges for the instance to shut down. Because instances are launched to
   terminate on shutdown, this terminates the instance when the script exits, and in any case after
   ``walltime``.
2. It installs Python with ``apt-get`` (Debian, Ubuntu) or ``dnf`` (Amazon Linux 2023). It prefers
   ``python3.X`` for the submitting side's ``3.X``, if the package manager offers it, and otherwise
   uses the image's default ``python3``.
3. It creates a virtual environment at ``/opt/parsl-venv``, installs ``parsl_requirement`` into it, and
   puts its ``bin`` directory first on ``PATH``.
4. It runs ``worker_init``. ``pip install`` here installs into the same environment.
5. It starts the worker pool through the launcher.

Choose an AMI whose Python matches the submitting side. For example, Ubuntu 24.04 provides only
Python 3.12, and Amazon Linux 2023 provides 3.9, 3.11 and 3.12.

``parsl_requirement`` defaults to ``parsl==<the submitting side's version>``. On a development version
of Parsl, which is not on PyPI, it defaults to the GitHub master branch. Set it to install any other
pip requirement, such as a fork or a wheel URL. The result must report the same Parsl version as the
submitting side.

The output of the script, including ``worker_init`` and the worker's own output, is in
``/var/log/cloud-init-output.log`` on the instance. With a ``key_name`` and a matching ``ssh_cidr``, you
can read it over SSH. You can also read it with ``aws ec2 get-console-output``.


Instance lifetime and cost
--------------------------

* **Instance shutdown.** Instances shut down, and so terminate, when their worker pool exits, for
  example after losing contact with the interchange. They also stop when Parsl cancels their block, for
  example after ``max_idletime`` or at the end of the workflow.
* **Walltime.** ``walltime`` (default ``01:00:00``) is enforced on each instance, counted from boot,
  and rounded up to whole minutes.
  To let workers finish their tasks first, set the HighThroughputExecutor ``drain_period`` somewhat
  below it.
* **Linger.** With ``linger=True``, instances are not shut down when their worker exits or at walltime,
  and Parsl's cancel requests are ignored. You are responsible for terminating them.
* **Spot.** ``spot_max_bid`` requests one-time Spot Instances at that maximum hourly price. An
  interrupted instance is terminated, and its tasks fail with ``ManagerLost``. Use ``retries`` in
  :class:`~parsl.config.Config` to rerun them.
* **Instances per block.** ``nodes_per_block`` should stay at 1. The provider always starts one
  instance per block.
* **Leftover resources.** The infrastructure costs nothing while it is idle, but it counts against
  account quotas. Instances are billed until they terminate. To check for anything left over, search
  for the ``parsl:provider=AWSProvider`` tag. Resources created by older versions of Parsl carry only
  a ``Name`` tag of the form ``parsl.aws.*``, and their internet gateway and route table are untagged.

An example configuration is in :ref:`the example configurations <configuring_aws>`.
