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

If ``region`` is not set, boto3 resolves it from ``AWS_REGION``, ``AWS_DEFAULT_REGION`` or the profile,
and falls back to ``us-east-2``. ``image_id`` must be an AMI in the resulting region.

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
* a security group. It allows all TCP and UDP traffic from within the VPC, and SSH from the CIDRs
  given in ``ssh_cidr`` (by default, none).

If the state file exists, the recorded infrastructure is reused instead.
Note these properties of the state file:

* ``state_file`` defaults to ``awsproviderstate.json``. A relative path is relative to the current
  working directory. A run from another directory therefore creates a second VPC, so prefer an
  absolute path.
* A state file that exists but cannot be read raises :class:`~parsl.errors.ConfigurationError`.
* If creating the infrastructure fails part way, the provider removes what it had created.

Each resource is tagged with the name of its infrastructure, ``parsl.aws.<timestamp>``.

The infrastructure is kept after Parsl shuts down, so that later runs can reuse it.
To remove it, either:

* call :meth:`~parsl.providers.AWSProvider.teardown` once you no longer need it; or
* pass ``teardown_on_shutdown=True``, which tears it down when the executor shuts down.

Teardown terminates the provider's instances, waits for them to terminate, and deletes the
infrastructure and the state file. It deletes only resources that the provider created and tagged, and
it can be re-run after a partial failure. Anything else in the VPC is left alone, for example an
instance you launched there yourself. Such resources stop the VPC from being deleted. In that case,
teardown logs what it left behind and keeps the state file.

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

* ``ssh_cidr`` must include the workstation's public address.
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
* **Walltime.** ``walltime`` (default ``01:00:00``) is enforced on each instance, counted from boot.
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
  for the ``parsl.aws.*`` ``Name`` tag.

An example configuration is in :ref:`the example configurations <configuring_aws>`.
