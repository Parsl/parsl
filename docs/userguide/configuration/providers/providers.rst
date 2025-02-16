Execution Providers
===================

Parsl presents a uniform interface to managing compute resources
for local systems, compute clusters, or cloud providers.
Each provider exposes three core actions: submit a
job for execution (e.g., sbatch for the Slurm resource manager),
retrieve the status of an allocation (e.g., squeue),
and cancel a running job (e.g., scancel).

Start by selecting a scheduler from following classes,
then configuring the details required.

.. contents::
   :local:
   :depth: 1

Local Provider
--------------

The :class:`~parsl.providers.LocalProvider` starts workers on the same
computer running the Parsl main script.
It is primarily used when testing Parsl programs
before deploying them to distributed resources.

Cluster Schedulers
------------------

Schedulers are used at facilities where a single compute clusters shared by many teams.
Users request individual "Jobs" that will be ran once resources are available.
Parsl will create and manage its own Jobs.

Configuring a cluster scheduler requires:

1. Account information (e.g., where to charge hours)
2. The size of each Job (e.g., node count, duration)

Available options include:

1. :class:`~parsl.providers.SlurmProvider`
2. :class:`~parsl.providers.CondorProvider`
3. :class:`~parsl.providers.GridEngineProvider`
4. :class:`~parsl.providers.TorqueProvider`
5. :class:`~parsl.providers.PBSProvider`
6. :class:`~parsl.providers.LSFProvider`


Cloud Providers
---------------

Cloud compute vendors supply compute nodes as virtual machines on request.
Users request a certain amount of virtual machines of a certain types and
will, generally, maintain access to them as long as they pay.
Parsl will manage provisioning and releasing reservations of virtual machines.

Configuring a cloud provider often requires:

1. Credentials to authenticate with vendor APIs
2. Specifying the type of compute node (e.g., availability region, OS)
3. Billing information (e.g., project information, spot bid proces).

Parsl currently supports:

1. :class:`~parsl.providers.AWSProvider`
2. :class:`~parsl.providers.GoogleCloudProvider`

Kubernetes
----------

**Provider**: :class:`~parsl.providers.KubernetesProvider`

Kubernetes manages many small services running together on a single set of compute nodes.
Users define services then request Kubernetes to start or stop copies as appropriate.
Kubernetes creates each service inside an isolated "container,"
decides where each copy runs, and manages connecting them to networks or storage.
Parsl can use Kubernetes to launch workers as services.

Configure :class:`~parsl.providers.KubernetesProvider` by providing:

1. Authentication details (e.g., user name, secrets)
2. The type of each container (e.g., base image, RAM, CPU)
3. Disk availability (e.g., which volumes containers can access)
