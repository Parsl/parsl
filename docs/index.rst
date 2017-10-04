.. libsubmit documentation master file, created by
   sphinx-quickstart on Mon Oct  2 13:39:42 2017.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Welcome to libsubmit's documentation!
=====================================

Libsubmit is responsible for managing execution resources with a Local Resource
Manager (LRM). For instance, campus clusters and supercomputers generally have
schedulers such as Slurm, PBS, Condor and. Clouds on the other hand have API
interfaces that allow much more fine grain composition of an execution environment.
An execution provider abstracts these resources and provides a single uniform
interface to them.


ExecutionProvider
-----------------

.. autoclass:: libsubmit.execution_provider_base.ExecutionProvider
   :members:  __init__, submit, status, cancel, scaling_enabled


Slurm
-----

.. autoclass:: libsubmit.slurm.slurm.Slurm
   :members:  __init__, submit, status, cancel, _status, scaling_enabled, _write_submite_script, current_capacity

.. autofunction:: libsubmit.slurm.slurm.execute_wait


Amazon Web Services
-------------------

.. autoclass:: libsubmit.aws.aws.EC2Provider
    :members:  __init__, submit, status, cancel, read_state_file, show_summary, create_session, create_vpc, spin_up_instance, shut_down_instance, get_instance_state, teardown, scale_in, scale_out

Azure
-----

.. autoclass:: libsubmit.azure.azureProvider.AzureProvider
   :members:  __init__, submit, status, cancel

.. autoclass:: libsubmit.azure.azureDeployer.Deployer
   :members: __init__, deploy, destroy



.. toctree::
   :maxdepth: 2
   :caption: Contents:



Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
