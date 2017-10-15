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

ExecutionProviders
------------------

An execution provider is basically an adapter to various types of execution resources. The providers abstract
away the interfaces provided by various systems to request, monitor, and cancel computate resources.

.. autoclass:: libsubmit.execution_provider_base.ExecutionProvider
   :members:  __init__, submit, status, cancel, scaling_enabled, channels_required


Slurm
^^^^^

.. autoclass:: libsubmit.slurm.slurm.Slurm
   :members:  __init__, submit, status, cancel, _status, scaling_enabled, _write_submit_script, current_capacity, channels_required

Cobalt
^^^^^^

.. autoclass:: libsubmit.cobalt.cobalt.Cobalt
   :members:  __init__, submit, status, cancel, _status, scaling_enabled, _write_submit_script, current_capacity, channels_required

Local
^^^^^

.. autoclass:: libsubmit.cobalt.cobalt.Cobalt
   :members:  __init__, submit, status, cancel, scaling_enabled, current_capacity, channels_required



Channels
--------

For certain resources such as campus clusters or supercomputers at research laboratories, resource requirements
may require authentication. For instance some resources may allow access to their job schedulers from only
their login-nodes which require you to authenticate on through SSH, GSI-SSH and sometimes even require
two factor authentication. Channels are simple abstractions that enable the ExecutionProvider component to talk
to the resource managers of compute facilities. The simplest Channel, *LocalChannel* simply executes commands
locally on a shell, while the *SshChannel* authenticates you to remote systems.

.. autoclass:: libsubmit.channels.channel_base.Channel
   :members:  execute_wait, script_dir, execute_no_wait, push_file, close

LocalChannel
^^^^^^^^^^^^
.. autoclass:: libsubmit.channels.local.local.LocalChannel
   :members:  __init__, execute_wait, execute_no_wait, push_file, script_dir, close

SshChannel
^^^^^^^^^^^^
.. autoclass:: libsubmit.channels.ssh.ssh.SshChannel
   :members:  __init__, execute_wait, execute_no_wait, push_file, script_dir, close

SshILChannel
^^^^^^^^^^^^
.. autoclass:: libsubmit.channels.ssh_il.ssh_il.SshILChannel
   :members:  __init__, execute_wait, execute_no_wait, push_file, script_dir, close




.. toctree::
   :maxdepth: 4
   :caption: Contents:



Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
