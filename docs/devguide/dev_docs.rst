Developer documentation
***********************

.. automodule:: libsubmit
   :no-undoc-members:

.. autofunction:: set_stream_logger

.. autofunction:: set_file_logger

ExecutionProviders
------------------

An execution provider is basically an adapter to various types of execution resources. The providers abstract
away the interfaces provided by various systems to request, monitor, and cancel computate resources.

.. autoclass:: libsubmit.execution_provider_base.ExecutionProvider
   :members:  __init__, submit, status, cancel, scaling_enabled, channels_required


Slurm
^^^^^

.. autoclass:: libsubmit.providers.slurm.slurm.Slurm
   :members:  __init__, submit, status, cancel, _status, scaling_enabled, _write_submit_script, current_capacity, channels_required

Cobalt
^^^^^^

.. autoclass:: libsubmit.providers.cobalt.cobalt.Cobalt
   :members:  __init__, submit, status, cancel, _status, scaling_enabled, _write_submit_script, current_capacity, channels_required

Condor
^^^^^^

.. autoclass:: libsubmit.providers.condor.condor.Condor
   :members:  __init__, submit, status, cancel, _status, scaling_enabled, _write_submit_script, current_capacity, channels_required

Torque
^^^^^^

.. autoclass:: libsubmit.providers.torque.torque.Torque
   :members:  __init__, submit, status, cancel, _status, scaling_enabled, _write_submit_script, current_capacity, channels_required

Local
^^^^^

.. autoclass:: libsubmit.providers.local.local.Local
   :members:  __init__, submit, status, cancel, scaling_enabled, current_capacity, channels_required

AWS
^^^

.. autoclass:: libsubmit.providers.aws.aws.EC2Provider
   :members:  __init__, submit, status, cancel, scaling_enabled, current_capacity, channels_required, create_vpc, read_state_file, write_state_file, create_session, security_group



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
   :members:  __init__, execute_wait, execute_no_wait, push_file, pull_file, script_dir, close

SshILChannel
^^^^^^^^^^^^
.. autoclass:: libsubmit.channels.ssh_il.ssh_il.SshILChannel
   :members:  __init__, execute_wait, execute_no_wait, push_file, pull_file, script_dir, close



Launchers
---------

Launchers are basically wrappers for user submitted scripts as they are submitted to
a specific execution resource.

.. autofunction:: libsubmit.launchers.singleNodeLauncher

