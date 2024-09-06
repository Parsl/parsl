API Reference guide
*******************

Core
====

.. autosummary::
    :toctree: stubs
    :nosignatures:

    parsl.app.app.python_app
    parsl.app.app.bash_app
    parsl.app.app.join_app
    parsl.dataflow.futures.AppFuture
    parsl.dataflow.dflow.DataFlowKernelLoader
    parsl.monitoring.MonitoringHub
    parsl.dataflow.dependency_resolvers.DependencyResolver
    parsl.dataflow.dependency_resolvers.DEEP_DEPENDENCY_RESOLVER
    parsl.dataflow.dependency_resolvers.SHALLOW_DEPENDENCY_RESOLVER

Configuration
=============

.. autosummary::
    :toctree: stubs
    :nosignatures:

    parsl.config.Config
    parsl.set_stream_logger
    parsl.set_file_logger
    parsl.addresses.address_by_hostname
    parsl.addresses.address_by_interface
    parsl.addresses.address_by_query
    parsl.addresses.address_by_route
    parsl.utils.get_all_checkpoints
    parsl.utils.get_last_checkpoint

Channels
========

Channels are deprecated in Parsl. See
`issue 3515 <https://github.com/Parsl/parsl/issues/3515>`_
for further discussion.

Data management
===============

.. autosummary::
    :toctree: stubs
    :nosignatures:

    parsl.app.futures.DataFuture
    parsl.data_provider.data_manager.DataManager
    parsl.data_provider.staging.Staging
    parsl.data_provider.files.File
    parsl.data_provider.ftp.FTPSeparateTaskStaging
    parsl.data_provider.ftp.FTPInTaskStaging
    parsl.data_provider.file_noop.NoOpFileStaging
    parsl.data_provider.globus.GlobusStaging
    parsl.data_provider.http.HTTPSeparateTaskStaging
    parsl.data_provider.http.HTTPInTaskStaging
    parsl.data_provider.rsync.RSyncStaging

Executors
=========

.. autosummary::
    :toctree: stubs
    :nosignatures:

    parsl.executors.base.ParslExecutor
    parsl.executors.status_handling.BlockProviderExecutor
    parsl.executors.ThreadPoolExecutor
    parsl.executors.HighThroughputExecutor
    parsl.executors.MPIExecutor
    parsl.executors.WorkQueueExecutor
    parsl.executors.taskvine.TaskVineExecutor
    parsl.executors.FluxExecutor
    parsl.executors.radical.RadicalPilotExecutor

Manager Selectors
=================

.. autosummary::
    :toctree: stubs
    :nosignatures:

    parsl.executors.high_throughput.manager_selector.RandomManagerSelector
    parsl.executors.high_throughput.manager_selector.BlockIdManagerSelector

Launchers
=========

.. autosummary::
    :toctree: stubs
    :nosignatures:

    parsl.launchers.base.Launcher
    parsl.launchers.SimpleLauncher
    parsl.launchers.SingleNodeLauncher
    parsl.launchers.SrunLauncher
    parsl.launchers.AprunLauncher
    parsl.launchers.SrunMPILauncher
    parsl.launchers.GnuParallelLauncher
    parsl.launchers.MpiExecLauncher
    parsl.launchers.MpiRunLauncher
    parsl.launchers.JsrunLauncher
    parsl.launchers.WrappedLauncher

Providers
=========

.. autosummary::
    :toctree: stubs
    :nosignatures:

    parsl.providers.AWSProvider
    parsl.providers.CobaltProvider
    parsl.providers.CondorProvider
    parsl.providers.GoogleCloudProvider
    parsl.providers.GridEngineProvider
    parsl.providers.LocalProvider
    parsl.providers.LSFProvider
    parsl.providers.SlurmProvider
    parsl.providers.TorqueProvider
    parsl.providers.KubernetesProvider
    parsl.providers.PBSProProvider
    parsl.providers.base.ExecutionProvider
    parsl.providers.cluster_provider.ClusterProvider

Batch jobs
==========

.. autosummary::
    :toctree: stubs
    :nosignatures:

    parsl.jobs.states.JobState
    parsl.jobs.states.JobStatus
    parsl.jobs.error_handlers.noop_error_handler
    parsl.jobs.error_handlers.simple_error_handler
    parsl.jobs.error_handlers.windowed_error_handler

Exceptions
==========

.. autosummary::
    :toctree: stubs
    :nosignatures:

    parsl.app.errors.AppBadFormatting
    parsl.app.errors.AppException
    parsl.app.errors.AppTimeout
    parsl.app.errors.BadStdStreamFile
    parsl.app.errors.BashAppNoReturn
    parsl.app.errors.BashExitFailure
    parsl.app.errors.MissingOutputs
    parsl.app.errors.ParslError
    parsl.errors.ConfigurationError
    parsl.errors.OptionalModuleMissing
    parsl.executors.errors.ExecutorError
    parsl.executors.errors.ScalingFailed
    parsl.executors.errors.BadMessage
    parsl.dataflow.errors.DataFlowException
    parsl.dataflow.errors.BadCheckpoint
    parsl.dataflow.errors.DependencyError
    parsl.dataflow.errors.JoinError
    parsl.launchers.errors.BadLauncher
    parsl.providers.errors.ExecutionProviderException
    parsl.providers.errors.ScaleOutFailed
    parsl.providers.errors.SchedulerMissingArgs
    parsl.providers.errors.ScriptPathError
    parsl.channels.errors.ChannelError
    parsl.channels.errors.BadHostKeyException
    parsl.channels.errors.BadScriptPath
    parsl.channels.errors.BadPermsScriptPath
    parsl.channels.errors.FileExists
    parsl.channels.errors.AuthException
    parsl.channels.errors.SSHException
    parsl.channels.errors.FileCopyException
    parsl.executors.high_throughput.errors.WorkerLost
    parsl.executors.high_throughput.interchange.ManagerLost
    parsl.serialize.errors.DeserializationError
    parsl.serialize.errors.SerializationError

Internal
========

.. autosummary::
    :toctree: stubs
    :nosignatures:

    parsl.app.app.AppBase
    parsl.app.bash.BashApp
    parsl.app.python.PythonApp
    parsl.dataflow.dflow.DataFlowKernel
    parsl.dataflow.memoization.id_for_memo
    parsl.dataflow.memoization.Memoizer
    parsl.dataflow.states.FINAL_STATES
    parsl.dataflow.states.States
    parsl.dataflow.taskrecord.TaskRecord
    parsl.jobs.job_status_poller.JobStatusPoller
    parsl.jobs.strategy.Strategy
    parsl.utils.Timer
