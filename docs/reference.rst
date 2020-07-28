API Reference guide
*******************

Miscellaneous
=============

.. autosummary::
    :toctree: stubs
    :nosignatures:

    parsl.app.app.python_app
    parsl.app.app.bash_app
    parsl.dataflow.futures.AppFuture
    parsl.dataflow.dflow.DataFlowKernelLoader
    parsl.monitoring.MonitoringHub

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

Channels
========

.. autosummary::
    :toctree: stubs
    :nosignatures:

    parsl.channels.LocalChannel
    parsl.channels.SSHChannel
    parsl.channels.OAuthSSHChannel
    parsl.channels.SSHInteractiveLoginChannel

Data management
===============

.. autosummary::
    :toctree: stubs
    :nosignatures:

    parsl.app.futures.DataFuture
    parsl.data_provider.data_manager.DataManager
    parsl.data_provider.data_manager.Staging
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
    parsl.executors.ThreadPoolExecutor
    parsl.executors.HighThroughputExecutor
    parsl.executors.WorkQueueExecutor
    parsl.executors.ExtremeScaleExecutor
    parsl.executors.swift_t.TurbineExecutor

Launchers
=========

.. autosummary::
    :toctree: stubs
    :nosignatures:

    parsl.launchers.SimpleLauncher
    parsl.launchers.SingleNodeLauncher
    parsl.launchers.SrunLauncher
    parsl.launchers.AprunLauncher
    parsl.launchers.SrunMPILauncher
    parsl.launchers.GnuParallelLauncher
    parsl.launchers.MpiExecLauncher
    parsl.launchers.JsrunLauncher
    parsl.launchers.WrappedLauncher

Providers
=========

.. autosummary::
    :toctree: stubs
    :nosignatures:

    parsl.providers.AdHocProvider
    parsl.providers.AWSProvider
    parsl.providers.CobaltProvider
    parsl.providers.CondorProvider
    parsl.providers.GoogleCloudProvider
    parsl.providers.GridEngineProvider
    parsl.providers.JetstreamProvider
    parsl.providers.LocalProvider
    parsl.providers.LSFProvider
    parsl.providers.GridEngineProvider
    parsl.providers.SlurmProvider
    parsl.providers.TorqueProvider
    parsl.providers.KubernetesProvider
    parsl.providers.PBSProProvider

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
    parsl.app.errors.NotFutureError
    parsl.app.errors.ParslError
    parsl.executors.errors.ControllerError
    parsl.executors.errors.ExecutorError
    parsl.executors.errors.ScalingFailed
    parsl.executors.errors.InsufficientMPIRanks
    parsl.executors.errors.DeserializationError
    parsl.executors.errors.BadMessage
    parsl.dataflow.error.DataFlowException
    parsl.dataflow.error.ConfigurationError
    parsl.dataflow.error.DuplicateTaskError
    parsl.dataflow.error.BadCheckpoint
    parsl.dataflow.error.DependencyError
    parsl.launchers.error.BadLauncher
    parsl.providers.error.ExecutionProviderException
    parsl.providers.error.OptionalModuleMissing
    parsl.providers.error.ChannelRequired
    parsl.providers.error.ScaleOutFailed
    parsl.providers.error.SchedulerMissingArgs
    parsl.providers.error.ScriptPathError
    parsl.channels.errors.ChannelError
    parsl.channels.errors.BadHostKeyException
    parsl.channels.errors.BadScriptPath
    parsl.channels.errors.BadPermsScriptPath
    parsl.channels.errors.FileExists
    parsl.channels.errors.AuthException
    parsl.channels.errors.SSHException
    parsl.channels.errors.FileCopyException
    parsl.executors.high_throughput.errors.WorkerLost
