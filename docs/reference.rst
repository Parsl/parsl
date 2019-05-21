Reference guide
***************

.. autosummary::
    :toctree: stubs
    :nosignatures:

    parsl.set_stream_logger
    parsl.set_file_logger
    parsl.app.app.python_app
    parsl.app.app.bash_app
    parsl.app.futures.DataFuture
    parsl.config.Config
    parsl.dataflow.futures.AppFuture
    parsl.dataflow.dflow.DataFlowKernelLoader
    parsl.data_provider.data_manager.DataManager
    parsl.data_provider.data_manager.Staging
    parsl.data_provider.files.File
    parsl.executors.base.ParslExecutor
    parsl.executors.ThreadPoolExecutor
    parsl.executors.IPyParallelExecutor
    parsl.executors.ipp_controller.Controller
    parsl.executors.HighThroughputExecutor
    parsl.executors.ExtremeScaleExecutor
    parsl.executors.swift_t.TurbineExecutor
    parsl.channels.LocalChannel
    parsl.channels.SSHChannel
    parsl.channels.OAuthSSHChannel
    parsl.channels.SSHInteractiveLoginChannel
    parsl.providers.AWSProvider
    parsl.providers.CobaltProvider
    parsl.providers.CondorProvider
    parsl.providers.GoogleCloudProvider
    parsl.providers.GridEngineProvider
    parsl.providers.JetstreamProvider
    parsl.providers.LocalProvider
    parsl.providers.GridEngineProvider
    parsl.providers.SlurmProvider
    parsl.providers.TorqueProvider
    parsl.providers.KubernetesProvider
    parsl.launchers.SimpleLauncher
    parsl.launchers.SingleNodeLauncher
    parsl.launchers.SrunLauncher
    parsl.launchers.AprunLauncher
    parsl.launchers.SrunMPILauncher
    parsl.launchers.GnuParallelLauncher
    parsl.launchers.MpiExecLauncher
    parsl.monitoring.MonitoringHub

.. autosummary::
    :toctree: stubs
    :nosignatures:

    parsl.app.errors.AppBadFormatting
    parsl.app.errors.AppException
    parsl.app.errors.AppFailure
    parsl.app.errors.AppTimeout
    parsl.app.errors.BadStdStreamFile
    parsl.app.errors.BashAppNoReturn
    parsl.app.errors.DependencyError
    parsl.app.errors.InvalidAppTypeError
    parsl.app.errors.MissingOutputs
    parsl.app.errors.NotFutureError
    parsl.app.errors.ParslError
    parsl.executors.errors.ControllerError
    parsl.executors.errors.ExecutorError
    parsl.executors.errors.ScalingFailed
    parsl.executors.exceptions.ExecutorException
    parsl.executors.exceptions.TaskExecException

