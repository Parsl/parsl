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
    parsl.data_provider.files.File
    parsl.executors.base.ParslExecutor
    parsl.executors.threads.ThreadPoolExecutor
    parsl.executors.ipp.IPyParallelExecutor
    parsl.executors.ipp_controller.Controller
    parsl.executors.swift_t.TurbineExecutor
    channels.local.local.LocalChannel
    channels.ssh.ssh.SshChannel
    providers.aws.aws.EC2Provider
    providers.azureProvider.azureProvider.AzureProvider
    providers.cobalt.cobalt.Cobalt
    providers.condor.condor.Condor
    providers.googlecloud.googlecloud.GoogleCloud
    providers.gridEngine.gridEngine.GridEngine
    providers.jetstream.jetstream.Jetstream
    providers.local.local.Local
    providers.sge.sge.GridEngine
    providers.slurm.slurm.Slurm
    providers.torque.torque.Torque
    providers.provider_base.ExecutionProvider

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

