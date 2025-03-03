import logging
import os

from parsl.executors.taskvine.errors import TaskVineFactoryFailure
from parsl.process_loggers import wrap_with_logs

# This try except clause prevents import errors
# when TaskVine is not used in Parsl.
try:
    from ndcctools.taskvine import Factory
    taskvine_available = True
except ImportError:
    taskvine_available = False

logger = logging.getLogger(__name__)


@wrap_with_logs
def _taskvine_factory(should_stop, factory_config):
    if not taskvine_available:
        logger.debug("TaskVine package cannot be found. Please install the ndcctools package.")
        return
    logger.debug("Starting TaskVine factory process")

    try:
        # create the factory according to the project name if given
        if factory_config._project_name:
            factory = Factory(batch_type=factory_config.batch_type,
                              manager_name=factory_config._project_name,
                              )
        else:
            factory = Factory(batch_type=factory_config.batch_type,
                              manager_host_port=f"{factory_config._project_address}:{factory_config._project_port}",
                              )
    except Exception as e:
        raise TaskVineFactoryFailure(f'Cannot create factory with exception {e}')

    # Set attributes of this factory
    if factory_config._project_password_file:
        factory.password = factory_config._project_password_file
    factory.factory_timeout = factory_config.factory_timeout
    factory.scratch_dir = factory_config.scratch_dir
    factory.min_workers = factory_config.min_workers
    factory.max_workers = factory_config.max_workers
    factory.workers_per_cycle = factory_config.workers_per_cycle

    # create scratch dir if factory process gets ahead of the manager.
    os.makedirs(factory.scratch_dir, exist_ok=True)

    if factory_config.worker_options:
        factory.extra_options = factory_config.worker_options
    factory.timeout = factory_config.worker_timeout
    if factory_config.cores:
        factory.cores = factory_config.cores
    if factory_config.gpus:
        factory.gpus = factory_config.gpus
    if factory_config.memory:
        factory.memory = factory_config.memory
    if factory_config.disk:
        factory.disk = factory_config.disk
    if factory_config.python_env:
        factory.python_env = factory_config.python_env

    if factory_config.condor_requirements:
        factory.condor_requirements = factory_config.condor_requirements
    if factory_config.batch_options:
        factory.batch_options = factory_config.batch_options

    # run factory through Python context and wait for signal to stop.
    with factory:
        should_stop.wait()

    logger.debug("Exiting TaskVine factory process")
