import logging
import time

from parsl.process_loggers import wrap_with_logs
from parsl.executors.taskvine.errors import TaskVineFactoryFailure

from ndcctools.taskvine import Factory

logger = logging.getLogger(__name__)


@wrap_with_logs
def _taskvine_factory(should_stop, factory_config):
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

    # setup factory context and sleep for a second in every loop to
    # avoid wasting CPU
    with factory:
        while not should_stop.value:
            time.sleep(1)

    logger.debug("Exiting TaskVine factory process")
    return 0
