import logging
from parsl.monitoring.handler import DatabaseHandler
from parsl.monitoring.handler import RemoteHandler
from parsl.utils import RepresentationMixin


class NullHandler(logging.Handler):
    """Setup default logging to /dev/null since this is library."""

    def emit(self, record):
        pass


class MonitoringStore(RepresentationMixin):

    def __init__(self,
                 host=None,
                 port=None):
        """
            Parameters
            ----------
            host : str
                 The hostname for running the visualization interface.
            port : int
                The port for the visualization interface
        """

        self.host = host
        self.port = port


class Database(MonitoringStore, RepresentationMixin):

    def __init__(self,
                 connection_string=None):
        """ Initializes a monitoring configuration class.

        Parameters
        ----------
        connection_string : str, optional
            Database connection string that defines how to connect to the database. If not set, DFK init will use a sqlite3
            database inside the rundir.
        """

        self.connection_string = connection_string


class LoggingServer(RepresentationMixin):

    def __init__(self,
                 host='http://localhost',
                 port=8899):
        """
            Parameters
            ----------
            host : str
                 The hostname for running the visualization interface.
            port : int
                The port for the visualization interface
        """

        self.host = host
        self.port = port


class VisualizationServer(RepresentationMixin):

    def __init__(self,
                 host='http://localhost',
                 port=8899):
        """
        Parameters
        ----------
        host : str
             The hostname for running the visualization interface.
        port : int
            The port for the visualization interface
        """

        self.host = host
        self.port = port


class Monitoring(RepresentationMixin):
    """ This is a config class for monitoring. """
    def __init__(self,
                 store=None,
                 logging_server=None,
                 visualization_server=None,
                 resource_loop_sleep_duration=15,
                 workflow_name=None,
                 version='1.0.0'):
        """ Initializes a monitoring configuration class.

        Parameters
        ----------

        resource_loop_sleep_duration : float, optional
            The amount of time in seconds to sleep in between resource monitoring logs per task.
        workflow_name : str, optional
            Name to record as the workflow base name, defaults to the name of the parsl script file if left as None.
        version : str, optional
            Optional workflow identification to distinguish between workflows with the same name, not used internally only for display to user.


        Example
        -------
        .. code-block:: python

            import parsl
            from parsl.config import Config
            from parsl.executors.threads import ThreadPoolExecutor
            from parsl.monitoring.db_logger import MonitoringConfig

            config = Config(
                executors=[ThreadPoolExecutor()],
                monitoring_config=MonitoringConfig(
                    MonitoringStore=DatabaseStore(
                        connection_string='sqlite///monitoring.db'
                    )
                    VisualizationInterface=VisualizationInterface(
                        host='http:localhost'
                        port='9999'
                    )
                )
            )
            parsl.load(config)
        """

        self.store = store
        self.logging_server = logging_server
        # We need a logging server, so create a default server if not specified
        if not logging_server:
            self.logging_server = LoggingServer()

        self.visualization_server = visualization_server
        self.version = version
        self.resource_loop_sleep_duration = resource_loop_sleep_duration
        self.workflow_name = workflow_name

        # for now just set this to none but can be used to present the dashboard location to user
        self.dashboard_link = None


def get_parsl_logger(
                  logger_name='parsl_monitor_logger',
                  is_logging_server=False,
                  monitoring_config=None,
                  **kwargs):
    """
    Parameters
    ----------
    logger_name : str, optional
        Name of the logger to use. Prevents adding repeat handlers or incorrect handlers
    is_logging_server : Bool, optional
        Used internally to determine which handler to return when using local db logging
    monitoring_config : MonitoringConfig, optional
        Pass in a logger class object to use for generating loggers.

    Returns
    -------
    logging.logger object

    Raises
    ------
    OptionalModuleMissing

    """

    logger = logging.getLogger(logger_name)

    if monitoring_config is None:
        logger.addHandler(NullHandler())
        return logger

    if monitoring_config.store is None:
        raise ValueError('No MonitoringStore defined')

    if is_logging_server:
        # add a handler that will take logs being received on the server and log them to the database
        handler = DatabaseHandler(monitoring_config.store.connection_string)
        # use the specific name generated by the server or the monitor wrapper
        logger = logging.getLogger(logger_name)
        logger.setLevel(logging.INFO)
        logger.addHandler(handler)
    else:
        # add a handler that will pass logs to the logging server
        handler = RemoteHandler(monitoring_config.logging_server.host, monitoring_config.logging_server.port)
        # use the specific name generated by the server or the monitor wrapper
        logger = logging.getLogger(logger_name)
        logger.setLevel(logging.INFO)
        logger.addHandler(handler)

    return logger
