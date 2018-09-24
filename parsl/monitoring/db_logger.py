import logging
import getpass
from parsl.monitoring.db_local import DatabaseHandler
from parsl.monitoring.db_local import RemoteHandler

try:
    from cmreslogging.handlers import CMRESHandler
except ImportError:
    _es_logging_enabled = False
else:
    _es_logging_enabled = True


class OptionalModuleMissing(Exception):
    ''' Error raised a required module is missing for a optional/extra provider
    '''

    def __init__(self, module_names, reason):
        self.module_names = module_names
        self.reason = reason

    def __repr__(self):
        return "Unable to initialize logger.Missing:{0},  Reason:{1}".format(
            self.module_names, self.reason
        )


class NullHandler(logging.Handler):
    """Setup default logging to /dev/null since this is library."""

    def emit(self, record):
        pass


class MonitoringConfig():
    """ This is a config class for monitoring. """
    def __init__(self,
                 host=None,
                 port=None,
                 enable_ssl=True,
                 database_type='local_database',
                 index_name="my_python_index",
                 logger_name='parsl_db_logger',
                 eng_link=None,
                 version='1.0.0',
                 web_app_host='http://localhost',
                 web_app_port=8899,
                 resource_loop_sleep_duration=15,
                 workflow_name=None):
        """ Initializes a monitoring configuration class.

        Parameters
        ----------
        host : str
            Used with Elasticsearch logging, the location of where to access Elasticsearch. Required when using logging_type = 'elasticsearch'.
        port : int
            Used with Elasticsearch logging, the port of where to access Elasticsearch. Required when using logging_type = 'elasticsearch'.
        enable_ssl : Bool, optional
            Used with Elasticsearch logging, whether to use ssl when connecting to Elasticsearch.
        database_type : str, optional
            Determines whether to use Elasticsearch logging or local database logging, defaults to 'local_database' and accepts 'elasticsearch'.
        index_name : str, optional
            Used with Elasticsearch logging, the name of the index to log to.
        logger_name : str, optional
            Used with both Elasticsearch and local db logging to define naming conventions for loggers.
        eng_link : str, optional
            Used with local database logging, SQLalchemy engine link to define where to connect to the database. If not set, DFK init will use a sqlite3
            database inside the rundir.
        version : str, optional
            Optional workflow identification to distinguish between workflows with the same name, not used internally only for display to user.
        web_app_host : str, optional
            Used with local database logging, how to access the tornado logging server that is spawned by Parsl.
        web_app_port : int, optional
            Used with local database logging, how to access the tornado logging server that is spawned by Parsl.
        resource_loop_sleep_duration : float, optional
            The amount of time in seconds to sleep in between resource monitoring logs per task.
        workflow_name : str, optional
            Name to record as the workflow base name, defaults to the name of the parsl script file if left as None.

        Example
        -------
        .. code-block:: python

            import parsl
            from parsl.config import Config
            from parsl.executors.threads import ThreadPoolExecutor
            from parsl.monitoring.db_logger import MonitoringConfig

            config = Config(
                executors=[ThreadPoolExecutor()],
                lazy_errors=True,
                monitoring_config=MonitoringConfig()
                # an example customization that will log to a specified database instead of the default and operate on a locally specified port.
                # monitoring_config=MonitoringConfig(eng_link='sqlite:///monitoring.db', web_app_host='http://localhost', web_app_port=9999)
            )
            parsl.load(config)
        """
        if database_type not in ['local_database', 'elasticsearch']:
            raise ValueError('Value of logger type was invalid, choices are ' + str(['local_database', 'elasticsearch']))
        self.database_type = database_type
        if database_type == 'elasticsearch':
            if host is None:
                raise ValueError('If using elastic search must specify a host location of the elasticsearch instance.')
            if port is None:
                raise ValueError('If using elastic search must specify a port of the elasticsearch instance.')
            if host.startswith('http'):
                raise ValueError('Do not include "http(s)://" in elasticsearch host string.')
        self.host = host
        self.port = port
        self.enable_ssl = enable_ssl
        self.index_name = index_name
        self.logger_name = logger_name
        self.eng_link = eng_link
        self.version = version
        self.web_app_host = web_app_host
        self.web_app_port = web_app_port
        self.resource_loop_sleep_duration = resource_loop_sleep_duration
        self.workflow_name = workflow_name
        # for now just set this to none but can be used to present the dashboard location to user
        self.dashboard_link = None


def get_db_logger(
                  logger_name='parsl_db_logger',
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

    if monitoring_config.database_type == 'elasticsearch':
        if not _es_logging_enabled:
            raise OptionalModuleMissing(
                ['CMRESHandler'], "Logging to ElasticSearch requires the cmreslogging module")

        handler = CMRESHandler(hosts=[{'host': monitoring_config.host,
                                       'port': monitoring_config.port}],
                               use_ssl=monitoring_config.enable_ssl,
                               auth_type=CMRESHandler.AuthType.NO_AUTH,
                               es_index_name=monitoring_config.index_name,
                               es_additional_fields={
                                   'Campaign': "test",
                                   'Version': monitoring_config.version,
                                   'Username': getpass.getuser()})
        logger = logging.getLogger(monitoring_config.logger_name)
        logger.setLevel(logging.INFO)
        logger.addHandler(handler)
    elif monitoring_config.database_type == 'local_database' and not is_logging_server:
        # add a handler that will pass logs to the logging server
        handler = RemoteHandler(monitoring_config.web_app_host, monitoring_config.web_app_port)
        # use the specific name generated by the server or the monitor wrapper
        logger = logging.getLogger(logger_name)
        logger.setLevel(logging.INFO)
        logger.addHandler(handler)
    elif monitoring_config.database_type == 'local_database' and is_logging_server:
        # add a handler that will take logs being recieved on the server and log them to the database
        handler = DatabaseHandler(monitoring_config.eng_link)
        # use the specific name generated by the server or the monitor wrapper
        logger = logging.getLogger(logger_name)
        logger.setLevel(logging.INFO)
        logger.addHandler(handler)
    else:
        raise ValueError('database_type must be one of ["local_database", "elasticsearch"]')

    return logger
