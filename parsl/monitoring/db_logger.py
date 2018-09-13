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


class LoggerConfig():
    """ This is a config class for creating a logger. """
    def __init__(self,
                 host=None,
                 port=None,
                 enable_ssl=True,
                 logger_type='local_database',
                 index_name="my_python_index",
                 logger_name='parsl_db_logger',
                 eng_link=None,
                 version='1.0.0',
                 web_app_host='http://localhost',
                 web_app_port=8899,
                 resource_loop_sleep_duration=15,
                 workflow_name=None):
        """ Initializes a db logger configuration class.

        Parameters
        ----------
        host : str
            Used with Elasticsearch logging, the location of where to access Elasticsearch. Required when using logging_type = 'elasticsearch'.
        port : int
            Used with Elasticsearch logging, the port of where to access Elasticsearch. Required when using logging_type = 'elasticsearch'.
        enable_ssl : Bool, optional
            Used with Elasticsearch logging, whether to use ssl when connecting to Elasticsearch.
        logger_type : str, optional
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
        """
        if logger_type not in ['local_database', 'elasticsearch']:
            raise ValueError('Value of logger type was invalid, choices arei ' + str(['local_database', 'elasticsearch']))
        self.logger_type = logger_type
        if logger_type == 'elasticsearch':
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
                  db_logger_config=None,
                  **kwargs):
    """
    Parameters
    ----------
    logger_name : str, optional
        Name of the logger to use. Prevents adding repeat handlers or incorrect handlers
    is_logging_server : Bool, optional
        Used internally to determine which handler to return when using local db logging
    db_logger_config : LoggerConfig, optional
        Pass in a logger class object to use for generating loggers.

    Returns
    -------
    logging.logger object

    Raises
    ------
    OptionalModuleMissing

    """
    logger = logging.getLogger(logger_name)
    if db_logger_config is None:
        logger.addHandler(NullHandler())
        return logger

    if db_logger_config.logger_type == 'elasticsearch':
        if not _es_logging_enabled:
            raise OptionalModuleMissing(
                ['CMRESHandler'], "Logging to ElasticSearch requires the cmreslogging module")

        handler = CMRESHandler(hosts=[{'host': db_logger_config.host,
                                       'port': db_logger_config.port}],
                               use_ssl=db_logger_config.enable_ssl,
                               auth_type=CMRESHandler.AuthType.NO_AUTH,
                               es_index_name=db_logger_config.index_name,
                               es_additional_fields={
                                   'Campaign': "test",
                                   'Version': db_logger_config.version,
                                   'Username': getpass.getuser()})
        logger = logging.getLogger(db_logger_config.logger_name)
        logger.setLevel(logging.INFO)
        logger.addHandler(handler)
    elif db_logger_config.logger_type == 'local_database' and not is_logging_server:
        # add a handler that will pass logs to the logging server
        handler = RemoteHandler(db_logger_config.web_app_host, db_logger_config.web_app_port)
        # use the specific name generated by the server or the monitor wrapper
        logger = logging.getLogger(logger_name)
        logger.setLevel(logging.INFO)
        logger.addHandler(handler)
    elif db_logger_config.logger_type == 'local_database' and is_logging_server:
        # add a handler that will take logs being recieved on the server and log them to the database
        handler = DatabaseHandler(db_logger_config.eng_link)
        # use the specific name generated by the server or the monitor wrapper
        logger = logging.getLogger(logger_name)
        logger.setLevel(logging.INFO)
        logger.addHandler(handler)
    else:
        raise ValueError('logger_type must be one of ["local_database", "elasticsearch"]')

    return logger
