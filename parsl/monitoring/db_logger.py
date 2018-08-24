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


def get_db_logger(host='search-parsl-logging-test-2yjkk2wuoxukk2wdpiicl7mcrm.us-east-1.es.amazonaws.com',
                  port=443,
                  enable_es_logging=False,
                  enable_ssl=True,
                  index_name="my_python_index",
                  logger_name='parsl_db_logger',
                  eng_link='sqlite:///parsl.db',
                  version='1.0.0',
                  enable_local_db_logging=False,
                  is_logging_server=False,
                  web_app_host='http://localhost',
                  web_app_port=8899,
                  **kwargs):
    """
    Parameters
    ----------
    host : str, optional
        URL to the elasticsearch cluster. Skip the http(s)://
    port : int, optional
        Port to use to access the elasticsearch cluster
    enable_es_logging : Bool, optional
        Set to True to enable logging to elasticsearch
    enable_ssl : Bool, optional
        Set to False if ssl is not supported by the elasticsearch server
    index_name : str, optional
        Index name to use for elasticsearch
    logger_name : str, optional
        Name of the logger to use. Prevents adding repeat handlers or incorrect handlers
    eng_link : str, optional
        The location of the SQL database to use for local logging which SQLalchemy recognizes as valid address
    version : str, optional
        Used to distinguish between different versions of a workflow in logs
    enable_local_db_logging : Bool, optional
        Enable to use local db logging/SQL logging
    is_logging_server : Bool, optional
        Used internally to determine which handler to return when using local db logging
    web_app_host : str, optional
        url which points to the logging server. Localhost works when using port forwarding or with local task executors.
    web_app_port : int, optional
        Port to use to access the logging server

    Returns
    -------
    logging.logger object

    Raises
    ------
    OptionalModuleMissing

    """
    logger = logging.getLogger(logger_name)
    if enable_es_logging:
        if not _es_logging_enabled:
            raise OptionalModuleMissing(
                ['CMRESHandler'], "Logging to ElasticSearch requires the cmreslogging module")

        handler = CMRESHandler(hosts=[{'host': host,
                                       'port': port}],
                               use_ssl=enable_ssl,
                               auth_type=CMRESHandler.AuthType.NO_AUTH,
                               es_index_name=index_name,
                               es_additional_fields={
                                   'Campaign': "test",
                                   'Version': version,
                                   'Username': getpass.getuser()})
        logger = logging.getLogger(logger_name)
        logger.setLevel(logging.INFO)
        logger.addHandler(handler)
    elif enable_local_db_logging and not is_logging_server:
        # add a handler that will pass logs to the logging server
        handler = RemoteHandler(web_app_host, web_app_port)
        logger = logging.getLogger(logger_name)
        logger.setLevel(logging.INFO)
        logger.addHandler(handler)
    elif enable_local_db_logging and is_logging_server:
        # add a handler that will take logs being recieved on the server and log them to the database
        handler = DatabaseHandler(eng_link)
        logger = logging.getLogger(logger_name)
        logger.setLevel(logging.INFO)
        logger.addHandler(handler)
    else:
        logger.addHandler(NullHandler())

    return logger
