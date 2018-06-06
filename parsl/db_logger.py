import logging

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
        return "Unable to Initialize logger.Missing:{0},  Reason:{1}".format(
            self.module_names, self.reason
        )


class NullHandler(logging.Handler):
    """Setup default logging to /dev/null since this is library."""

    def emit(self, record):
        pass


def get_db_logger(host='search-parsl-logging-test-2yjkk2wuoxukk2wdpiicl7mcrm.us-east-1.es.amazonaws.com',
                  port=443,
                  level=logging.CRITICAL,
                  index_name="parsl.campaign"):
    """
    Parameters
    ----------
    host : str, optional
        URL to the elasticsearch cluster. Skip the http(s)://
    port : int, optional
        Port to use to access the elasticsearch cluster
    level : logging.LEVEL, optional
        This module will log *only* if logging.INFO or lower is provided.
    index_name : str, optional
        Index name to use for elasticsearch

    Returns
    -------
    logging.logger object

    Raises
    ------

    """
    logger = logging.getLogger("ParslElasticsearch")
    if level > logging.INFO:
        logger.addHandler(NullHandler())
    else:
        if not _es_logging_enabled:
            raise OptionalModuleMissing(
                ['cmreslogging'], "Logging to ElasticSearch requires the cmreslogging module")

        handler = CMRESHandler(hosts=[{'host': host,
                                       'port': port}],
                               use_ssl=True,
                               auth_type=CMRESHandler.AuthType.NO_AUTH,
                               es_index_name="my_python_index",
                               es_additional_fields={'Campaign': "test", 'Username': "yadu"})
        logger = logging.getLogger("ParslElasticsearch")
        logger.setLevel(level)
        logger.addHandler(handler)

    return logger
