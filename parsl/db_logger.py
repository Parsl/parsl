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
                  index_name="parsl.campaign"):
    """
    Parameters
    ----------
    host : str, optional
        URL to the elasticsearch cluster. Skip the http(s)://
    port : int, optional
        Port to use to access the elasticsearch cluster
    enable_es_logging : Bool, optional
        Set to True to enable logging to elasticsearch
    index_name : str, optional
        Index name to use for elasticsearch

    Returns
    -------
    logging.logger object

    Raises
    ------
    OptionalModuleMissing

    """
    logger = logging.getLogger(__file__)
    if enable_es_logging is True:
        if not _es_logging_enabled:
            raise OptionalModuleMissing(
                ['cmreslogging'], "Logging to ElasticSearch requires the cmreslogging module")

        handler = CMRESHandler(hosts=[{'host': host,
                                       'port': port}],
                               use_ssl=True,
                               auth_type=CMRESHandler.AuthType.NO_AUTH,
                               es_index_name="my_python_index",
                               es_additional_fields={'Campaign': "test"})
        logger = logging.getLogger("ParslElasticsearch")
        logger.setLevel(level)
        logger.addHandler(handler)
    else:
        logger.addHandler(NullHandler())

    return logger
