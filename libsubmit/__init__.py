'''
Libsubmit
=========

Uniform interface to diverse and multi-lingual set of computational resources.

'''
import logging
logger = logging.getLogger(__name__)

from libsubmit.version import VERSION
from libsubmit.providers import LocalProvider

from libsubmit.providers import CobaltProvider
from libsubmit.providers import CondorProvider
from libsubmit.providers import GridEngineProvider
from libsubmit.providers import SlurmProvider
from libsubmit.providers import TorqueProvider

from libsubmit.providers import AWSProvider
from libsubmit.providers import AzureProvider
from libsubmit.providers import GoogleCloudProvider
from libsubmit.providers import JetstreamProvider

from libsubmit.providers import KubernetesProvider

from libsubmit.channels import SSHChannel
from libsubmit.channels import SSHInteractiveLoginChannel
from libsubmit.channels import LocalChannel

from libsubmit.launchers import SimpleLauncher, SingleNodeLauncher, SrunLauncher, \
    AprunLauncher, SrunMPILauncher, AprunLauncher


__author__ = 'Yadu Nand Babuji'
__version__ = VERSION

__all__ = ['LocalProvider',
           'CobaltProvider',
           'CondorProvider',
           'GridEngineProvider',
           'SlurmProvider',
           'TorqueProvider',
           'AWSProvider',
           'AzureProvider',
           'GoogleCloudProvider',
           'JetstreamProvider',
           'KubernetesProvider',
           'LocalChannel',
           'SSHChannel',
           'SSHInteractiveLoginChannel',
           'SimpleLauncher',
           'SingleNodeLauncher',
           'SrunLauncher',
           'AprunLauncher',
           'SrunMPILauncher',
           'AprunLauncher']


def set_stream_logger(name='libsubmit', level=logging.DEBUG, format_string=None):
    '''
    Add a stream log handler

    Args:
         - name (string) : Set the logger name.
         - level (logging.LEVEL) : Set to logging.DEBUG by default.
         - format_string (sting) : Set to None by default.

    Returns:
         - None
    '''

    if format_string is None:
        format_string = "%(asctime)s %(name)s [%(levelname)s]  %(message)s"

    logger = logging.getLogger(name)
    logger.setLevel(level)
    handler = logging.StreamHandler()
    handler.setLevel(level)
    formatter = logging.Formatter(format_string)
    handler.setFormatter(formatter)
    logger.addHandler(handler)


def set_file_logger(filename, name='libsubmit', level=logging.DEBUG, format_string=None):
    ''' Add a stream log handler

    Args:
        - filename (string): Name of the file to write logs to
        - name (string): Logger name
        - level (logging.LEVEL): Set the logging level.
        - format_string (string): Set the format string

    Returns:
       -  None
    '''

    if format_string is None:
        format_string = "%(asctime)s %(name)s [%(levelname)s] %(message)s"

    logger = logging.getLogger(name)
    logger.setLevel(level)
    handler = logging.FileHandler(filename)
    handler.setLevel(level)
    formatter = logging.Formatter(format_string)
    handler.setFormatter(formatter)
    logger.addHandler(handler)


class NullHandler(logging.Handler):
    ''' Setup default logging to /dev/null since this is library.

    '''

    def emit(self, record):
        pass


logging.getLogger('libsubmit').addHandler(NullHandler())
