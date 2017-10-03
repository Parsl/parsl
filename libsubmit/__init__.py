'''
Libsubmit
=========

Uniform interface to diverse and multi-lingual set of computational resources.

'''
import logging

logger = logging.getLogger(__name__)

from libsubmit.error import *
from libsubmit.slurm.slurm import Slurm
from libsubmit.aws.aws import EC2Provider
from libsubmit.azure.azureProvider import AzureProvider
from libsubmit.jetstream.jetstream import Jetstream
from libsubmit.midway.midway import Midway
from libsubmit.local.local import Local

