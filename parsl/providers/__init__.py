# Workstation Provider
from libsubmit.providers.local.local import LocalProvider

# Cluster Providers

from libsubmit.providers.cobalt.cobalt import CobaltProvider
from libsubmit.providers.condor.condor import CondorProvider
from libsubmit.providers.grid_engine.grid_engine import GridEngineProvider
from libsubmit.providers.slurm.slurm import SlurmProvider
from libsubmit.providers.torque.torque import TorqueProvider

# Cloud Providers
from libsubmit.providers.aws.aws import AWSProvider
from libsubmit.providers.googlecloud.googlecloud import GoogleCloudProvider
from libsubmit.providers.azure.azure import AzureProvider
from libsubmit.providers.jetstream.jetstream import JetstreamProvider

# Kubernetes
from libsubmit.providers.kubernetes.kube import KubernetesProvider

__all__ = ['LocalProvider',
           'CobaltProvider',
           'CondorProvider',
           'GridEngineProvider',
           'SlurmProvider',
           'TorqueProvider',
           'AWSProvider',
           'GoogleCloudProvider',
           'AzureProvider',
           'JetstreamProvider',
           'KubernetesProvider']
