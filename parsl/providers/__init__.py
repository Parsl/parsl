# Workstation Provider
from parsl.providers.local.local import LocalProvider

# Cluster Providers

from parsl.providers.cobalt.cobalt import CobaltProvider
from parsl.providers.condor.condor import CondorProvider
from parsl.providers.grid_engine.grid_engine import GridEngineProvider
from parsl.providers.slurm.slurm import SlurmProvider
from parsl.providers.torque.torque import TorqueProvider
from parsl.providers.pbspro.pbspro import PBSProProvider

# Cloud Providers
from parsl.providers.aws.aws import AWSProvider
from parsl.providers.googlecloud.googlecloud import GoogleCloudProvider
from parsl.providers.jetstream.jetstream import JetstreamProvider

# Kubernetes
from parsl.providers.kubernetes.kube import KubernetesProvider

__all__ = ['LocalProvider',
           'CobaltProvider',
           'CondorProvider',
           'GridEngineProvider',
           'SlurmProvider',
           'TorqueProvider',
           'PBSProProvider',
           'AWSProvider',
           'GoogleCloudProvider',
           'JetstreamProvider',
           'KubernetesProvider']
