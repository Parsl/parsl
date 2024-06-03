# Workstation Provider
from parsl.providers.ad_hoc.ad_hoc import AdHocProvider

# Cloud Providers
from parsl.providers.aws.aws import AWSProvider
from parsl.providers.azure.azure import AzureProvider
from parsl.providers.cobalt.cobalt import CobaltProvider
from parsl.providers.condor.condor import CondorProvider
from parsl.providers.googlecloud.googlecloud import GoogleCloudProvider
from parsl.providers.grid_engine.grid_engine import GridEngineProvider

# Kubernetes
from parsl.providers.kubernetes.kube import KubernetesProvider
from parsl.providers.local.local import LocalProvider
from parsl.providers.lsf.lsf import LSFProvider
from parsl.providers.pbspro.pbspro import PBSProProvider
from parsl.providers.slurm.slurm import SlurmProvider
from parsl.providers.torque.torque import TorqueProvider

__all__ = ['LocalProvider',
           'CobaltProvider',
           'CondorProvider',
           'GridEngineProvider',
           'SlurmProvider',
           'TorqueProvider',
           'LSFProvider',
           'AdHocProvider',
           'PBSProProvider',
           'AWSProvider',
           'GoogleCloudProvider',
           'KubernetesProvider',
           'AzureProvider']
