# Workstation Provider
from parsl.providers.local.local import Local

# Cluster Providers

from parsl.providers.cobalt.cobalt import Cobalt
from parsl.providers.condor.condor import Condor
from parsl.providers.grid_engine.grid_engine import GridEngine
from parsl.providers.slurm.slurm import Slurm
from parsl.providers.torque.torque import Torque

# Cloud Providers
from parsl.providers.aws.aws import AWS
from parsl.providers.googlecloud.googlecloud import GoogleCloud
from parsl.providers.jetstream.jetstream import Jetstream

# Kubernetes
from parsl.providers.kubernetes.kube import Kubernetes

__all__ = ['Local',
           'Cobalt',
           'Condor',
           'GridEngine',
           'Slurm',
           'Torque',
           'AWS',
           'GoogleCloud',
           'Jetstream',
           'Kubernetes']
