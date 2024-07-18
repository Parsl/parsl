

from typing import TYPE_CHECKING

if TYPE_CHECKING:
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

    # Workstation Provider
    from parsl.providers.local.local import LocalProvider
    from parsl.providers.lsf.lsf import LSFProvider
    from parsl.providers.pbspro.pbspro import PBSProProvider
    from parsl.providers.slurm.slurm import SlurmProvider
    from parsl.providers.torque.torque import TorqueProvider


lazys = {
        # Workstation Provider
        'LocalProvider': 'parsl.providers.local.local',

        'CobaltProvider': 'parsl.providers.cobalt.cobalt',
        'CondorProvider': 'parsl.providers.condor.condor',
        'GridEngineProvider': 'parsl.providers.grid_engine.grid_engine',
        'SlurmProvider': 'parsl.providers.slurm.slurm',
        'TorqueProvider': 'parsl.providers.torque.torque',
        'PBSProProvider': 'parsl.providers.pbspro.pbspro',
        'LSFProvider': 'parsl.providers.lsf.lsf',
        'AdHocProvider': 'parsl.providers.ad_hoc.ad_hoc',

        # Cloud Providers
        'AWSProvider': 'parsl.providers.aws.aws',
        'GoogleCloudProvider': 'parsl.providers.googlecloud.googlecloud',
        'AzureProvider': 'parsl.providers.azure.azure',

        # Kubernetes
        'KubernetesProvider': 'parsl.providers.kubernetes.kube'
}

import parsl.providers as px


def lazy_loader(name):
    if name in lazys:
        import importlib
        m = lazys[name]
        # print(f"lazy load {name} from module {m}")
        v = importlib.import_module(m)
        # print(f"imported module: {v}")
        a = v.__getattribute__(name)
        px.__setattr__(name, a)
        return a
    raise AttributeError(f"No (lazy loadable) attribute in {__name__} for {name}")


px.__getattr__ = lazy_loader  # type: ignore[method-assign]
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
