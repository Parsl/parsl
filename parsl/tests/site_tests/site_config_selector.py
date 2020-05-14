import os
import platform
hostname = platform.uname().node


def get_site_config(hostname):
    print(f"Loading config for {hostname}")

    if 'thetalogin' in hostname:
        from parsl.tests.configs.theta import config
        print("Loading Theta config")
    elif 'frontera' in hostname:
        print("Loading Frontera config")
        from parsl.tests.configs.frontera import config
    else:
        print("Loading Local HTEX config")
        from parsl.tests.configs.htex_local import config
        config.executors[0].max_workers = 2
        config.executors[0].provider.init_blocks=2
        config.executors[0].provider.max_blocks=2
        config.executors[0].provider.min_blocks=2
        
    return config


preferred_hostname = os.getenv('PARSL_HOSTNAME', hostname)
config = get_site_config(preferred_hostname)
