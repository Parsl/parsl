import os
import platform
hostname = platform.uname().node


def get_site_config(hostname):
    print(f"Loading config for {hostname}")

    if 'thetalogin' in hostname:
        from parsl.tests.configs.theta import config
    elif 'frontera' in hostname:
        from parsl.tests.configs.frontera import config
    else:
        raise Exception("No matching site")

    return config


preferred_hostname = os.getenv('PARSL_HOSTNAME', hostname)
config = get_site_config(preferred_hostname)
