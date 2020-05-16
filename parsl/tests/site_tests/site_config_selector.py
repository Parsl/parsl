import os
import platform


def fresh_config():
    hostname = os.getenv('PARSL_HOSTNAME', platform.uname().node)
    print("Loading config for {}".format(hostname))

    if 'thetalogin' in hostname:
        from parsl.tests.configs.theta import fresh_config
        config = fresh_config()
        print("Loading Theta config")

    elif 'frontera' in hostname:
        print("Loading Frontera config")
        from parsl.tests.configs.frontera import fresh_config
        config = fresh_config()

    elif 'cori' in hostname:
        print("Loading Cori config")
        from parsl.tests.configs.cori import fresh_config
        config = fresh_config()

    elif 'stampede2' in hostname:
        print("Loading Stampede2 config")
        from parsl.tests.configs.stampede2 import fresh_config
        config = fresh_config()

    elif 'comet' in hostname:
        print("Loading Comet config")
        from parsl.tests.configs.comet import fresh_config
        config = fresh_config()

    else:
        print("Loading Local HTEX config")
        from parsl.tests.configs.htex_local import fresh_config
        config = fresh_config()
        config.executors[0].max_workers = 1
        config.executors[0].provider.init_blocks = 1
        config.executors[0].provider.min_blocks = 0
        config.executors[0].provider.max_blocks = 1
        # We should skip this.

    return config


config = fresh_config()
