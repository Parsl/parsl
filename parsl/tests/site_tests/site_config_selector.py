import os
import platform
import socket


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

    elif 'nscc' in hostname:
        print("Loading NSCC Singapore config")
        from parsl.tests.configs.nscc_singapore import fresh_config
        config = fresh_config()

    elif 'summit' in socket.getfqdn():
        print("Loading Frontera config")
        from parsl.tests.configs.summit import fresh_config
        config = fresh_config()

    elif 'comet' in hostname:
        print("Loading Comet config")
        from parsl.tests.configs.comet import fresh_config
        config = fresh_config()

    elif 'midway' in hostname:
        print("Loading Midway config")
        from parsl.tests.configs.midway import fresh_config
        config = fresh_config()

    elif 'h2ologin' in hostname:
        print("Loading Blue Waters config")
        from parsl.tests.configs.bluewaters import fresh_config
        config = fresh_config()

    elif 'in2p3' in socket.getfqdn():
        print("Loading CC-IN2P3 config")
        from parsl.tests.configs.cc_in2p3 import fresh_config
        config = fresh_config()

    elif 'bridges' in socket.getfqdn():
        print("Loading bridges config")
        from parsl.tests.configs.bridges import fresh_config
        config = fresh_config()

    elif 'cooley' in hostname:
        print("on Cooley, loading PetrelKube config")
        from parsl.tests.configs.petrelkube import fresh_config
        config = fresh_config()

    else:
        raise RuntimeError("This site cannot be identified")

    return config
