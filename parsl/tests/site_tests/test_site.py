import argparse
import time

import parsl
from parsl.app.app import python_app  # , bash_app

@python_app
def platform(sleep=10, stdout=None):
    import platform
    import time
    time.sleep(sleep)
    return platform.uname()


def test_platform(n=2, sleep_dur=10):
    """ This should sleep to make sure that concurrent apps will go to different workers
    """

    dfk = parsl.dfk()
    name = list(dfk.executors.keys())[0]
    print("Trying to get executor : ", name)

    x = platform(sleep=0)
    print(x.result())

    print("Executor : ", dfk.executors[name])
    print("Connected   : ", dfk.executors[name].connected_workers)
    print("Outstanding : ", dfk.executors[name].outstanding)

    d = []
    for i in range(0, n):
        x = platform(sleep=sleep_dur)
        d.append(x)

    pinfo = set([i.result()for i in d])
    assert len(pinfo) == 2, "Expected two nodes, instead got {}".format(pinfo)

    print("Test passed")
    return True


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument("-s", "--sitespec", default=None)
    parser.add_argument("-c", "--count", default="4",
                        help="Count of apps to launch")
    parser.add_argument("-t", "--time", default="60",
                        help="Sleep time for each app")
    
    parser.add_argument("-d", "--debug", action='store_true',
                        help="Count of apps to launch")
    args = parser.parse_args()

    if args.debug:
        parsl.set_stream_logger()

    if args.sitespec:
        config = None
        try:
            exec("import parsl; from {} import config".format(args.sitespec))
            # Here we will set workers per node or manager to 1 to make sure we have 2 managers connecting
            config.executors[0].max_workers = 2
            # Make sure that we are requesting one 2 node block.
            assert config.executors[0].provider.nodes_per_block == 2, "Expecting nodes_per_block = 2, got:{}".format(
                config.executors[0].provider.nodes_per_block)
            parsl.load(config)
        except Exception:
            print("Failed to load the requested config : ", args.sitespec)
            exit(0)
    else:
        from site_config_selector import config
        parsl.load(config)

    x = test_platform(n=int(args.count), sleep_dur=int(args.time))
