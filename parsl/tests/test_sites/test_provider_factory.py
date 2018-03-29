import parsl

parsl.set_stream_logger()

from parsl.execution_provider.provider_factory import ExecProviderFactory


def test_factory_1():

    config = {
        "sites": [
            {"site": "RCC_Midway_Remote",
             "auth": {
                 "channel": "ssh",
                 "hostname": "swift.rcc.uchicago.edu",
                 "username": "yadunand"
             },
             "execution": {
                 "executor": "ipp",
                 "provider": "slurm",  # LIKELY SHOULD BE BOUND TO SITE
                 "scriptDir": ".scripts",
                 "block": {  # Definition of a block
                     "nodes": 1,            # of nodes in that block
                     "taskBlocks": 1,        # total tasks in a block
                     "walltime": "00:05:00",
                     "Options": {
                         "partition": "debug",
                         "account": "pi-wilde",
                         "overrides": "#SBATCH--constraint=haswell"
                     }
                 }
             }
             }
        ],
        "globals": {
            "lazyErrors": True
        }
    }

    epf = ExecProviderFactory()
    epf.make(config)


def test_factory_2():

    config = {"site": "ipp_local",
              "execution":
              {"executor": "ipp",
               "provider": "local",
               "channel": "None",
               "options":
               {"initParallelism": 2,
                "maxParallelism": 2,
                "minParallelism": 0,
                   "walltime": "00:25:00",
                }
               }}

    epf = ExecProviderFactory()
    epf.make(config)


def test_factory_3():

    config = {
        "sites": [
            {"site": "Local",
             "auth": {
                 "channel": None
             },
             "execution": {
                 "executor": "threads",
                 "provider": None,  # LIKELY SHOULD BE BOUND TO SITE
                 "maxThreads": 4
             }
             }
        ],
        "globals": {
            "lazyErrors": True
        }
    }

    epf = ExecProviderFactory()
    epf.make(config)


if __name__ == '__main__':

    # test_factory_1()
    # test_factory_2()
    test_factory_3()
