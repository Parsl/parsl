"""The following config uses threads, say for local lightweight apps and IPP workers for heavy weight applications.

The @app decorator in the example uses a new parameter sites=[<list of sites>]
to specify the site to which apps should be directed.

"""
threads_ipp = {
    "sites": [
        {"site": "Local_threads",
         "auth": {
             "channel": None,
         },
         "execution": {
             "executor": "threads",
             "provider": None,
             "maxThreads": 4
         }
         },
        {"site": "Local_IPP",
         "auth": {
             "channel": None,
         },
         "execution": {
             "executor": "ipp",
             "provider": "local",
             "block": {                 # Definition of a block
                 "nodes": 1,            # of nodes in that block
                 "taskBlocks": 1,       # total tasks in a block
                 "walltime": "00:05:00",
                 "initBlocks": 4,
             }
         }
         }],

    "globals": {
        "lazyErrors": True
    }
}


"""The following config uses two IPP sites designed for python apps which may
not show any performance improvements on local threads. This also allows you to
send work to two separate remote sites, or to two separate partitions.
"""

multi_ipp = {
    "sites": [
        {"site": "Local_IPP_1",
         "auth": {"channel": None},
         "execution": {
             "executor": "ipp",
             "provider": "local",
             "block": {
                 "nodes": 1,            # of nodes in that block
                 "taskBlocks": 1,       # total tasks in a block
                 "walltime": "00:15:00",
                 "initBlocks": 4,
             }
         }
         },
        {"site": "Local_IPP_2",
         "auth": {"channel": None},
         "execution": {
             "executor": "ipp",
             "provider": "local",
             "block": {
                 "nodes": 1,            # of nodes in that block
                 "taskBlocks": 1,       # total tasks in a block
                 "walltime": "00:15:00",
                 "initBlocks": 2,
             }
         }
         }],
    "globals": {
        "lazyErrors": True
    }
}
