localThreads = {
    "sites": [
        {"site": "Local_Threads",
         "auth": {"channel": None},
         "execution": {
             "executor": "threads",
             "provider": None,
             "maxThreads": 4
         }
         }],
    "globals": {"lazyErrors": True}
}

localIPP = {
    "sites": [
        {"site": "Local_IPP",
         "auth": {
             "channel": None,
         },
         "execution": {
             "executor": "ipp",
             "provider": "local",  # LIKELY SHOULD BE BOUND TO SITE
             "block": {  # Definition of a block
                 "taskBlocks": 4,       # total tasks in a block
                 "initBlocks": 4,
             }
         }
         }]
}

""" Use the following config with caution.
"""

localIPPReuse = {
    "sites": [
        {"site": "Local_IPP",
         "auth": {
             "channel": None,
         },
         "execution": {
             "executor": "ipp",
             "provider": "local",  # LIKELY SHOULD BE BOUND TO SITE
             "script_dir": ".scripts",
             "block": {  # Definition of a block
                 "nodes": 1,            # of nodes in that block
                 "taskBlocks": 1,       # total tasks in a block
                 "initBlocks": 1,
                 "maxBlocks": 1,
             }
         }
         }
    ],
    "globals": {
        "lazyErrors": True
    },
    "controller": {"reuse": True}
}
