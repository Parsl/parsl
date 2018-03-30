localDockerIPP = {
    "sites": [
        {"site": "Local_IPP",
         "auth": {"channel": None},
         "execution": {
             "executor": "ipp",
             "container": {
                 "type": "docker",
                 "image": "parslbase_v0.1",
             },
             "provider": "local",
             "block": {
                 "initBlocks": 2,  # Start with 4 workers
             },
         }
         }],
    "globals": {"lazyErrors": True}
}

localSimpleIPP = {
    "sites": [
        {"site": "Local_IPP",
         "auth": {"channel": None},
         "execution": {
             "executor": "ipp",
             "provider": "local",
             "block": {
                 "initBlocks": 4,  # Start with 4 workers
             },
         }
         }],
    "globals": {"lazyErrors": True}
}

localDockerMulti = {
    "sites": [
        {"site": "pool_app1",
         "auth": {"channel": None},
         "execution": {
             "executor": "ipp",
             "container": {
                 "type": "docker",
                 "image": "app1_v0.1",
             },
             "provider": "local",
             "block": {
                 "initBlocks": 1,  # Start with 4 workers
             },
         }
         },
        {"site": "pool_app2",
         "auth": {"channel": None},
         "execution": {
             "executor": "ipp",
             "container": {
                 "type": "docker",
                 "image": "app2_v0.1",
             },
             "provider": "local",
             "block": {
                 "initBlocks": 1,  # Start with 4 workers
             },
         }
         }
    ],
    "globals": {"lazyErrors": True}
}
