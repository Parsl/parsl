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
                 "initBlocks": 4,  # Start with 4 workers
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
