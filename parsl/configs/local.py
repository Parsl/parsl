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
         "auth": {"channel": None},
         "execution": {
             "executor": "ipp",
             "provider": "local",
             "block": {
                 "initBlocks": 4,  # Start with 4 workers
             }
         }
        }],
    "globals": {"lazyErrors": True}
}
