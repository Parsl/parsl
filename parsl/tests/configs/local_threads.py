config = {
    "sites": [
        {
            "site": "local_threads",
            "auth": {
                "channel": None
            },
            "execution": {
                "executor": "threads",
                "provider": None,
                "maxThreads": 6,
            }
        }
    ],
    "globals": {
        "lazyErrors": True
    }
}
