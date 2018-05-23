config = {
    "sites": [
        {
            "site": "local_ipp",
            "auth": {
                "channel": None
            },
            "execution": {
                "executor": "ipp",
                "provider": "local",
                "block": {
                    "initBlocks": 4,
                }
            }
        }
    ],
    "globals": {
        "lazyErrors": True,
    }
}
