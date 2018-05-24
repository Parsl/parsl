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
                    "init_blocks": 4,
                }
            }
        }
    ],
    "globals": {
        "lazyErrors": True,
    }
}
