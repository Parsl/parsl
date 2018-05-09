from parsl.tests.utils import get_rundir

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
                "maxThreads": 4,
            }
        }
    ],
    "globals": {
        "appCache": False,
        "runDir": get_rundir()
    }
}
