from parsl import *
import parsl
import libsubmit

print(parsl.__version__)
print(libsubmit.__version__)

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
                 "initBlocks": 0,
                 "minBlocks": 0,
                 "maxBlocks": 10,
                 "parallelism": 0,
             }
         }
         }]
}

dfk = DataFlowKernel(config=localIPP)


@App("python", dfk)
def python_app():
    import platform
    return "Hello from {0}".format(platform.uname())


def test_python(N=2):
    """No blocks provisioned if parallelism==0

    If I set initBlocks=0 and parallelism=0 I don't think any blocks will be provisioned.
    I tested and the script makes no progress. Perhaps we should catch this case and present an error to users.
    """

    results = {}
    for i in range(0, N):
        results[i] = python_app()

    print("Waiting ....")
    for i in range(0, N):
        print(results[0].result())


if __name__ == '__main__':

    parsl.set_stream_logger()
    test_python()
