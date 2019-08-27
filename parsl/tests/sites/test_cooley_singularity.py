from parsl.app.app import App
from parsl.tests.configs.cooley_local_single_node import config


local_config = config


@App("bash")
def freesurfer(stdout=None, stderr=None):
    return """singularity exec ~madduri/freesurfer.simg recon-all
    """


if __name__ == "__main__":

    N = 4
    results = {}
    for i in range(0, N):
        results[i] = freesurfer(stdout="freesurfer.{}.out".format(i),
                                stderr="freesurfer.{}.err".format(i))

    for i in range(0, N):
        results[i].result()

    print("Waiting ....")
    try:
        print(results[0].result())
    except Exception:
        print("Caught an exception, but this is not a problem")
        pass
    print("STDOUT from 0th run :")
    print(open(results[0].stdout, 'r').read())
    print(open(results[0].stderr, 'r').read())
