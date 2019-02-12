import parsl
from parsl.app.app import App
from parsl.tests.configs.local_ipp import config


@App("bash")
def bash_app(stdout=None, stderr=None):
    return 'echo "Hello from $(uname -a)" ; sleep 2'


def test_bash():
    """Testing basic scaling|Bash 0 -> 1 block """

    import os
    fname = os.path.basename(__file__)

    x = bash_app(stdout="{0}.out".format(fname))
    print("Waiting ....")
    print(x.result())


if __name__ == "__main__":
    parsl.clear()
    parsl.load(config)
    test_bash()
