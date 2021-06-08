import parsl


@parsl.bash_app
def bash_app(stdout=None, stderr=None):
    return 'echo "Hello from $(uname -a)" ; sleep 2'


def test_bash():
    """Testing basic scaling|Bash 0 -> 1 block """

    import os
    fname = os.path.basename(__file__)

    x = bash_app(stdout=f"{fname}.out")
    print("Waiting ....")
    print(x.result())
