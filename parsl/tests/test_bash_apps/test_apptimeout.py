import parsl
from parsl.app.app import App
from parsl.tests.configs.local_threads import config


@App('bash')
def echo_to_file(inputs=[], outputs=[], stderr='std.err', stdout='std.out', walltime=0.5):
    return """echo "sleeping";
    sleep 1 """


def test_walltime():
    """Testing walltime exceeded exception """
    x = echo_to_file()

    try:
        r = x.result()
        print("Got result : ", r)
    except Exception as e:
        print(e.__class__)
        print("Caught exception ", e)


if __name__ == "__main__":
    parsl.clear()
    parsl.load(config)
    test_walltime()
