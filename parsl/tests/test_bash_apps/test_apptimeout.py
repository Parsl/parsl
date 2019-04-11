import parsl
from parsl.app.app import App
from parsl.tests.configs.htex_local import config ## TODO reset


@App('bash')
def echo_to_file(inputs=[], outputs=[], stderr='std.err', stdout='std.out', walltime=0.5):
    return """echo "sleeping";
    sleep 1 """


def test_walltime():
    """Testing walltime exceeded exception """
    x = echo_to_file()

#    try:

    try:
         r = x.result()
    except Exception as e:
         print("TT1 got exception: {}".format(e))
         print("TT1 got exception.__class__: {}".format(e.__class__))
    e2 = x.exception()
    print("TT2: exception is {}".format(e2))
    print("TT3: e2.__class__ is {}".format(e2.__class__))
    # print("Got result : ", r)
#    except Exception as e:
#        print(e.__class__)
#        print("Caught exception ", e)


if __name__ == "__main__":
    parsl.set_stream_logger()
    parsl.clear()
    parsl.load(config)
    test_walltime()
