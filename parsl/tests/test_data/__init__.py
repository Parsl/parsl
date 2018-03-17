import os

if str(os.environ.get('PARSL_TESTING', False)).lower() != 'true':
    raise RuntimeError("must first run 'export PARSL_TESTING=True'")


def setup_package():
    import os
    try:
        os.mkdir("data")
    except BaseException:
        pass

    with open("data/test1.txt", 'w') as f:
        f.write("1\n")
    with open("data/test2.txt", 'w') as f:
        f.write("2\n")


def teardown_package():
    pass
