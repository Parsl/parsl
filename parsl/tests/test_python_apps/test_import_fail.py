import parsl
from parsl.app.app import python_app


@python_app
def platform_name():
    return platform.platform()


def test_name_error(n=2):
    """Catch NameError for missing name
    """

    p = platform_name()

    try:
        p.result()
    except NameError:
        print("Caught NameError")
    else:
        assert False, "Raise the wrong Error"


@python_app
def bad_import():
    import non_existent
    return non_existent.foo()


def test_import_error(n=2):
    """Catch ImportError for missing name
    """

    p = bad_import()

    try:
        p.result()
    except ImportError:
        print("Caught ImportError")
    else:
        assert False, "Raise the wrong Error"
