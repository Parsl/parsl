# this file contains some tests of passing complicated
# callables as parameters to apps, mainly to check that
# they are serialised correctly with serialization based
# executors.

import importlib
import pathlib
from functools import partial

import parsl


@parsl.python_app
def app(x):
    return True


def test_check_base_app():
    app(0).result()


def somefunc(*args):
    pass


def test_check_this_module_function():
    app(somefunc).result()


def test_check_this_module_function_partial():
    app(partial(somefunc, 1)).result()


def test_check_import_module_function():
    from parsl.tests.callables_helper import some_aux_func
    app(some_aux_func).result()


def test_check_import_module_function_partial():
    from parsl.tests.callables_helper import some_aux_func
    app(partial(some_aux_func, 1)).result()


def test_check_importlib_file_function():
    helper_path = pathlib.Path(__file__).parent / "callables_helper.py"
    spec = importlib.util.spec_from_file_location("dynamically_loaded_module", helper_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    some_aux_func = module.some_aux_func
    app(some_aux_func).result()


def test_check_importlib_file_function_partial():
    helper_path = pathlib.Path(__file__).parent / "callables_helper.py"
    spec = importlib.util.spec_from_file_location("dynamically_loaded_module", helper_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    some_aux_func = module.some_aux_func
    app(partial(some_aux_func, 1)).result()


def test_check_importlib_module_function():
    module = importlib.import_module("parsl.tests.callables_helper")
    some_aux_func = module.some_aux_func
    app(some_aux_func).result()


def test_check_importlib_module_partial():
    module = importlib.import_module("parsl.tests.callables_helper")
    some_aux_func = module.some_aux_func
    app(partial(some_aux_func, 1)).result()
