#!/usr/bin/python3

from parsl.app.errors import RemoteExceptionWrapper
from parsl.data_provider.files import File
import traceback
import sys
import pickle


# This scripts executes a parsl function which is pickled in a file:
#
# exec_parsl_function.py map_file function_file result_file
#
# map_file: Contains a pickled dictionary that indicates which local_paths the
#           parsl Files should take.
#
# function_file: Contains a pickle parsl function.
#
# result_file: It will contain the result of the function, including any
#              exception generated. Exceptions will be wrapped with RemoteExceptionWrapper.
#
# Exit codes:
# 0: The function was evaluated to completion. The result or any exception
#    wrapped with RemoteExceptionWrapper were written to result_file.
# anything else: There was an error that prevented writing to the result file altogether.
#                The exit code corresponds to whatever the python interpreter gives.
#


def load_pickled_file(filename):
    with open(filename, "rb") as f_in:
        return pickle.load(f_in)


def dump_result_to_file(result_file, result_package):
    with open(result_file, "wb") as f_out:
        pickle.dump(result_package, f_out)


def _remap_file(f: object) -> None:
    if not isinstance(f, File):
        return
    f.url = f.filename


def remap_files(fn_args, fn_kwargs):
    # remap any positional argument given to the function that looks like a
    # File

    for f in fn_kwargs.get('inputs', []) + fn_kwargs.get('outputs', []) + list(fn_args):
        _remap_file(f)

    for k, v in fn_kwargs.items():
        if k in ['stdout', 'stderr']:
            v = File(v)
            fn_kwargs[k] = v
            _remap_file(v)
        else:
            _remap_file(v)


def unpack_function(function_info, user_namespace):
    if "source code" in function_info:
        return unpack_source_code_function(function_info, user_namespace)
    elif "byte code" in function_info:
        return unpack_byte_code_function(function_info, user_namespace)
    else:
        raise ValueError("Function file does not have a valid function representation.")


def unpack_source_code_function(function_info, user_namespace):
    source_code = function_info["source code"]
    name = function_info["name"]
    args = function_info["args"]
    kwargs = function_info["kwargs"]
    return (source_code, name, args, kwargs)


def unpack_byte_code_function(function_info, user_namespace):
    from parsl.serialize import unpack_apply_message
    func, args, kwargs = unpack_apply_message(function_info["byte code"], user_namespace, copy=False)
    return (func, 'parsl_function_name', args, kwargs)


def encode_function(user_namespace, fn, fn_name, fn_args, fn_kwargs):
    # Returns a tuple (code, result_name)
    # code can be exec in the user_namespace to produce result_name.

    # Add variables to the namespace to make function call
    user_namespace.update({'__parsl_args': fn_args,
                           '__parsl_kwargs': fn_kwargs,
                           '__parsl_result': None})

    if isinstance(fn, str):
        code = encode_source_code_function(user_namespace, fn, fn_name)
    elif callable(fn):
        code = encode_byte_code_function(user_namespace, fn, fn_name)
    else:
        raise ValueError("Function object does not look like a function.")

    return code


def encode_source_code_function(user_namespace, fn, fn_name):
    # We drop the first line as it names the parsl decorator used (i.e., @python_app)
    source = fn.split('\n')[1:]
    fn_app = "__parsl_result = {0}(*__parsl_args, **__parsl_kwargs)".format(fn_name)

    source.append(fn_app)

    code = "\n".join(source)
    return code


def encode_byte_code_function(user_namespace, fn, fn_name):
    user_namespace.update({fn_name: fn})
    code = "__parsl_result = {0}(*__parsl_args, **__parsl_kwargs)".format(fn_name)
    return code


def load_function():
    # Decodes the function and its file arguments to be executed into
    # function_code, and updates a user namespace with the function name and
    # the variable named result_name. When the function is executed, its result
    # will be stored in this variable in the user namespace.
    # Returns (namespace, function_code, result_name)

    # Create the namespace to isolate the function execution.
    user_ns = locals()
    user_ns.update({'__builtins__': __builtins__})

    function_info = load_pickled_file('.function')

    (fn, fn_name, fn_args, fn_kwargs) = unpack_function(function_info, user_ns)

    remap_files(fn_args, fn_kwargs)

    code = encode_function(user_ns, fn, fn_name, fn_args, fn_kwargs)

    return (user_ns, code)


def execute_function(namespace, function_code):
    # On executing the function inside the namespace, its result will be in a
    # variable named result_name.

    exec(function_code, namespace, namespace)
    return namespace.get('__parsl_result')


if __name__ == "__main__":
    try:
        # parse the three required command line arguments:
        # map_file: contains a pickled dictionary to map original names to
        #           names at the execution site.
        # function_file: contains the pickled parsl function to execute.
        # result_file: any output (including exceptions) will be written to
        #              this file.

        try:
            (namespace, function_code) = load_function()
        except Exception:
            print("There was an error setting up the function for execution.")
            raise

        try:
            result = execute_function(namespace, function_code)
        except Exception:
            print("There was an error executing the function.")
            raise
    except Exception:
        traceback.print_exc()
        result = RemoteExceptionWrapper(*sys.exc_info())

    # Write out function result to the result file
    try:
        dump_result_to_file('.result', result)
    except Exception:
        print("Could not write to result file.")
        traceback.print_exc()
        sys.exit(1)
