from parsl.app.errors import RemoteExceptionWrapper
from parsl.data_provider.files import File
from parsl.utils import get_std_fname_mode
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


def remap_location(mapping, parsl_file):
    if not isinstance(parsl_file, File):
        return
    # Below we rewrite .local_path when scheme != file only when the local_name
    # was given by the main parsl process.  This is the case when scheme !=
    # 'file' but .local_path (via filepath) is in mapping.
    if parsl_file.scheme == 'file' or parsl_file.local_path:
        master_location = parsl_file.filepath
        if master_location in mapping:
            parsl_file.local_path = mapping[master_location]


def remap_list_of_files(mapping, maybe_files):
    for maybe_file in maybe_files:
        remap_location(mapping, maybe_file)


def remap_all_files(mapping, fn_args, fn_kwargs):
    # remap any positional argument given to the function that looks like a
    # File
    remap_list_of_files(mapping, fn_args)

    # remap any keyword argument in the same way, but we need to treat
    # "inputs" and "outputs" specially because they are lists, and
    # "stdout" and "stderr", because they are not File's.
    for kwarg, maybe_file in fn_kwargs.items():
        if kwarg in ["inputs", "outputs"]:
            remap_list_of_files(mapping, maybe_file)
        if kwarg in ["stdout", "stderr"]:
            (fname, mode) = get_std_fname_mode(kwarg, maybe_file)
            if fname in mapping:
                fn_kwargs[kwarg] = (mapping[fname], mode)
        else:
            # Treat anything else as a possible File to be remapped.
            remap_location(mapping, maybe_file)


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
    prefix = "parsl_"
    args_name = prefix + "args"
    kwargs_name = prefix + "kwargs"
    result_name = prefix + "result"

    # Add variables to the namespace to make function call
    user_namespace.update({args_name: fn_args,
                           kwargs_name: fn_kwargs,
                           result_name: result_name})

    if isinstance(fn, str):
        code = encode_source_code_function(user_namespace, fn, fn_name, args_name, kwargs_name, result_name)
    elif callable(fn):
        code = encode_byte_code_function(user_namespace, fn, fn_name, args_name, kwargs_name, result_name)
    else:
        raise ValueError("Function object does not look like a function.")

    return (code, result_name)


def encode_source_code_function(user_namespace, fn, fn_name, args_name, kwargs_name, result_name):
    # We drop the first line as it names the parsl decorator used (i.e., @python_app)
    source = fn.split('\n')[1:]
    fn_app = "{0} = {1}(*{2}, **{3})".format(result_name, fn_name, args_name, kwargs_name)

    source.append(fn_app)

    code = "\n".join(source)
    return code


def encode_byte_code_function(user_namespace, fn, fn_name, args_name, kwargs_name, result_name):
    user_namespace.update({fn_name: fn})
    code = "{0} = {1}(*{2}, **{3})".format(result_name, fn_name, args_name, kwargs_name)
    return code


def load_function(map_file, function_file):
    # Decodes the function and its file arguments to be executed into
    # function_code, and updates a user namespace with the function name and
    # the variable named result_name. When the function is executed, its result
    # will be stored in this variable in the user namespace.
    # Returns (namespace, function_code, result_name)

    # Create the namespace to isolate the function execution.
    user_ns = locals()
    user_ns.update({'__builtins__': __builtins__})

    function_info = load_pickled_file(function_file)

    (fn, fn_name, fn_args, fn_kwargs) = unpack_function(function_info, user_ns)

    mapping = load_pickled_file(map_file)
    remap_all_files(mapping, fn_args, fn_kwargs)

    (code, result_name) = encode_function(user_ns, fn, fn_name, fn_args, fn_kwargs)

    return (user_ns, code, result_name)


def execute_function(namespace, function_code, result_name):
    # On executing the function inside the namespace, its result will be in a
    # variable named result_name.

    exec(function_code, namespace, namespace)
    result = namespace.get(result_name)

    return result


if __name__ == "__main__":
    try:
        # parse the three required command line arguments:
        # map_file: contains a pickled dictionary to map original names to
        #           names at the execution site.
        # function_file: contains the pickled parsl function to execute.
        # result_file: any output (including exceptions) will be written to
        #              this file.
        try:
            (map_file, function_file, result_file) = sys.argv[1:]
        except ValueError:
            print("Usage:\n\t{} function result mapping\n".format(sys.argv[0]))
            raise

        try:
            (namespace, function_code, result_name) = load_function(map_file, function_file)
        except Exception:
            print("There was an error setting up the function for execution.")
            raise

        try:
            result = execute_function(namespace, function_code, result_name)
        except Exception:
            print("There was an error executing the function.")
            raise
    except Exception:
        traceback.print_exc()
        result = RemoteExceptionWrapper(*sys.exc_info())

    # Write out function result to the result file
    try:
        dump_result_to_file(result_file, result)
    except Exception:
        print("Could not write to result file.")
        traceback.print_exc()
        sys.exit(1)
