from parsl.data_provider.files import File
from parsl.utils import get_std_fname_mode
import sys
import pickle

def load_pickled_file(filename):
    try:
        with open(filename, "rb") as f_in:
            return pickle.load(f_in)
    except pickle.PickleError:
        print("Error while unplickling {}".format(filename))
        sys.exit(2)

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


if __name__ == "__main__":
    name = "parsl"
    fn_from_source = False
    file_type_string = None

    # Parse command line options
    try:
        if len(sys.argv) != 4:
            raise IndexError

        (map_file, function_file, result_file) = sys.argv[1:4]
    except IndexError:
        print("Usage:\n\t{} function result mapping".format(sys.argv[0]))
        exit(1)

    # Get all variables from the user namespace, and add __builtins__
    user_ns = locals()
    user_ns.update({'__builtins__': __builtins__})

    # Load function data
    try:
        function_info = load_pickled_file(function_file)
        # Extract information from transferred source code
        if "source code" in function_info:
            fn_from_source = True
            source_code = function_info["source code"]
            name = function_info["name"]
            args = function_info["args"]
            kwargs = function_info["kwargs"]
        # Extract information from function pointer
        elif "byte code" in function_info:
            fn_from_source = False
            from ipyparallel.serialize import unpack_apply_message
            func, args, kwargs = unpack_apply_message(function_info["byte code"], user_ns, copy=False)
        else:
            print("Function file does not have a valid function representation.")
            sys.exit(2)
    except Exception as e:
        print(e)
        exit(2)

    try:
        mapping = load_pickled_file(map_file)
        for maybe_file in args:
            remap_location(mapping, maybe_file)
        for maybe_file in kwargs.get("inputs", []):
            remap_location(mapping, maybe_file)
        for maybe_file in kwargs.get("outputs", []):
            remap_location(mapping, maybe_file)

        # Iterate through all arguments to the function
        for kwarg, maybe_file in kwargs.items():
            # Process the "stdout" and "stderr" arguments and add them to kwargs
            # They come in the form of str, or (str, str)
            if kwarg == "stdout" or kwarg == "stderr":
                (fname, mode) = get_std_fname_mode(kwarg, maybe_file)
                if fname in mapping:
                    kwargs[kwarg] = (mapping[fname], mode)
            elif isinstance(maybe_file, File):
                remap_location(mapping, maybe_file)
    except Exception as e:
        print(e)
        exit(3)

    prefix = "parsl_"
    argname = prefix + "args"
    kwargname = prefix + "kwargs"
    resultname = prefix + "result"

    # Add variables to the namespace to make function call
    user_ns.update({argname: args,
                    kwargname: kwargs,
                    resultname: resultname})

    # Import function source code and create function call
    if fn_from_source:
        source_list = source_code.split('\n')[1:]
        full_source = ""
        for line in source_list:
            full_source = full_source + line + "\n"
        code = "{0} = {1}(*{2}, **{3})".format(resultname, name,
                                               argname, kwargname)
        code = full_source + code
    # Otherwise, only import function call
    else:
        fname = prefix + "f"
        user_ns.update({fname: func})
        code = "{0} = {1}(*{2}, **{3})".format(resultname, fname,
                                               argname, kwargname)

    # Perform the function call and handle errors
    try:
        exec(code, user_ns, user_ns)
    # Failed function execution
    except Exception:
        from tblib import pickling_support
        pickling_support.install()
        exec_info = sys.exc_info()
        result_package = {"failure": True, "result": pickle.dumps(exec_info)}
    # Successful function execution
    else:
        result = user_ns.get(resultname)
        result_package = {"failure": False, "result": result}

    # Write out function result to the result file
    try:
        with open(result_file, "wb") as f_out:
            pickle.dump(result_package, f_out)
        exit(0)
    except Exception as e:
        print(e)
        exit(4)
