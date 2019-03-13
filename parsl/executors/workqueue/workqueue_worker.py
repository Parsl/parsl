import sys
import pickle
import logging
from ipyparallel.serialize import unpack_apply_message

if __name__ == "__main__":
    name = "parsl"
    logger = logging.getLogger(name)

    shared_fs = False
    input_function_file = None
    output_result_file = None
    remapping_string = None

    index = 1
    while index < len(sys.argv):
        if sys.argv[index] == "-i":
            input_function_file = sys.argv[index + 1]
            index += 1
        elif sys.argv[index] == "-o":
            output_result_file = sys.argv[index + 1]
            index += 1
        elif sys.argv[index] == "-r":
            remapping_string = sys.argv[index + 1]
            index += 1
        elif sys.argv[index] == "--shared-fs":
            shared_fs = True
        else:
            print("command line argument not supported")
            exit(-2)
        index += 1

    input_function = open(input_function_file, "rb")
    function_tuple = pickle.load(input_function)
    input_function.close()

    user_ns = locals()
    user_ns.update({'__builtins__': __builtins__})
    f, args, kwargs = unpack_apply_message(function_tuple, user_ns, copy=False)

    mapping = {}

    if shared_fs is False and remapping_string is not None:
        from parsl.data_provider.files import File

        for i in remapping_string.split(","):
            split_mapping = i.split(":")
            mapping[split_mapping[0]] = split_mapping[1]

        func_inputs = kwargs.get("inputs", [])
        for inp in func_inputs:
            if isinstance(inp, File):
                if inp.filepath in mapping:
                    inp.local_path = mapping[inp.filepath]

        for kwarg, potential_f in kwargs.items():
            if isinstance(potential_f, File):
                if potential_f.filepath in mapping:
                    potential_f.local_path = mapping[potential_f.filepath]

        for inp in args:
            if isinstance(inp, File):
                if inp.filepath in mapping:
                    inp.local_path = mapping[inp.filepath]

        func_outputs = kwargs.get("outputs", [])
        for output in func_outputs:
            if isinstance(output, File):
                if output.filepath in mapping:
                    output.local_path = mapping[output.filepath]

    prefix = "parsl_"
    fname = prefix + "f"
    argname = prefix + "args"
    kwargname = prefix + "kwargs"
    resultname = prefix + "result"

    user_ns.update({fname: f,
                    argname: args,
                    kwargname: kwargs,
                    resultname: resultname})

    code = "{0} = {1}(*{2}, **{3})".format(resultname, fname,
                                           argname, kwargname)

    try:
        # logger.debug("[RUNNER] Executing: {0}".format(code))
        exec(code, user_ns, user_ns)

    except Exception as e:
        logger.warning("Caught exception; will raise it: {}".format(e), exc_info=True)
        raise e
    else:
        result = user_ns.get(resultname)
        f = open(output_result_file, "wb")
        pickle.dump(result, f)
        f.close()
