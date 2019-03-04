import sys
import pickle
import logging
from ipyparallel.serialize import unpack_apply_message

if __name__ == "__main__":
    name = "parsl"
    logger = logging.getLogger(name)

    shared_fs = False
    input_function_file = sys.argv[1]
    output_result_file = sys.argv[2]
    if len(sys.argv) > 3:
        if sys.argv[3] == "--shared-fs":
            shared_fs = True

    input_function = open(input_function_file, "rb")
    function_tuple = pickle.load(input_function)
    input_function.close()

    user_ns = locals()
    user_ns.update({'__builtins__': __builtins__})
    f, args, kwargs = unpack_apply_message(function_tuple, user_ns, copy=False)

    if shared_fs is False:
        from parsl.data_provider.files import File

        func_inputs = kwargs.get("inputs", [])
        for inp in func_inputs:
            if isinstance(inp, File):
                inp.set_in_task()

        for kwarg, potential_f in kwargs.items():
            if isinstance(potential_f, File):
                potential_f.set_in_task()

        for inp in args:
            if isinstance(inp, File):
                inp.set_in_task()

        func_outputs = kwargs.get("outputs", [])
        for output in func_outputs:
            if isinstance(output, File):
                output.set_in_task()

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
