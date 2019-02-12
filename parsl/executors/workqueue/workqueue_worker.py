import sys
import pickle
from ipyparallel.serialize import unpack_apply_message
import logging

if __name__ == "__main__":
    name = "parsl"
    logger = logging.getLogger(name)

    input_function_file = sys.argv[1]
    output_result_file = sys.argv[2]

    input_function = open(input_function_file, "rb")
    function_tuple = pickle.load(input_function)
    input_function.close()

    user_ns = locals()
    user_ns.update({'__builtins__': __builtins__})
    f, args, kwargs = unpack_apply_message(function_tuple, user_ns, copy=False)

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
