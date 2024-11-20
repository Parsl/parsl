import os

from parsl.serialize import unpack_res_spec_apply_message


def execute_task(bufs: bytes):
    """Deserialize the buffer and execute the task.
    Returns the result or throws exception.
    """
    f, args, kwargs, resource_spec = unpack_res_spec_apply_message(bufs)

    for varname in resource_spec:
        envname = "PARSL_" + str(varname).upper()
        os.environ[envname] = str(resource_spec[varname])

    # We might need to look into callability of the function from itself
    # since we change it's name in the new namespace
    prefix = "parsl_"
    fname = prefix + "f"
    argname = prefix + "args"
    kwargname = prefix + "kwargs"
    resultname = prefix + "result"

    code = "{0} = {1}(*{2}, **{3})".format(resultname, fname,
                                           argname, kwargname)

    user_ns = locals()
    user_ns.update({
        '__builtins__': __builtins__,
        fname: f,
        argname: args,
        kwargname: kwargs,
        resultname: resultname
    })

    exec(code, user_ns, user_ns)
    return user_ns.get(resultname)
