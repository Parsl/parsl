import os

from parsl.serialize import unpack_apply_message


def execute_task(bufs: bytes, context: dict | None = None):
    """Deserialize the buffer and execute the task.
    Returns the result or throws exception.
    """
    f, args, kwargs = unpack_apply_message(bufs)

    if context:
        res_spec = context.get("resource_spec", {})
        for varname in res_spec:
            envname = "PARSL_" + str(varname).upper()
            os.environ[envname] = str(res_spec[varname])

    # We might need to look into callability of the function from itself
    # since we change its name in the new namespace
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
