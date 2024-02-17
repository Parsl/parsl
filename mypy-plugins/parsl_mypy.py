from mypy.plugin import FunctionContext, Plugin
from mypy.types import Type
import mypy.nodes as nodes

def plugin(v):
    return ParslMypyPlugin

class ParslMypyPlugin(Plugin):
    def get_type_analyze_hook(self, t):
        # print("BENC: gtah t={}".format(t))
        return None

    def get_function_hook(self, f):
        if f == "parsl.app.app.python_appXXXX":
            return python_app_function_hook
        else:
            return None

def python_app_function_hook(ctx: FunctionContext) -> Type:
    print("inside python_app function_hook")
    print("ctx = {}".format(ctx))

    # if python_app is being called with a function parameter (rather than
    # None, the default) then the return type of the python_app decorator
    # is a variation (with a Future added on the type of the decorated
    # function...)

    if ctx.callee_arg_names[0] == "function":  # will this always be at position 0? probably fragile to assume so, but this code does make that assumption
        print(f"python_app called with a function supplied: {ctx.args[0]}")
        function_node = ctx.args[0][0]
        print(f"function node repr is {repr(function_node)} with type {type(function_node)}")

        # return the type of function_node - actually it needs modifying to have the Future wrapper added....
        if isinstance(function_node, nodes.TempNode):
            print(f"temporary node has type {function_node.type}")
            print(f"Python type of tempnode.type is {type(function_node.type)}")
            print(ctx.api)
            # return_type = ctx.api.named_type_or_none("concurrent.futures.Future", [function_node.type.ret_type])
            # return_type = ctx.api.named_generic_type("concurrent.futures.Future", [function_node.type.ret_type])
            # return_type = ctx.api.named_generic_type("builtins.list", [function_node.type.ret_type])
            return_type = function_node.type.ret_type
            # return_type = ctx.default_return_type
            print(f"plugin chosen return type is {return_type}")
            return function_node.type.copy_modified(ret_type=return_type)
        else:
            print("function node is not specified as something this plugin understands")
            return_type = ctx.default_return_type
            return return_type
    else:
        print("python_app called without a function supplied")
        # TODO: this should return a type that is aligned with the implementation:
        # it's the type of the decorator, assuming that it will definitely be given
        # a function this time? or something along those lines...

        print("will return ctx.default_return_type = {}".format(ctx.default_return_type))
        return ctx.default_return_type
