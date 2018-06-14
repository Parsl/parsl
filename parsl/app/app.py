"""Definitions for the @App decorator and the App classes.

The App class encapsulates a generic leaf task that can be executed asynchronously.
"""
import logging
from inspect import signature

logger = logging.getLogger(__name__)


class AppBase(object):
    """This is the base class that defines the two external facing functions that an App must define.

    The  __init__ () which is called when the interpreter sees the definition of the decorated
    function, and the __call__ () which is invoked when a decorated function is called by the user.

    """

    def __init__(self, func, executor=None, walltime=60, sites='all', cache=False, exec_type="bash"):
        """Construct the App object.

        Args:
             - func (function): Takes the function to be made into an App

        Kwargs:
             - executor (Executor): Executor for the execution resource. This can be omitted only
               after calling :meth:`parsl.dataflow.dflow.DataFlowKernelLoader.load`.
             - walltime (int) : Walltime in seconds for the app execution
             - sites (str|list) : List of site names that this app could execute over. default is 'all'
             - exec_type (string) : App type (bash|python)
             - cache (Bool) : Enable caching of this app ?

        Returns:
             - App object.

        """
        self.__name__ = func.__name__
        self.func = func
        self.executor = executor
        self.exec_type = exec_type
        self.status = 'created'
        self.sites = sites
        self.cache = cache

        params = signature(func).parameters

        self.kwargs = {}
        if 'stdout' in params:
            self.kwargs['stdout'] = params['stdout'].default
        if 'stderr' in params:
            self.kwargs['stderr'] = params['stderr'].default
        self.outputs = params['outputs'].default if 'outputs' in params else []
        self.inputs = params['inputs'].default if 'inputs' in params else []

    def __call__(self, *args, **kwargs):
        """The __call__ function must be implemented in the subclasses."""
        raise NotImplementedError


def app_wrapper(func):

    def wrapper(*args, **kwargs):
        logger.debug("App wrapper begins")
        x = func(*args, **kwargs)
        logger.debug("App wrapper ends")
        return x

    return wrapper


def App(apptype, executor=None, walltime=60, cache=False, sites='all'):
    """The App decorator function.

    Args:
        - apptype (string) : Apptype can be bash|python

    Kwargs:
        - executor (Executor): Executor for the execution resource. This can be omitted only
          after calling :meth:`parsl.dataflow.dflow.DataFlowKernelLoader.load`.
        - walltime (int) : Walltime for app in seconds,
             default=60
        - sites (str|List) : List of site names on which the app could execute
             default='all'
        - cache (Bool) : Enable caching of the app call
             default=False

    Returns:
         An AppFactory object, which when called runs the apps through the executor.
    """
    from parsl import APP_FACTORY_FACTORY

    def wrapper(f):
        return APP_FACTORY_FACTORY.make(apptype, f,
                                        executor=executor,
                                        sites=sites,
                                        cache=cache,
                                        walltime=walltime)

    return wrapper
