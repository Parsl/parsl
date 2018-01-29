'''
Parsl Apps
==========

Here lies the definitions for the @App decorator and the APP classes.
The APP class encapsulates a generic leaf task that can be executed asynchronously.

'''
import logging
from inspect import signature, Parameter

# Logging moved here in the PEP8 conformance fixes.
logger = logging.getLogger(__name__)


class AppBase (object):
    """
    This is the base class that defines the two external facing functions that an App must define.
    The  __init__ () which is called when the interpretor sees the definition of the decorated
    function, and the __call__ () which is invoked when a decorated function is called by the user.

    """

    def __init__(self, func, executor, walltime=60, sites='all', cache=False, exec_type="bash"):
        ''' Constructor for the APP object.

        Args:
             - func (function): Takes the function to be made into an App
             - executor (executor): Executor for the execution resource

        Kwargs:
             - walltime (int) : Walltime in seconds for the app execution
             - sites (str|list) : List of site names that this app could execute over. default is 'all'
             - exec_type (string) : App type (bash|python)
             - cache (Bool) : Enable caching of this app ?

        Returns:
             - APP object.

        '''
        self.__name__ = func.__name__
        self.func = func
        self.executor = executor
        self.exec_type = exec_type
        self.status = 'created'
        self.sites = sites
        self.cache = cache

        sig = signature(func)
        self.kwargs = {}
        for s in sig.parameters:
            if sig.parameters[s].default != Parameter.empty:
                self.kwargs[s] = sig.parameters[s].default

        self.stdout = sig.parameters['stdout'].default if 'stdout' in sig.parameters else None
        self.stderr = sig.parameters['stderr'].default if 'stderr' in sig.parameters else None
        self.inputs = sig.parameters['inputs'].default if 'inputs' in sig.parameters else []
        self.outputs = sig.parameters['outputs'].default if 'outputs' in sig.parameters else []

    def __call__(self, *args, **kwargs):
        ''' The __call__ function must be implemented in the subclasses
        '''
        raise NotImplemented


def app_wrapper(func):

    def wrapper(*args, **kwargs):
        logger.debug("App wrapper begins")
        x = func(*args, **kwargs)
        logger.debug("App wrapper ends")
        return x

    return wrapper


def App(apptype, executor, walltime=60, cache=False, sites='all'):
    ''' The App decorator function

    Args:
        - apptype (string) : Apptype can be bash|python
        - executor (Executor) : Executor object wrapping threads/process pools etc.

    Kwargs:
        - walltime (int) : Walltime for app in seconds,
             default=60
        - sites (str|List) : List of site names on which the app could execute
             default='all'
        - cache (Bool) : Enable caching of the app call
             default=False

    Returns:
         An AppFactory object, which when called runs the apps through the executor.
    '''

    from parsl import APP_FACTORY_FACTORY

    def Exec(f):
        return APP_FACTORY_FACTORY.make(apptype, executor, f,
                                        sites=sites,
                                        cache=cache,
                                        walltime=walltime)

    return Exec
