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

    def __init__(self, func, data_flow_kernel=None, walltime=60, executors='all', cache=False, exec_type="bash"):
        """Construct the App object.

        Args:
             - func (function): Takes the function to be made into an App

        Kwargs:
             - data_flow_kernel (DataFlowKernel): The :class:`~parsl.dataflow.dflow.DataFlowKernel` responsible for
               managing this app. This can be omitted only
               after calling :meth:`parsl.dataflow.dflow.DataFlowKernelLoader.load`.
             - walltime (int) : Walltime in seconds for the app execution.
             - executors (str|list) : Labels of the executors that this app can execute over. Default is 'all'.
             - exec_type (string) : App type (bash|python)
             - cache (Bool) : Enable caching of this app ?

        Returns:
             - App object.

        """
        self.__name__ = func.__name__
        self.func = func
        self.data_flow_kernel = data_flow_kernel
        self.exec_type = exec_type
        self.status = 'created'
        self.executors = executors
        if not (isinstance(executors, list) or isinstance(executors, str)):
            logger.error("App {} specifies invalid executor option, expects string or list".format(
                func.__name__))
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


def App(apptype, data_flow_kernel=None, walltime=60, cache=False, executors='all'):
    """The App decorator function.

    Args:
        - apptype (string) : Apptype can be bash|python

    Kwargs:
        - data_flow_kernel (DataFlowKernel): The :class:`~parsl.dataflow.dflow.DataFlowKernel` responsible for
          managing this app. This can be omitted only
          after calling :meth:`parsl.dataflow.dflow.DataFlowKernelLoader.load`.
        - walltime (int) : Walltime for app in seconds,
             default=60
        - executors (str|list) : Labels of the executors that this app can execute over. Default is 'all'.
        - cache (Bool) : Enable caching of the app call
             default=False

    Returns:
         An AppFactory object, which when called runs the apps through the executor.
    """
    from parsl import APP_FACTORY_FACTORY

    def wrapper(f):
        return APP_FACTORY_FACTORY.make(apptype, f,
                                        data_flow_kernel=data_flow_kernel,
                                        executors=executors,
                                        cache=cache,
                                        walltime=walltime)

    return wrapper


def python_app(function=None, data_flow_kernel=None, walltime=60, cache=False, executors='all'):
    """Decorator function for making python apps.

    Parameters
    ----------
    function : function
        Do not pass this keyword argument directly. This is needed in order to allow for omitted parenthesis,
        for example, `@python_app` if using all defaults or `@python_app(walltime=120)`. If the
        decorator is used alone, function will be the actual function being decorated, whereas if it
        is called with arguments, function will be None. Default is None.
    data_flow_kernel : DataFlowKernel
        The :class:`~parsl.dataflow.dflow.DataFlowKernel` responsible for managing this app. This can
        be omitted only after calling :meth:`parsl.dataflow.dflow.DataFlowKernelLoader.load`. Default is None.
    walltime : int
        Walltime for app in seconds. Default is 60.
    executors : string or list
        Labels of the executors that this app can execute over. Default is 'all'.
    cache : bool
        Enable caching of the app call. Default is False.
    """

    from parsl import APP_FACTORY_FACTORY

    def decorator(func):
        def wrapper(f):
            return APP_FACTORY_FACTORY.make('python', f,
                                            data_flow_kernel=data_flow_kernel,
                                            executors=executors,
                                            cache=cache,
                                            walltime=walltime)

        return wrapper(func)
    if function is not None:
        return decorator(function)
    return decorator


def bash_app(function=None, data_flow_kernel=None, walltime=60, cache=False, executors='all'):
    """Decorator function for making bash apps.

    Parameters
    ----------
    function : function
        Do not pass this keyword argument directly. This is needed in order to allow for omitted parenthesis,
        for example, `@bash_app` if using all defaults or `@bash_app(walltime=120)`. If the
        decorator is used alone, function will be the actual function being decorated, whereas if it
        is called with arguments, function will be None. Default is None.
    data_flow_kernel : DataFlowKernel
        The :class:`~parsl.dataflow.dflow.DataFlowKernel` responsible for managing this app. This can
        be omitted only after calling :meth:`parsl.dataflow.dflow.DataFlowKernelLoader.load`. Default is None.
    walltime : int
        Walltime for app in seconds. Default is 60.
    executors : string or list
        Labels of the executors that this app can execute over. Default is 'all'.
    cache : bool
        Enable caching of the app call. Default is False.
    """
    from parsl import APP_FACTORY_FACTORY

    def decorator(func):
        def wrapper(f):
            return APP_FACTORY_FACTORY.make('bash', f,
                                            data_flow_kernel=data_flow_kernel,
                                            executors=executors,
                                            cache=cache,
                                            walltime=walltime)

        return wrapper(func)
    if function is not None:
        return decorator(function)
    return decorator
