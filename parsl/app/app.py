"""Definitions for the @App decorator and the App classes.

The App class encapsulates a generic leaf task that can be executed asynchronously.
"""
import logging
from abc import ABCMeta, abstractmethod
from inspect import getsource
from hashlib import md5
from inspect import signature

logger = logging.getLogger(__name__)


class AppBase(metaclass=ABCMeta):
    """This is the base class that defines the two external facing functions that an App must define.

    The  __init__ () which is called when the interpreter sees the definition of the decorated
    function, and the __call__ () which is invoked when a decorated function is called by the user.

    """

    def __init__(self, func, data_flow_kernel=None, executors='all', cache=False, ignore_for_cache=None):
        """Construct the App object.

        Args:
             - func (function): Takes the function to be made into an App

        Kwargs:
             - data_flow_kernel (DataFlowKernel): The :class:`~parsl.dataflow.dflow.DataFlowKernel` responsible for
               managing this app. This can be omitted only
               after calling :meth:`parsl.dataflow.dflow.DataFlowKernelLoader.load`.
             - executors (str|list) : Labels of the executors that this app can execute over. Default is 'all'.
             - cache (Bool) : Enable caching of this app ?

        Returns:
             - App object.

        """
        self.__name__ = func.__name__
        self.func = func
        self.data_flow_kernel = data_flow_kernel
        self.status = 'created'
        self.executors = executors
        self.cache = cache
        self.ignore_for_cache = ignore_for_cache
        if not (isinstance(executors, list) or isinstance(executors, str)):
            logger.error("App {} specifies invalid executor option, expects string or list".format(
                func.__name__))

        if cache is True:
            try:
                self.fn_source = getsource(func)
            except OSError:
                logger.warning("Unable to get source code for app caching. Recommend creating module")
                self.fn_source = func.__name__

            self.func_hash = md5(self.fn_source.encode('utf-8')).hexdigest()
        else:
            self.func_hash = func.__name__

        params = signature(func).parameters

        self.kwargs = {}
        if 'stdout' in params:
            self.kwargs['stdout'] = params['stdout'].default
        if 'stderr' in params:
            self.kwargs['stderr'] = params['stderr'].default
        if 'walltime' in params:
            self.kwargs['walltime'] = params['walltime'].default
        self.outputs = params['outputs'].default if 'outputs' in params else []
        self.inputs = params['inputs'].default if 'inputs' in params else []

    @abstractmethod
    def __call__(self, *args, **kwargs):
        pass


def python_app(function=None, data_flow_kernel=None, cache=False, executors='all', ignore_for_cache=None):
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
    executors : string or list
        Labels of the executors that this app can execute over. Default is 'all'.
    cache : bool
        Enable caching of the app call. Default is False.
    """
    from parsl.app.python import PythonApp

    def decorator(func):
        def wrapper(f):
            return PythonApp(f,
                             data_flow_kernel=data_flow_kernel,
                             cache=cache,
                             executors=executors,
                             ignore_for_cache=ignore_for_cache)
        return wrapper(func)
    if function is not None:
        return decorator(function)
    return decorator


def bash_app(function=None, data_flow_kernel=None, cache=False, executors='all', ignore_for_cache=None):
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
    from parsl.app.bash import BashApp

    def decorator(func):
        def wrapper(f):
            return BashApp(f,
                           data_flow_kernel=data_flow_kernel,
                           cache=cache,
                           executors=executors,
                           ignore_for_cache=ignore_for_cache)
        return wrapper(func)
    if function is not None:
        return decorator(function)
    return decorator
