"""Definitions for the @App decorator and the App classes.

The App class encapsulates a generic leaf task that can be executed asynchronously.
"""
import logging
from abc import ABCMeta, abstractmethod
from inspect import signature
from typing import Any, Callable, Dict, List, Optional, Sequence, Union

import typeguard
from typing_extensions import Literal

from parsl.dataflow.dflow import DataFlowKernel
from parsl.dataflow.futures import AppFuture

logger = logging.getLogger(__name__)


class AppBase(metaclass=ABCMeta):
    """This is the base class that defines the two external facing functions that an App must define.

    The  __init__ () which is called when the interpreter sees the definition of the decorated
    function, and the __call__ () which is invoked when a decorated function is called by the user.

    """

    @typeguard.typechecked
    def __init__(self, func: Callable,
                 data_flow_kernel: Optional[DataFlowKernel] = None,
                 executors: Union[List[str], Literal['all']] = 'all',
                 cache: bool = False,
                 ignore_for_cache: Optional[Sequence[str]] = None) -> None:
        """Construct the App object.

        Args:
             - func (function): Takes the function to be made into an App

        Kwargs:
             - data_flow_kernel (DataFlowKernel): The :class:`~parsl.dataflow.dflow.DataFlowKernel` responsible for
               managing this app. This can be omitted only
               after calling :meth:`parsl.dataflow.dflow.DataFlowKernelLoader.load`.
             - executors (str|list) : Labels of the executors that this app can execute over. Default is 'all'.
             - cache (Bool) : Enable caching of this app ?
             - ignore_for_cache (sequence|None): Names of arguments which will be ignored by the caching mechanism.

        Returns:
             - App object.

        """
        self.__name__ = func.__name__
        self.func = func
        self.data_flow_kernel = data_flow_kernel
        self.executors = executors
        self.cache = cache
        self.ignore_for_cache = ignore_for_cache

        params = signature(func).parameters

        self.kwargs: Dict[str, Any]
        self.kwargs = {}
        if 'stdout' in params:
            self.kwargs['stdout'] = params['stdout'].default
        if 'stderr' in params:
            self.kwargs['stderr'] = params['stderr'].default
        if 'walltime' in params:
            self.kwargs['walltime'] = params['walltime'].default
        if 'parsl_resource_specification' in params:
            self.kwargs['parsl_resource_specification'] = params['parsl_resource_specification'].default
        if 'outputs' in params:
            self.kwargs['outputs'] = params['outputs'].default
        if 'inputs' in params:
            self.kwargs['inputs'] = params['inputs'].default

    @abstractmethod
    def __call__(self, *args: Any, **kwargs: Any) -> AppFuture:
        pass


@typeguard.typechecked
def python_app(function: Optional[Callable] = None,
               data_flow_kernel: Optional[DataFlowKernel] = None,
               cache: bool = False,
               executors: Union[List[str], Literal['all']] = 'all',
               ignore_for_cache: Optional[Sequence[str]] = None) -> Callable:
    """Decorator function for making python apps.

    Parameters
    ----------
    function : function
        Do not pass this keyword argument directly. This is needed in order to allow for omitted parenthesis,
        for example, ``@python_app`` if using all defaults or ``@python_app(walltime=120)``. If the
        decorator is used alone, function will be the actual function being decorated, whereas if it
        is called with arguments, function will be None. Default is None.
    data_flow_kernel : DataFlowKernel
        The :class:`~parsl.dataflow.dflow.DataFlowKernel` responsible for managing this app. This can
        be omitted only after calling :meth:`parsl.dataflow.dflow.DataFlowKernelLoader.load`. Default is None.
    executors : string or list
        Labels of the executors that this app can execute over. Default is 'all'.
    cache : bool
        Enable caching of the app call. Default is False.
    ignore_for_cache : (sequence|None)
        Names of arguments which will be ignored by the caching mechanism.
    """
    from parsl.app.python import PythonApp

    def decorator(func: Callable) -> Callable:
        def wrapper(f: Callable) -> PythonApp:
            return PythonApp(f,
                             data_flow_kernel=data_flow_kernel,
                             cache=cache,
                             executors=executors,
                             ignore_for_cache=ignore_for_cache,
                             join=False)
        return wrapper(func)
    if function is not None:
        return decorator(function)
    return decorator


@typeguard.typechecked
def join_app(function: Optional[Callable] = None,
             data_flow_kernel: Optional[DataFlowKernel] = None,
             cache: bool = False,
             ignore_for_cache: Optional[Sequence[str]] = None) -> Callable:
    """Decorator function for making join apps

    Parameters
    ----------
    function : function
        Do not pass this keyword argument directly. This is needed in order to allow for omitted parenthesis,
        for example, ``@python_app`` if using all defaults or ``@python_app(walltime=120)``. If the
        decorator is used alone, function will be the actual function being decorated, whereas if it
        is called with arguments, function will be None. Default is None.
    data_flow_kernel : DataFlowKernel
        The :class:`~parsl.dataflow.dflow.DataFlowKernel` responsible for managing this app. This can
        be omitted only after calling :meth:`parsl.dataflow.dflow.DataFlowKernelLoader.load`. Default is None.
    cache : bool
        Enable caching of the app call. Default is False.
    ignore_for_cache : (sequence|None)
        Names of arguments which will be ignored by the caching mechanism.
    """
    from parsl.app.python import PythonApp

    def decorator(func: Callable) -> Callable:
        def wrapper(f: Callable) -> PythonApp:
            return PythonApp(f,
                             data_flow_kernel=data_flow_kernel,
                             cache=cache,
                             executors=["_parsl_internal"],
                             ignore_for_cache=ignore_for_cache,
                             join=True)
        return wrapper(func)
    if function is not None:
        return decorator(function)
    return decorator


@typeguard.typechecked
def bash_app(function: Optional[Callable] = None,
             data_flow_kernel: Optional[DataFlowKernel] = None,
             cache: bool = False,
             executors: Union[List[str], Literal['all']] = 'all',
             ignore_for_cache: Optional[Sequence[str]] = None) -> Callable:
    """Decorator function for making bash apps.

    Parameters
    ----------
    function : function
        Do not pass this keyword argument directly. This is needed in order to allow for omitted parenthesis,
        for example, ``@bash_app`` if using all defaults or ``@bash_app(walltime=120)``. If the
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
    ignore_for_cache : (list|None)
        Names of arguments which will be ignored by the caching mechanism.
    """
    from parsl.app.bash import BashApp

    def decorator(func: Callable) -> Callable:
        def wrapper(f: Callable) -> BashApp:
            return BashApp(f,
                           data_flow_kernel=data_flow_kernel,
                           cache=cache,
                           executors=executors,
                           ignore_for_cache=ignore_for_cache)
        return wrapper(func)
    if function is not None:
        return decorator(function)
    return decorator
