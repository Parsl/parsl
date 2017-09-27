'''Parsl Apps
==========

Here lies the definitions for the @App decorator and the APP classes.
The APP class encapsulates a generic leaf task that can be executed asynchronously.

'''

import sys
import logging
import subprocess
from inspect import signature, Parameter
from concurrent.futures import Future

logger = logging.getLogger(__name__)

from parsl.app.futures import DataFuture
from parsl.app.errors import *
from parsl.dataflow.dflow import DataFlowKernel
from functools import partial


class AppBase (object):
    """
    This is the base class that defines the two external facing functions that an App must define.
    The  __init__ () which is called when the interpretor sees the definition of the decorated
    function, and the __call__ () which is invoked when a decorated function is called by the user.

    """

    def __init__ (self, func, executor, walltime=60, exec_type="bash"):
        ''' Constructor for the APP object.

        Args:
             - func (function): Takes the function to be made into an App
             - executor (executor): Executor for the execution resource

        Kwargs:
             - walltime (int) : Walltime in seconds for the app execution
             - exec_type (string) : App type (bash|python)

        Returns:
             - APP object.

        '''
        self.__name__   = func.__name__
        self.func       = func
        self.executor   = executor
        self.exec_type  = exec_type
        self.status     = 'created'

        sig = signature(func)
        self.kwargs     = {}
        for s in sig.parameters:
            if sig.parameters[s].default != Parameter.empty:
                self.kwargs[s] = sig.parameters[s].default

        self.stdout  = sig.parameters['stdout'].default  if 'stdout'  in sig.parameters else None
        self.stderr  = sig.parameters['stderr'].default  if 'stderr'  in sig.parameters else None
        self.inputs  = sig.parameters['inputs'].default  if 'inputs'  in sig.parameters else []
        self.outputs = sig.parameters['outputs'].default if 'outputs' in sig.parameters else []

    def __call__ (self, *args, **kwargs):
        ''' The __call__ function must be implemented in the subclasses
        '''
        raise NotImplemented


def bash_executor(executable, *args, **kwargs):
    ''' The callable fn for external apps.
    '''
    import os
    import time
    import subprocess
    import logging
    logging.basicConfig(filename='/tmp/bashexec.{0}.log'.format(time.time()), level=logging.DEBUG)

    start_t = time.time()

    try:
        executable = executable.format(*args, **kwargs)
    except IndexError:
        raise AppBadFormatting("App Formatting Failed", None)

    # Updating stdout, stderr if values passed at call time.
    stdout = kwargs.get('stdout', None)
    stderr = kwargs.get('stderr', None)
    logging.debug("Stdout  : %s", stdout)
    logging.debug("Stderr  : %s", stderr)

    std_out = open(stdout, 'w') if stdout else None
    std_err = open(stderr, 'w') if stderr else None

    start_time = time.time()

    try :
        proc = subprocess.Popen(executable, stdout=std_out, stderr=std_err, shell=True, executable='/bin/bash')
        proc.wait()
        returncode = proc.returncode
    except Exception as e:
        #logger.error("Caught exception : {0}".format(e))
        error = e
        status = 'failed'
        raise AppException("App caught exception : {0}".format(proc.returncode), e)
    finally:
        if returncode != 0:
            raise AppFailure("App Failed exit code: {0}".format(proc.returncode), proc.returncode)

    # TODO : Add support for globs here

    missing = []
    for outputfile in kwargs.get('outputs', []):
        fpath = outputfile
        if type(outputfile) != str:
            fpath = outputfile.filepath

        if not os.path.exists(fpath):
            missing.extend([outputfile])

    if missing:
        raise MissingOutputs("Missing outputs", missing)


    exec_duration = time.time() - start_t
    return returncode


class BashApp(AppBase):

    def __init__ (self, func, executor, walltime=60):
        super().__init__ (func, executor, walltime=60, exec_type="bash")

    def _trace_cmdline(self, *args, **kwargs):
        ''' Internal function used to trace the values set to the special variable
        cmd_line in the function body.

        '''

        def tracer(frame, event, arg):
            if event=='return':
                self._locals = frame.f_locals.copy()

        # Activate tracer
        sys.setprofile(tracer)
        try:
            # trace the function call
            res = self.func(*args, **kwargs)
        finally:
            # disable tracer and replace with old one
            sys.setprofile(None)

        return self._locals['cmd_line']


    def __call__(self, *args, **kwargs):
        ''' This is where the call to a Bash app is handled

        Args:
             - Arbitrary

        Kwargs:
             - Arbitrary

        Returns:
             If outputs=[...] was a kwarg then:
                   App_fut, [Data_Futures...]
             else:
                   App_fut

        '''

        cmd_line = self._trace_cmdline(*args, **kwargs)
        self.kwargs.update(kwargs)
        self.executable = cmd_line

        app_fut = self.executor.submit(bash_executor, cmd_line, *args, **self.kwargs)

        logger.debug("Tid : %s" % app_fut.tid)
        out_futs = [DataFuture(app_fut, o, parent=app_fut, tid=app_fut.tid) for o in kwargs.get('outputs', []) ]
        app_fut._outputs = out_futs

        return app_fut



def app_wrapper (func):

    def wrapper(*args, **kwargs):
        logger.debug("App wrapper begins")
        x = func(*args, **kwargs)
        logger.debug("App wrapper ends")
        return x

    return wrapper

class PythonApp(AppBase):
    """ Extends AppBase to cover the Python App

    """
    def __init__ (self, func, executor, walltime=60):
        ''' Initialize the super. This bit is the same for both bash & python apps.
        '''
        super().__init__ (func, executor, walltime=60, exec_type="python")


    def __call__(self, *args, **kwargs):
        ''' This is where the call to a python app is handled

        Args:
             - Arbitrary
        Kwargs:
             - Arbitrary

        Returns:
             If outputs=[...] was a kwarg then:
                   App_fut, [Data_Futures...]
             else:
                   App_fut

        '''
        #logger.debug("Submitting to : %s", self.executor )
        #app_fut = self.executor.submit(app_wrapper(self.func), *args, **kwargs)
        app_fut = self.executor.submit(self.func, *args, **kwargs)

        out_futs = [DataFuture(app_fut, o, parent=app_fut, tid=app_fut.tid) for o in kwargs.get('outputs', []) ]
        app_fut._outputs = out_futs

        return app_fut


def App(apptype, executor, walltime=60):
    ''' The App decorator function

    Args:
        apptype (string) : Apptype can be bash|python
        executor (Executor) : Executor object wrapping threads/process pools etc.

    Kwargs:
        walltime (int) : Walltime for app in seconds, default=60

    Returns:
         An AppFactory object, which when called runs the apps through the executor.
    '''

    from parsl import APP_FACTORY_FACTORY

    def Exec(f):
        return APP_FACTORY_FACTORY.make(apptype, executor, f, walltime=walltime)

    return Exec
