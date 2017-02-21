""" Parsl Apps
==============

Here lies the definitions for the @App decorator and the APP classes.
The APP class encapsulates a generic leaf task that can be executed asynchronously.

"""

import sys
import logging
import subprocess
from concurrent.futures import Future

logger = logging.getLogger(__name__)

from parsl.app.futures import DataFuture
from parsl.app.errors import *
from parsl.dataflow.dflow import DataFlowKernel
from functools import partial

class APP (object):
    """ Encapsulates the generic App
    """

    def __init__ (self, func, executor, inputs=[], outputs=[], env={},
                  walltime=60, exec_type="bash"):
        ''' Constructor
        '''
        self.func       = func
        self.inputs     = inputs
        self.executor   = executor
        self.outputs    = outputs
        self.exec_type  = exec_type
        self.status     = 'created'
        self.stdout     = 'STDOUT.txt'
        self.stderr     = 'STDERR.txt'
        logger.debug('__init__ ')

    def _callable(self):
        ''' The callable fn for external apps.
        '''
        import time
        import subprocess
        start_t = time.time()
        if self.exec_type != "bash":
            raise NotImplemented

        logger.debug("Running app : bash")
        std_out = open(self.stdout, 'w')
        std_err = open(self.stderr, 'w')
        start_time = time.time()

        try :
            logger.debug("Launching app : {0}".format(self.executable))
            proc = subprocess.Popen(self.executable, stdout=std_out, stderr=std_err, shell=True, executable='/bin/bash')
            proc.wait()
        except Exception as e:
            logger.error("Caught exception : {0}".format(e))
            self.error = e
            self.status = 'failed'
            return -1

        self.exec_duration = time.time() - start_t
        logger.debug("RunCommand Completed {0}".format(self.executable))
        return self.exec_duration

    def __call__(self, *args, **kwargs):
        logger.debug("In __Call__")

        input_deps = []
        if 'inputs' in kwargs:
            # Identify the futures in the inputs
            logger.debug("Received : %s ", kwargs['inputs'])

            input_deps = [item for item in kwargs['inputs']
                          if isinstance(item, Future) or issubclass(type(item), Future)]

            # kwargs['inputs'] is a list of strings or DataFutures
            newlist = []
            for item in kwargs['inputs']:
                if isinstance(item, DataFuture):
                    newlist.append(item.filepath)
                else:
                    newlist.append(item)
            kwargs['inputs'] = newlist


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

        logger.debug("Submitting via : %s",  self.executor)
        #print(kwargs)
        logger.debug("cmd    : %s", self._locals['cmd_line'])
        self.executable = self._locals['cmd_line'].format(**kwargs)
        logger.debug("Exec   : %s", self.executable)

        if type(self.executor) == DataFlowKernel:
            app_fut = self.executor.submit(self._callable, input_deps, None)
        else:
            app_fut = self.executor.submit(self._callable)

        out_futs = [DataFuture(app_fut, o) for o in kwargs.get('outputs', []) ]
        return app_fut, out_futs


class PythonApp(object):
    """ Extend App to cover the Python App
    TODO : Well, this needs a lot of work.
    """
    def __init__ (self, func, executor, inputs=[], outputs=[], env={},
                  walltime=60, exec_type='python'):
        ''' Initialize the python app
        '''
        self.func       = func
        self.inputs     = inputs
        self.executor   = executor
        self.outputs    = outputs
        self.exec_type  = exec_type
        self.status     = 'created'
        self.stdout     = 'STDOUT.txt'
        self.stderr     = 'STDERR.txt'
        logger.debug('__init__ ')

    def __call__(self, *args, **kwargs):
        logger.debug("In __Call__")

        input_deps = []

        input_deps.extend([item for item in args
                           if isinstance(item, Future) or issubclass(type(item), Future)])

        if 'inputs' in kwargs:
            # Identify the futures in the inputs
            logger.debug("Received : %s ", kwargs['inputs'])

            input_deps.extend([item for item in kwargs['inputs']
                               if isinstance(item, Future) or issubclass(type(item), Future)])

            # kwargs['inputs'] is a list of strings or DataFutures
            newlist = []
            for item in kwargs['inputs']:
                if isinstance(item, DataFuture):
                    newlist.append(item.filepath)
                else:
                    newlist.append(item)
            kwargs['inputs'] = newlist


        logger.debug("Submitting via : %s",  self.executor)

        self.executable = partial(self.func, *args, **kwargs)
        logger.debug("Exec   : %s", self.executable)

        if type(self.executor) == DataFlowKernel:
            app_fut = self.executor.submit(self.executable, input_deps, None)
        else:
            app_fut = self.executor.submit(self.executable)

        out_futs = [DataFuture(app_fut, o) for o in kwargs.get('outputs', []) ]
        return app_fut, out_futs


def App(apptype, executor):
    # App Decorator

    logger.debug('Apptype : %s', apptype)
    logger.debug('Executor : %s', type(executor))

    app_def = { "exec_type" : apptype,
                "inputs"    : [],
                "outputs"   : [],
                "env"       : {} }

    if apptype == 'bash' :
        def Exec(f):
            logger.debug("Decorator Exec : %s", f)
            return APP(f, executor, **app_def)

        return Exec

    elif apptype == 'python' :
        def Exec(f):
            logger.debug("Decorator Exec : %s", f)
            return PythonApp(f, executor, **app_def)

        return Exec
    else:
        raise InvalidAppTypeError("Valid @App types are 'bash' or 'python'")


if __name__ == '__main__' :

    app = APP ("echo 'hi'")

    print(app())
