""" Definitions for the @App decorator and the App classes
"""

import sys
import logging
import subprocess

logger = logging.getLogger(__name__)

from parsl.app.futures import DataFuture

class APP (object):
    """ Encapsulates the generic App
    """

    def __init__ (self, func, executor, inputs=[], outputs=[], env={},
                  walltime=60, exec_type="bash"):
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
            proc = subprocess.Popen(self.executable, stdout=std_out, stderr=std_err, shell=True)
            proc.wait()
        except Exception as e:
            logger.error("Caught exception : {0}".format(e))
            self.error = e
            self.status = 'failed'
            return -1

        self.exec_duration = time.time() - start_t
        logger.debug("RunCommand Completed {0}".format(self.executable))
        return self.exec_duration

    def test(self):
        import time
        time.sleep(0)
        return 100

    def __call__(self, *args, **kwargs):
        logger.debug("In __Call__")
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

        logger.debug("Submitting via : {0}".format( self.executor))
        #print(kwargs)
        logger.debug("cmd    : ", self._locals['cmd_line'])
        self.executable = self._locals['cmd_line'].format(**kwargs)
        logger.debug("Exec : {0}".format(self.executable))


        app_fut = self.executor.submit(self._callable)

        out_futs = [DataFuture(app_fut, o) for o in kwargs['outputs'] ]
        return app_fut, out_futs
        #return self.executor.submit(self.test)

class BashApp(APP):
    """ Extend App to cover the Bash App
    TODO : Well, this needs a lot of work.
    """
    def __init__ (self, func, executor, inputs=[], outputs=[], env={},
                  walltime=60, exec_type="bash"):
        ''' Initialize the bash app
        '''
        super(BashApp, self).__init__(*args, **kwargs)


def App(apptype, executor):
    # App Decorator

    logger.debug('Apptype : ')
    logger.debug('Executor : ')

    app_def = { "exec_type" : apptype,
                "inputs" : [],
                "outputs" : [],
                "env" : {} }

    def Exec(f):
        logger.debug("Decorator Exec : ", f)
        return APP(f, executor, **app_def)

    return Exec

if __name__ == '__main__' :

    app = APP ("echo 'hi'")

    print(app())
