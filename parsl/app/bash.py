import logging
from functools import update_wrapper
from inspect import signature, Parameter

from parsl.app.errors import wrap_error
from parsl.app.futures import DataFuture
from parsl.app.app import AppBase
from parsl.dataflow.dflow import DataFlowKernelLoader

logger = logging.getLogger(__name__)


def remote_side_bash_executor(func, *args, **kwargs):
    """Execute the bash app type function and return the command line string.

    This string is reformatted with the *args, and **kwargs
    from call time.
    """
    import os
    import time
    import subprocess
    import logging
    import parsl.app.errors as pe

    logging.basicConfig(filename='/tmp/bashexec.{0}.log'.format(time.time()), level=logging.DEBUG)

    # start_t = time.time()

    func_name = func.__name__

    partial_cmdline = None

    # Try to run the func to compose the commandline
    try:
        # Execute the func to get the commandline
        partial_cmdline = func(*args, **kwargs)
        # Reformat the commandline with current args and kwargs
        executable = partial_cmdline.format(*args, **kwargs)

    except AttributeError as e:
        if partial_cmdline is not None:
            raise pe.AppBadFormatting("App formatting failed for app '{}' with AttributeError: {}".format(func_name, e))
        else:
            raise pe.BashAppNoReturn("Bash app '{}' did not return a value, or returned none - with this exception: {}".format(func_name, e), None)

    except IndexError as e:
        raise pe.AppBadFormatting("App formatting failed for app '{}' with IndexError: {}".format(func_name, e))
    except Exception as e:
        logging.error("Caught exception during formatting of app '{}': {}".format(func_name, e))
        raise e

    logging.debug("Executable: %s", executable)

    # Updating stdout, stderr if values passed at call time.

    def open_std_fd(fdname):
        # fdname is 'stdout' or 'stderr'
        stdfspec = kwargs.get(fdname)  # spec is str name or tuple (name, mode)
        if stdfspec is None:
            return None
        elif isinstance(stdfspec, str):
            fname = stdfspec
            mode = 'a+'
        elif isinstance(stdfspec, tuple):
            if len(stdfspec) != 2:
                raise pe.BadStdStreamFile("std descriptor %s has incorrect tuple length %s" % (fdname, len(stdfspec)), TypeError('Bad Tuple Length'))
            fname, mode = stdfspec
        else:
            raise pe.BadStdStreamFile("std descriptor %s has unexpected type %s" % (fdname, str(type(stdfspec))), TypeError('Bad Tuple Type'))

        try:
            if os.path.dirname(fname):
                os.makedirs(os.path.dirname(fname), exist_ok=True)
            fd = open(fname, mode)
        except Exception as e:
            raise pe.BadStdStreamFile(fname, e)
        return fd

    std_out = open_std_fd('stdout')
    std_err = open_std_fd('stderr')
    timeout = kwargs.get('walltime')

    if std_err is not None:
        print('--> executable follows <--\n{}\n--> end executable <--'.format(executable), file=std_err)

    returncode = None
    try:
        proc = subprocess.Popen(executable, stdout=std_out, stderr=std_err, shell=True, executable='/bin/bash')
        proc.wait(timeout=timeout)
        returncode = proc.returncode

    except subprocess.TimeoutExpired:
        # print("Timeout")
        raise pe.AppTimeout("[{}] App exceeded walltime: {}".format(func_name, timeout))

    except Exception as e:
        # print("Caught exception: ", e)
        raise pe.AppException("[{}] App caught exception: {}".format(func_name, proc.returncode), e)

    if returncode != 0:
        raise pe.AppFailure("[{}] App failed with exit code: {}".format(func_name, proc.returncode), proc.returncode)

    # TODO : Add support for globs here

    missing = []
    for outputfile in kwargs.get('outputs', []):
        fpath = outputfile
        if type(outputfile) != str:
            fpath = outputfile.filepath

        if not os.path.exists(fpath):
            missing.extend([outputfile])

    if missing:
        raise pe.MissingOutputs("[{}] Missing outputs".format(func_name), missing)

    # exec_duration = time.time() - start_t
    return returncode


class BashApp(AppBase):

    def __init__(self, func, data_flow_kernel=None, walltime=60, cache=False, executors='all'):
        super().__init__(func, data_flow_kernel=data_flow_kernel, walltime=60, executors=executors, cache=cache)
        self.kwargs = {}

        # We duplicate the extraction of parameter defaults
        # to self.kwargs to ensure availability at point of
        # command string format. Refer: #349
        sig = signature(func)

        for s in sig.parameters:
            if sig.parameters[s].default != Parameter.empty:
                self.kwargs[s] = sig.parameters[s].default

    def __call__(self, *args, **kwargs):
        """Handle the call to a Bash app.

        Args:
             - Arbitrary

        Kwargs:
             - Arbitrary

        Returns:
             If outputs=[...] was a kwarg then:
                   App_fut, [Data_Futures...]
             else:
                   App_fut

        """
        # Update kwargs in the app definition with ones passed in at calltime
        self.kwargs.update(kwargs)

        if self.data_flow_kernel is None:
            dfk = DataFlowKernelLoader.dfk()
        else:
            dfk = self.data_flow_kernel

        app_fut = dfk.submit(wrap_error(update_wrapper(remote_side_bash_executor, self.func)),
                             self.func, *args,
                             executors=self.executors,
                             fn_hash=self.func_hash,
                             cache=self.cache,
                             **self.kwargs)

        out_futs = [DataFuture(app_fut, o, tid=app_fut.tid)
                    for o in kwargs.get('outputs', [])]
        app_fut._outputs = out_futs

        return app_fut
