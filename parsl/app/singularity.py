from functools import update_wrapper
from functools import partial
from inspect import signature, Parameter
from parsl.app.errors import wrap_error
from parsl.app.app import AppBase
from parsl.dataflow.dflow import DataFlowKernelLoader
import sys
import inspect
import codecs
import re
import pickle

import logging

logger = logging.getLogger(__name__)


def remote_side_bash_executor(func, *args, **kwargs):
    """Executes the supplied function with *args and **kwargs to get a
    command-line to run, and then run that command-line using bash.
    """
    import os
    import subprocess
    import parsl.app.errors as pe
    from parsl.utils import get_std_fname_mode

    func_name = func.__name__
    command = kwargs.get('command')
    walltime = kwargs.get('walltime')
    logger.debug("COMMAND: {}".format(command))

    executable = None

    # Try to run the func to compose the commandline
    try:
        # Execute the func to get the commandline
        executable = func(*args, **kwargs)
        if not isinstance(executable, str):
            raise ValueError(f"Expected a str for bash_app commandline, got {type(executable)}")

    except AttributeError as e:
        if executable is not None:
            raise pe.AppBadFormatting("App formatting failed for app '{}' with AttributeError: {}".format(func_name, e))
        else:
            raise pe.BashAppNoReturn("Bash app '{}' did not return a value, or returned None - with this exception: {}".format(func_name, e))

    except IndexError as e:
        raise pe.AppBadFormatting("App formatting failed for app '{}' with IndexError: {}".format(func_name, e))
    except Exception as e:
        raise e

    # Updating stdout, stderr if values passed at call time.

    def open_std_fd(fdname):
        # fdname is 'stdout' or 'stderr'
        stdfspec = kwargs.get(fdname)  # spec is str name or tuple (name, mode)
        if stdfspec is None:
            return None

        fname, mode = get_std_fname_mode(fdname, stdfspec)
        try:
            if os.path.dirname(fname):
                os.makedirs(os.path.dirname(fname), exist_ok=True)
            fd = open(fname, mode)
        except Exception as e:
            raise pe.BadStdStreamFile(fname, e)
        return fd

    std_out = open_std_fd('stdout')
    std_err = open_std_fd('stderr')
    timeout = 60 #kwargs.get('walltime')

    if std_err is not None:
        print('--> executable follows <--\n{}\n--> end executable <--'.format(executable), file=std_err, flush=True)

    returncode = None
    result = None
    with open('log.log','w') as log:
        try:
                log.write(command+'\n')
                proc = subprocess.Popen(command, stdout=std_out, stderr=std_err, shell=True)
                proc.wait(timeout=timeout)
                returncode = proc.returncode

                import pickle
                log.write("RT: "+str(returncode))
                with open('output.pickle', 'rb') as input:
                    result = pickle.load(input)

        except subprocess.TimeoutExpired:
            import traceback
            log.write(traceback.format_exc())
            raise pe.AppTimeout("[{}] App exceeded walltime: {} seconds".format(func_name, timeout))

        except Exception as e:
            import traceback
            log.write("COMMAND:{}\n".format(command))
            log.write(traceback.format_exc())
            raise pe.AppException("[{}] App caught exception with returncode: {}".format(func_name, traceback.format_exc()), e)

    if returncode > 1:
        raise pe.BashExitFailure(func_name, proc.returncode)

    # TODO : Add support for globs here

    missing = []
    for outputfile in kwargs.get('outputs', []):
        fpath = outputfile.filepath

        if not os.path.exists(fpath):
            missing.extend([outputfile])

    if missing:
        raise pe.MissingOutputs("[{}] Missing outputs".format(func_name), missing)

    return result


class SingularityApp(AppBase):

    def __init__(self, func, data_flow_kernel=None, cache=False, executors='all', ignore_for_cache=None, cmd=None, image=None, python=None, data=None, walltime=60):
        super().__init__(func, data_flow_kernel=data_flow_kernel, executors=executors, cache=cache, ignore_for_cache=ignore_for_cache)
        self.kwargs = {}

        sig = signature(func)

        if data:
            self.command = "{} exec --bind {}:/data --bind .:/app {} ".format(cmd, data, image)
        else:
            self.command = "{} exec --bind .:/app {} ".format(cmd, image)

        self.python = python
        self.data = data

        for s in sig.parameters:
            if sig.parameters[s].default is not Parameter.empty:
                self.kwargs[s] = sig.parameters[s].default

        remote_fn = partial(remote_side_bash_executor, self.func, image=image, command=self.command, walltime=walltime)
        remote_fn.__name__ = self.func.__name__
        
        self.kwargs['script'] = 'container'
        logger.debug("Func name: {}".format(self.func.__name__))

        self.wrapped_remote_function = remote_fn
        self.walltime = walltime

    def __call__(self, *args, **kwargs):
        """Handle the call to a Bash app.

        Args:
             - Arbitrary

        Kwargs:
             - Arbitrary

        Returns:
                   App_fut

        """
        invocation_kwargs = {}
        invocation_kwargs.update(self.kwargs)
        invocation_kwargs.update(kwargs)

        if self.data_flow_kernel is None:
            dfk = DataFlowKernelLoader.dfk()
        else:
            dfk = self.data_flow_kernel

        import json
        lines = inspect.getsource(self.func)
        appname = self.func.__name__

        inputs = kwargs['inputs'] if 'inputs' in kwargs else []

        try:
            logger.debug("Appname: {} Inputs: {}".format(appname, json.dumps(inputs)))
        except:
            logger.debug("Appname: {} ".format(appname))

        try:
            pargs = codecs.encode(pickle.dumps(inputs), "base64").decode()
            pargs = re.sub(r'\n', "", pargs).strip()
        except:
            pargs = codecs.encode(pickle.dumps([]), "base64").decode()
            pargs = re.sub(r'\n', "", pargs).strip()

        source = "import pickle\n" \
                 "import os\n" \
                 "import json\n" \
                 "import codecs\n" \
                 "{}\n" \
                 "pargs = '{}'\n" \
                 "args = pickle.loads(codecs.decode(pargs.encode(), \"base64\"))\n" \
                 "result = {}(inputs=[*args])\n" \
                 "with open('output.pickle','wb') as output:\n" \
                 "    pickle.dump(result, output)\n".format(
            lines,
            pargs,
            appname) + \
                 "metadata = {\"type\":\"python\",\"file\":os.path.abspath('output.pickle')}\n" \
                 "with open('job.metadata','w') as job:\n" \
                 "    job.write(json.dumps(metadata))\n" \
                 "print(result)\n"

        source = source.replace('@python_app', '#@python_app')
        source = source.replace('@container_app', '#@container_app')
        source = source.replace('@bash_app', '#@bash_app')

        with open('app.py', 'w') as app:
            app.write(source)

        shell_app = self.command+" /files/runapp.sh <<HEREDOC\n{}\nHEREDOC".format(source)

        def invoke_container(command=None, inputs=[]):

            return command

        remote_fn = partial(remote_side_bash_executor, invoke_container, command=shell_app)
        remote_fn.__name__ = self.func.__name__
        self.wrapped_remote_function = wrap_error(remote_fn)
        self.wrapped_remote_function.func = self.func

        app_fut = dfk.submit(self.wrapped_remote_function,
                             app_args=args,
                             executors=self.executors,
                             cache=self.cache,
                             ignore_for_cache=self.ignore_for_cache,
                             app_kwargs=invocation_kwargs)

        return app_fut
