import inspect
import logging
import os
import shlex
import subprocess
import time
import typeguard
from contextlib import contextmanager
from typing import List

import parsl
from parsl.version import VERSION

logger = logging.getLogger(__name__)


@typeguard.typechecked
def get_version() -> str:
    version = parsl.__version__
    work_tree = os.path.dirname(os.path.dirname(__file__))
    git_dir = os.path.join(work_tree, '.git')
    if os.path.exists(git_dir):
        env = {'GIT_WORK_TREE': work_tree, 'GIT_DIR': git_dir}
        try:
            cmd = shlex.split('git rev-parse --short HEAD')
            head = subprocess.check_output(cmd, env=env).strip().decode('utf-8')
            diff = subprocess.check_output(shlex.split('git diff HEAD'), env=env)
            status = 'dirty' if diff else 'clean'
            version = '{v}-{head}-{status}'.format(v=VERSION, head=head, status=status)
        except Exception:
            pass

    return version


@typeguard.typechecked
def get_all_checkpoints(rundir: str = "runinfo") -> List[str]:
    """Finds the checkpoints from all last runs.

    Note that checkpoints are incremental, and this helper will not find
    previous checkpoints from earlier than the most recent run. It probably
    should be made to do so.

    Kwargs:
       - rundir(str) : Path to the runinfo directory

    Returns:
       - a list suitable for the checkpointFiles parameter of the DataFlowKernel
         constructor

    """

    if(not os.path.isdir(rundir)):
        return []

    dirs = sorted(os.listdir(rundir))

    checkpoints = []

    for runid in dirs:

        checkpoint = os.path.abspath('{}/{}/checkpoint'.format(rundir, runid))

        if os.path.isdir(checkpoint):
            checkpoints.append(checkpoint)

    return checkpoints


@typeguard.typechecked
def get_last_checkpoint(rundir: str = "runinfo") -> List[str]:
    """Finds the checkpoint from the last run, if one exists.

    Note that checkpoints are incremental, and this helper will not find
    previous checkpoints from earlier than the most recent run. It probably
    should be made to do so.

    Kwargs:
       - rundir(str) : Path to the runinfo directory

    Returns:
     - a list suitable for the checkpointFiles parameter of the DataFlowKernel
       constructor, with 0 or 1 elements

    """
    if not os.path.isdir(rundir):
        return []

    dirs = sorted(os.listdir(rundir))

    if len(dirs) == 0:
        return []

    last_runid = dirs[-1]
    last_checkpoint = os.path.abspath('{}/{}/checkpoint'.format(rundir, last_runid))

    if(not(os.path.isdir(last_checkpoint))):
        return []

    return [last_checkpoint]


def get_std_fname_mode(fdname, stdfspec):
    import parsl.app.errors as pe
    if stdfspec is None:
        return None, None
    elif isinstance(stdfspec, str):
        fname = stdfspec
        mode = 'a+'
    elif isinstance(stdfspec, tuple):
        if len(stdfspec) != 2:
            raise pe.BadStdStreamFile("std descriptor %s has incorrect tuple length %s" % (fdname, len(stdfspec)), TypeError('Bad Tuple Length'))
        fname, mode = stdfspec
        if not isinstance(fname, str) or not isinstance(mode, str):
            raise pe.BadStdStreamFile("std descriptor %s has unexpected type %s" % (fdname, str(type(stdfspec))), TypeError('Bad Tuple Type'))
    else:
        raise pe.BadStdStreamFile("std descriptor %s has unexpected type %s" % (fdname, str(type(stdfspec))), TypeError('Bad Tuple Type'))
    return fname, mode


@contextmanager
def wait_for_file(path, seconds=10):
    for i in range(0, int(seconds * 100)):
        time.sleep(seconds / 100.)
        if os.path.exists(path):
            break
    yield


@contextmanager
def time_limited_open(path, mode, seconds=1):
    with wait_for_file(path, seconds):
        logger.debug("wait_for_file yielded")
    f = open(path, mode)
    yield f
    f.close()


def wtime_to_minutes(time_string):
    ''' wtime_to_minutes

    Convert standard wallclock time string to minutes.

    Args:
        - Time_string in HH:MM:SS format

    Returns:
        (int) minutes

    '''
    hours, mins, seconds = time_string.split(':')
    total_mins = int(hours) * 60 + int(mins)
    if total_mins < 1:
        logger.warning("Time string '{}' parsed to {} minutes, less than 1".format(time_string, total_mins))
    return total_mins


class RepresentationMixin(object):
    """A mixin class for adding a __repr__ method.

    The __repr__ method will return a string equivalent to the code used to instantiate
    the child class, with any defaults included explicitly. The __max_width__ class variable
    controls the maximum width of the representation string. If this width is exceeded,
    the representation string will be split up, with one argument or keyword argument per line.

    Any arguments or keyword arguments in the constructor must be defined as attributes, or
    an AttributeError will be raised.

    Examples
    --------
    >>> from parsl.utils import RepresentationMixin
    >>> class Foo(RepresentationMixin):
            def __init__(self, first, second, third='three', fourth='fourth'):
                self.first = first
                self.second = second
                self.third = third
                self.fourth = fourth
    >>> bar = Foo(1, 'two', fourth='baz')
    >>> bar
    Foo(1, 'two', third='three', fourth='baz')
    """
    __max_width__ = 80

    def __repr__(self):
        init = self.__init__

        # This test looks for a single layer of wrapping performed by
        # functools.update_wrapper, commonly used in decorators. This will
        # allow RepresentationMixin to see through a single such decorator
        # applied to the __init__ method of a class, and find the underlying
        # arguments. It will not see through multiple layers of such
        # decorators, or cope with other decorators which do not use
        # functools.update_wrapper.

        if hasattr(init, '__wrapped__'):
            init = init.__wrapped__

        argspec = inspect.getfullargspec(init)
        if len(argspec.args) > 1 and argspec.defaults is not None:
            defaults = dict(zip(reversed(argspec.args), reversed(argspec.defaults)))
        else:
            defaults = {}

        for arg in argspec.args[1:]:
            if not hasattr(self, arg):
                template = 'class {} uses {} in the constructor, but does not define it as an attribute'
                raise AttributeError(template.format(self.__class__.__name__, arg))

        if len(defaults) != 0:
            args = [getattr(self, a) for a in argspec.args[1:-len(defaults)]]
        else:
            args = [getattr(self, a) for a in argspec.args[1:]]
        kwargs = {key: getattr(self, key) for key in defaults}

        def assemble_multiline(args, kwargs):
            def indent(text):
                lines = text.splitlines()
                if len(lines) <= 1:
                    return text
                return "\n".join("    " + line for line in lines).strip()
            args = ["\n    {},".format(indent(repr(a))) for a in args]
            kwargs = ["\n    {}={}".format(k, indent(repr(v)))
                      for k, v in sorted(kwargs.items())]

            info = "".join(args) + ", ".join(kwargs)
            return self.__class__.__name__ + "({}\n)".format(info)

        def assemble_line(args, kwargs):
            kwargs = ['{}={}'.format(k, repr(v)) for k, v in sorted(kwargs.items())]

            info = ", ".join([repr(a) for a in args] + kwargs)
            return self.__class__.__name__ + "({})".format(info)

        if len(assemble_line(args, kwargs)) <= self.__class__.__max_width__:
            return assemble_line(args, kwargs)
        else:
            return assemble_multiline(args, kwargs)
