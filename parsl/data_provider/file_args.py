import logging
import os
from typing import Callable

from parsl.app.app import python_app
from parsl.utils import RepresentationMixin
from parsl.data_provider.staging import Staging

logger = logging.getLogger(__name__)


class FileArgsStaging(Staging, RepresentationMixin):
    """A staging provider than can stage in/out local files
    by sending the file alongside the app function call. This
    provider probably only works for small files. It does not
    need a shared file system on the executor side.

    Staging-in happens in two pieces:

    * The task is wrapped with a wrapper that will write out the file
      contents from a shared data structure. That data structure will
      often be serialised across the wire (and so become non-shared),
      but only after an app task is submitted for execution. Up until
      that time, the shared data structure can be updated on the submit
      side by other code.

    * A stage_in method which runs when the input file is ready - after
      any tasks which might produce that output have complete - and
      updates the shared data structure with the contents of that
      input file.

    Staging out needs some similar behaviour, but more awkwardly needs
    the ability to remove the modified return result on the submit side
    afterwards. This is not symmetric with the stage_in way of doing things
    - why? Because we need to run code *after* the app has returned, which
    there isn't a hook for at the moment - the hook for running code
    *before* an app is launched exists as replace_task_stage_in which
    as well as making a hook can run arbitrary code at that point.

    The stage_out hook can do things to do with files but it *can't* do
    things to do with post-processing results: by that time, the result
    has already been handed over to user code via the AppFuture.

    Probably then there should be another hook point here for stageout
    which allows that post-processing to happen.

    """

    def can_stage_in(self, file) -> bool:
        logger.debug("FileArgsStaging checking file {}".format(file.__repr__()))
        logger.debug("file has scheme {}".format(file.scheme))
        return file.scheme == 'file'

    def can_stage_out(self, file) -> bool:
        logger.debug("FileArgsStaging checking file {} for stageout".format(file.__repr__()))
        logger.debug("file has scheme {}".format(file.scheme))
        return file.scheme == 'file'

    # I think there is a race condition here. We read in the file content at the time
    # of setting up the staging - but that might be before a prior task has completed
    # which is going to produce the content we want to stage.
    # There should be a test case which tests this (which can work for any file staging,
    # I think?)
    # Maybe what should happen is that there is a stage_in function which runs at the
    # appropriate point: after any dependent input content has been created, but before
    # the task has been submitted for execution. At that point, we could update some
    # shared object before it is serialised. However, it's a bit awkward in the API at the
    # moment how that memory should be shared? There's nothing in the API that specifically
    # ties a replace_stage and a stage_in together except the File object. So perhaps
    # FileArgsStaging should keep some persistent state which is keyed by that File object?
    # (a bit like workqueue does? it brings up a question about mutable files and how to
    # free that memory when it's no longer needed)
    def replace_task(self, dm, executor, file, func) -> Callable:
        working_dir = dm.dfk.executors[executor].working_dir
        return in_task_transfer_in_wrapper(func, file, working_dir)


    def stage_in(self, dm, executor, file, parent_fut):
    #   # globus_scheme = _get_globus_scheme(dm.dfk, executor)
    #   # stage_in_app = globus_scheme._globus_stage_in_app(executor=executor, dfk=dm.dfk)
    #
        logger.debug("launching stage_in_app")
        # stage_in_app needs to do the read and update
        app_fut = stage_in_app(outputs=[file], staging_inhibit_output=True, parent_fut=parent_fut)
        return app_fut._outputs[0]


    def replace_task_stage_out(self, dm, executor, file, func, unwrap_func) -> Callable:
        working_dir = dm.dfk.executors[executor].working_dir
        return in_task_transfer_out_wrapper(func, unwrap_func, file, working_dir)

# TODO: force to me on data_manager executor
@python_app
def stage_in_app(outputs=[], parent_fut=None, staging_inhibit_output=True):
    file = outputs[0]
    with open(file.path, 'rb') as fh:
        file.content = fh.read()


def in_task_transfer_in_wrapper(func, file, working_dir):
    """Write out the named file before invoking the wrapped function."""
    logger.debug("Wrapping for args-based stagein")

    def wrapper(*args, **kwargs):
        if working_dir:
            os.makedirs(working_dir, exist_ok=True)
            file.local_path = os.path.join(working_dir, file.filename)
        else:
            file.local_path = file.filename
        with open(file.local_path, 'wb') as fh:
            fh.write(file.content)

        result = func(*args, **kwargs)
        return result
    return wrapper


def in_task_transfer_out_wrapper(func, unwrap_func, file, working_dir):
    logger.debug("Wrapping for args-based stageout")
    """Read in the named file before invoking the wrapped function, and
       return its contents to be written out on the submit side.
    """
    def wrapper(*args, **kwargs):
        logger.debug("BENC: in wrapper - setting local_path")
        if working_dir:
            os.makedirs(working_dir, exist_ok=True)
            file.local_path = os.path.join(working_dir, file.filename)
        else:
            file.local_path = file.filename
        logger.debug("BENC: pre-exec local_path is {}".format(file.local_path))
        result = func(*args, **kwargs)
        logger.debug("BENC: post-exec local_path is {}".format(file.local_path))

        with open(file.local_path, 'rb') as fh:
            content = fh.read()

        return (result, content)

    def unwrapper(r):
        (result, content) = r
        with open(file.path, 'wb') as fh:
            fh.write(content)
        return unwrap_func(result) 

    return (wrapper, unwrapper)
