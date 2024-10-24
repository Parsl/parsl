import logging
import os
import subprocess

logger = logging.getLogger(__name__)


def execute_wait(cmd, walltime=None):
    ''' Synchronously execute a commandline string on the shell.

    Args:
        - cmd (string) : Commandline string to execute
        - walltime (int) : walltime in seconds

    Returns:
        - retcode : Return code from the execution
        - stdout  : stdout string
        - stderr  : stderr string
    '''
    try:
        logger.debug("Creating process with command '%s'", cmd)
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            shell=True,
            preexec_fn=os.setpgrp
        )
        logger.debug("Created process with pid %s. Performing communicate", proc.pid)
        (stdout, stderr) = proc.communicate(timeout=walltime)
        retcode = proc.returncode
        logger.debug("Process %s returned %s", proc.pid, proc.returncode)

    except Exception:
        logger.exception(f"Execution of command failed:\n{cmd}")
        raise
    else:
        logger.debug("Execution of command in process %s completed normally", proc.pid)

    return (retcode, stdout.decode("utf-8"), stderr.decode("utf-8"))
