import os
import subprocess
import logging

logger = logging.getLogger(__name__)

def execute_wait (cmd, walltime):
    '''  ***DEPRECATED***

    Synchronously execute a commandline string on the shell.
    Args:
         - cmd (string) : Commandline string to execute
         - walltime (int) : walltime in seconds, this is not really used now.

    Returns:
         A tuple of the following:
         retcode : Return code from the execution, -1 on fail
         stdout  : stdout string
         stderr  : stderr string

    Raises:
         None.
    '''
    retcode = -1
    stdout = None
    stderr = None
    try :
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True, preexec_fn=os.setsid)
        proc.wait(timeout=walltime)
        stdout = proc.stdout.read()
        stderr = proc.stderr.read()
        retcode = proc.returncode

    except Exception as e:
        print("Caught exception : {0}".format(e))
        logger.warn("Execution of command [%s] failed due to \n %s ",  (cmd, e))

    return (retcode, stdout.decode("utf-8"), stderr.decode("utf-8"))

def execute_no_wait (cmd, walltime):
    '''  ***DEPRECATED***
    Synchronously execute a commandline string on the shell.
    Args:
         - cmd (string) : Commandline string to execute
         - walltime (int) : walltime in seconds, this is not really used now.

    Returns:
         A tuple of the following:
         retcode : Return code from the execution, -1 on fail
         stdout  : stdout string
         stderr  : stderr string

    Raises:
         None.
    '''
    retcode = -1
    stdout = None
    stderr = None
    try :
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True, preexec_fn=os.setsid)
        pid = proc.pid

    except Exception as e:
        print("Caught exception : {0}".format(e))
        logger.warn("Execution of command [%s] failed due to \n %s ",  (cmd, e))

    return pid, proc

def wtime_to_minutes(time_string):
    ''' wtime_to_minutes

    Convert standard wallclock time string to minutes.

    Args:
        - Time_string in HH:MM:SS format

    Returns:
        (int) minutes

    '''
    hours, mins, seconds = time_string.split(':')
    return int(hours)*60 + int(mins) + 1
