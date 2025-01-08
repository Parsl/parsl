
Bash Apps
---------

.. code-block:: python

       @bash_app
       def echo(
           name: str,
           stdout=parsl.AUTO_LOGNAME  # Requests Parsl to return the stdout
       ):
           return f'echo "Hello, {name}!"'

       future = echo('user')
       future.result() # block until task has completed

       with open(future.stdout, 'r') as f:
           print(f.read())


A Parsl Bash app executes an external application by making a command-line execution.
Parsl will execute the string returned by the function as a command-line script on a remote worker.

Rules for Function Contents
^^^^^^^^^^^^^^^^^^^^^^^^^^^

Bash Apps follow the same rules :ref:`as Python Apps <function-rules>`.
For example, imports may need to be inside functions and global variables will be inaccessible.

Inputs and Outputs
^^^^^^^^^^^^^^^^^^

Bash Apps can use the same kinds of inputs as Python Apps, but only communicate results with Files.

Bash Apps, unlike Python Apps, can also return the content printed to the Standard Output and Error.

Special Keywords Arguments
++++++++++++++++++++++++++

In addition to the ``inputs``, ``outputs``, and ``walltime`` keyword arguments
described above, a Bash app can accept the following keywords:

1. stdout: (string, tuple or ``parsl.AUTO_LOGNAME``) The path to a file to which standard output should be redirected. If set to ``parsl.AUTO_LOGNAME``, the log will be automatically named according to task id and saved under ``task_logs`` in the run directory. If set to a tuple ``(filename, mode)``, standard output will be redirected to the named file, opened with the specified mode as used by the Python `open <https://docs.python.org/3/library/functions.html#open>`_ function.
2. stderr: (string or ``parsl.AUTO_LOGNAME``) Like stdout, but for the standard error stream.
3. label: (string) If the app is invoked with ``stdout=parsl.AUTO_LOGNAME`` or ``stderr=parsl.AUTO_LOGNAME``, this argument will be appended to the log name.

Outputs
+++++++

If the Bash app exits with Unix exit code 0, then the AppFuture will complete. If the Bash app
exits with any other code, Parsl will treat this as a failure, and the AppFuture will instead
contain an `BashExitFailure` exception. The Unix exit code can be accessed through the
``exitcode`` attribute of that `BashExitFailure`.


Execution Options
^^^^^^^^^^^^^^^^^

Bash Apps have the same execution options (e.g., pinning to specific sites) as the Python Apps.

MPI Apps
^^^^^^^^

Applications which employ MPI to span multiple nodes are a special case of Bash apps,
and require special modification of Parsl's `execution environment <../configuration/execution.html>`_ to function.
Support for MPI applications is described `in a later section <mpi_apps.html>`_.
