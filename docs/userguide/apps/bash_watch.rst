
Bash Watch
----------

.. code-block:: python

    @bash_watch
    def my_app(
        a: str
    ):
        return f'my_exe {a}'

    outs = DynamicFileList()
    future = my_app(outs, 5, paths='workers')

A Parsl Bash Watch, like the Bash app, executes an external application by making a command-line execution,
with the additional capability of monitoring directories for newly produced files.

Rules for Function Contents
^^^^^^^^^^^^^^^^^^^^^^^^^^^

Bash Watches follow the same rules :ref:`as Python Apps <function-rules>`.
For example, imports may need to be inside functions and global variables will be inaccessible.

Inputs and Outputs
^^^^^^^^^^^^^^^^^^

Bash Watches can use the same kinds of inputs as Python Apps, but only communicate results with Files.

Bash Watches, unlike Python Apps, can also return the content printed to the Standard Output and Error.

Special Keywords Arguments
++++++++++++++++++++++++++

In addition to the ``inputs`` keyword argument described above, a Bash Watch can accept the following keywords:

1. outputs: (:py:class:`parsl.data_provider.dynamic_files.DynamicFileList`) Unlike Python Apps and Bash Apps, this argument is required for the Bash Watch, and must be an instance of :py:class:`parsl.data_provider.dynamic_files.DynamicFileList`.
2. paths: (string or list of strings) The path(s) to monitor for new files being created by the executed command line.
3. stdout: (string, tuple or ``parsl.AUTO_LOGNAME``) The path to a file to which standard output should be redirected. If set to ``parsl.AUTO_LOGNAME``, the log will be automatically named according to task id and saved under ``task_logs`` in the run directory. If set to a tuple ``(filename, mode)``, standard output will be redirected to the named file, opened with the specified mode as used by the Python `open <https://docs.python.org/3/library/functions.html#open>`_ function.
4. stderr: (string or ``parsl.AUTO_LOGNAME``) Like stdout, but for the standard error stream.
5. label: (string) If the app is invoked with ``stdout=parsl.AUTO_LOGNAME`` or ``stderr=parsl.AUTO_LOGNAME``, this argument will be appended to the log name.

Outputs
+++++++

If the Bash Watch exits with Unix exit code 0, then the AppFuture will complete. If the Bash app
exits with any other code, Parsl will treat this as a failure, and the AppFuture will instead
contain an `BashExitFailure` exception. The Unix exit code can be accessed through the
``exitcode`` attribute of that `BashExitFailure`.


Execution Options
^^^^^^^^^^^^^^^^^

Bash Watchers have the same execution options (e.g., pinning to specific sites) as the Python Apps.
