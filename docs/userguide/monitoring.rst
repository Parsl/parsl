Monitoring
==========

Parsl inculdes two distinct monitoring systems. The first(`Parsl Monitoring`_) captures
task state as well as resource usage over time, while the second (`File Monitoring`_)
monitors the file system for new files and calls functions when specific files are found.

Parsl Monitoring
----------------
The Parsl monitoring system aims to provide detailed
information and diagnostic capabilities to help track the state of your
programs, down to the individual apps that are executed on remote machines.

The monitoring system records information to an SQLite database while a
workflow runs. This information can then be visualised in a web dashboard
using the ``parsl-visualize`` tool, or queried using SQL using regular
SQLite tools.

Monitoring configuration
^^^^^^^^^^^^^^^^^^^^^^^^

Parsl monitoring is only supported with the `parsl.executors.HighThroughputExecutor`.

The following example shows how to enable monitoring in the Parsl
configuration. Here the `parsl.monitoring.MonitoringHub` is specified to use port
55055 to receive monitoring messages from workers every 10 seconds.

.. code-block:: python

   import parsl
   from parsl.monitoring.monitoring import MonitoringHub
   from parsl.config import Config
   from parsl.executors import HighThroughputExecutor
   from parsl.addresses import address_by_hostname

   import logging

   config = Config(
      executors=[
          HighThroughputExecutor(
              label="local_htex",
              cores_per_worker=1,
              max_workers=4,
              address=address_by_hostname(),
          )
      ],
      monitoring=MonitoringHub(
          hub_address=address_by_hostname(),
          hub_port=55055,
          monitoring_debug=False,
          resource_monitoring_interval=10,
      ),
      strategy=None
   )


Visualization
^^^^^^^^^^^^^

To run the web dashboard utility ``parsl-visualize`` you first need to install
its dependencies:

   $ pip install 'parsl[monitoring]'

To view the web dashboard while or after a Parsl program has executed, run
the ``parsl-visualize`` utility::

   $ parsl-visualize

By default, this command expects that the default ``monitoring.db`` database is used
in the runinfo directory. Other databases can be loaded by passing
the database URI on the command line.  For example, if the full path
to the database is ``/tmp/my_monitoring.db``, run::

   $ parsl-visualize sqlite:////tmp/my_monitoring.db

By default, the visualization web server listens on ``127.0.0.1:8080``. If the web server is deployed on a machine with a web browser, the dashboard can be accessed in the browser at ``127.0.0.1:8080``. If the web server is deployed on a remote machine, such as the login node of a cluster, you will need to use an ssh tunnel from your local machine to the cluster::

   $ ssh -L 50000:127.0.0.1:8080 username@cluster_address

This command will bind your local machine's port 50000 to the remote cluster's port 8080.
The dashboard can then be accessed via the local machine's browser at ``127.0.0.1:50000``.

.. warning:: Alternatively you can deploy the visualization server on a public interface. However, first check that this is allowed by the cluster's security policy. The following example shows how to deploy the web server on a public port (i.e., open to Internet via ``public_IP:55555``)::

   $ parsl-visualize --listen 0.0.0.0 --port 55555


Workflows Page
**************

The workflows page lists all Parsl workflows that have been executed with monitoring enabled
with the selected database.
It provides a high level summary of workflow state as shown below:

.. image:: ../images/mon_workflows_page.png

Throughout the dashboard, all blue elements are clickable. For example, clicking a specific worklow
name from the table takes you to the Workflow Summary page described in the next section.

Workflow Summary
****************

The workflow summary page captures the run level details of a workflow, including start and end times
as well as task summary statistics. The workflow summary section is followed by the *App Summary* that lists
the various apps and invocation count for each.

.. image:: ../images/mon_workflow_summary.png


The workflow summary also presents three different views of the workflow:

* Workflow DAG - with apps differentiated by colors: This visualization is useful to visually inspect the dependency
  structure of the workflow. Hovering over the nodes in the DAG shows a tooltip for the app represented by the node and it's task ID.

.. image:: ../images/mon_task_app_grouping.png

* Workflow DAG - with task states differentiated by colors: This visualization is useful to identify what tasks have been completed, failed, or are currently pending.

.. image:: ../images/mon_task_state_grouping.png

* Workflow resource usage: This visualization provides resource usage information at the workflow level.
  For example, cumulative CPU/Memory utilization across workers over time.

.. image:: ../images/mon_resource_summary.png

.. _file-monitor-label:

File Monitoring
---------------

The idea behind File Monitoring is to have a mechanism to provide the status of a job, beyond the typical waiting/running/done
status. Specifically, this is best suited for long running jobs (days, weeks, etc.) that output files periodically, such
as at the end of a timestep. The user specifies what files to look for and a function(s) to call when these files are found.
The `parsl.monitoring.FileMonitor` class provides the interface for specifying the patterns and callbacks.

.. note:: The file monitoring system only works on single machine configurations or on systems where both the Parsl Executor and the worker node use a shared file system.

The file monitoring system will periodically scan the file system (within the given root directory) for any file(s)
of the given file type(s) or regex pattern(s). If any are found they are sent to the user specified callback function(s).
The infrastructure tracks files that have been found previously and only sends newly found files to the callback. The
system also tries to verify that the file(s) are no longer being written to. On shared file systems, there is no direct
mechanism to do this, so it works under the assumption that any file with a modification timestamp that is at least
a specified (by user) seconds old will be considered. If a process is expected to periodically write to a file then
the ``sleep_dur`` parameter of the FileMonitor class should be set to a larger value.

The file monitoring system is given a list of regex style patterns and/or a list of file types to use for searching for
files. For regex style patters they should be strings preceeded by **r** as they are compiled into regex objects by the
file monitoring system (see the `re <https://docs.python.org/3/library/re.html>`_ Python module documentation for
regex specifics). For file type patterns a list of file suffixes should be given. Suffixes with and without an asterisk
are acceptable (e.g. ``pdf``, ``.pdf``, and ``*.pdf`` are all equivalent). File types will have the given ``path`` prepended
to them.

The callback functions have only a few restrictions on them

    #. The function should take a single argument that is a list of the detected files (string, including full path)
    #. The function should return either ``None``, a string like value, or something that can be cast to a string with ``str()``
    #. Any files produced by the function need to be handled by the user (transfer, etc.)

If an email address is provided to the `parsl.monitoring.FileMonitor` instance then the return value of the each callback
function is sent as the body of an email to that address. Currenty, attachments are not supported.

.. note:: Not all systems are capable of sending emails (e.g. those behind high security). These instances will not causes an error with the file monitoring system, but it will log the issue in the Parsl logs.

Either a single callback function can be given that will handle any files found or a list of callback functions, one for
each given pattern. For example::

    filetype = ["pdf", "gif", "jpg"]

    callback = [callback1, callback2, callback3]

If any pdf files are found then they are sent to callback1, any gif files are sent to callback2, and any jpg files
are sent to callback3. In the case that both regex and file types being given the regex expressions will be processed
first, and thus their callbacks should be specified first. When called, the callbacks are launched asynchronously in a
multiprocessing.Pool. The number of concurrently running callbacks can be controlled by the `parsl.monitoring.FileMonitor`
being used.

Examples
^^^^^^^^

With the following given by the user (the callback functions are not defined here):

.. code-block:: python

    from parsl.monitoring import FileMonitor

    fm = FileMonitor(callback = [c1, c2, c3 ,c4],
                     pattern = [r'(?<=-)\d{2}info\.dat', r'results-(\S+)\.txt'],
                     filetype = ["*.gif", "*.jpg"],
                     path = 'images')

and with these files in the system (working directory is ``/scr/run1``

    images/composite.gif
    images/rgb.png
    output/results-psi.txt
    output/results-temperature.txt
    output/results-psi.gif
    output/results-info.dat

will result in the following callback calls

    +----------+-------------------------------+--------------------------------------------------+
    | Callback | Pattern                       | Callback args                                    |
    +==========+===============================+==================================================+
    | c1       | ``r'(?<=-)\d{2}info\.dat'``   | ``['/scr/run1/output/results-info.dat']``        |
    +----------+-------------------------------+--------------------------------------------------+
    | c2       | ``r'results-(\S+)\.txt'``     | ``['/scr/run1/output/results-psi.txt',``         |
    |          |                               | ``'/scr/run1/output/results-temperature.txt']``  |
    +----------+-------------------------------+--------------------------------------------------+
    | c3       | ``*.gif`` -> ``images/*.gif`` | ``['/scr/run1/images/composite.gif']``           |
    +----------+-------------------------------+--------------------------------------------------+
    | c4       | ``*.jpg`` -> ``images/*.jpg`` |                                                  |
    +----------+-------------------------------+--------------------------------------------------+
