Historical: Changelog
=====================


.. note::
   After Parsl 1.2.0, releases moved to a lighter weight automated model.
   This manual changelog is no longer updated and is now marked as
   historical.

   Change information is delivered as commit messages on individual pull
   requests, which can be see using any relevant git history browser -
   for example, on the web at https://github.com/Parsl/parsl/commits/master/
   or on the commandline using using git log.


Parsl 1.2.0
-----------

Release date: January 13th, 2022.

Parsl v1.2.0 includes 99 pull requests with contributions from:

Ben Clifford @benclifford, Daniel S. Katz @danielskatz, Douglas Thain @dthain, James Corbett @jameshcorbett, Jonas Rübenach @jrueb, Logan Ward @WardLT, Matthew R. Becker @beckermr, Vladimir @vkhodygo, Yadu Nand Babuji @yadudoc, Yo Yehudi @yochannah, Zhuozhao Li @ZhuozhaoLi, yongyanrao @yongyanrao, Tim Jenness @timj, Darko Marinov @darko-marinov, Quentin Le Boulc'h


High Throughput Executor
^^^^^^^^^^^^^^^^^^^^^^^^

* Remove htex self.tasks race condition that shows under high load (#2034)
* Fix htex scale down breakage due to overly aggressive result heartbeat (#2119)  [ TODO: this fixes a bug introduced since 1.1.0 so note that? #2104 ]
* Send heartbeats via results connection (#2104)


Work Queue Executor
^^^^^^^^^^^^^^^^^^^

* Allow use of WorkQueue running_time_min resource constraint (#2113) - WQ recently introduced an additional resource constraint: workers can be aware of their remaining wall time, and tasks can be constrained to only go to workers with sufficient remaining time.
    
* Implement priority as a Work Queue resource specification (#2067) - The allows a workflow script to influence the order in which queued tasks are executed using Work Queue's existing priority mechanism.


* Disable WQ-level retries with an option to re-enable (#2059) - Previously by default, Work Queue will retry tasks that fail at the WQ level (for example, because of worker failure) an infinite number of times, inside the same parsl-level execution try.  That hides the repeated tries from parsl (so monitoring does not report start/end times as might naively be expected for a try, and parsl retry counting does not count).
    
* Document WorkqueueExecutor project_name remote reporting better (#2089)
* wq executor should show itself using representation mixin (#2064)
* Make WorkQueue worker command configurable (#2036)



Flux Executor
^^^^^^^^^^^^^

The new FluxExecutor class uses the Flux resource manager
(github: flux-framework/flux-core) to launch tasks. Each
task is a Flux job.


Condor Provider
^^^^^^^^^^^^^^^

* Fix bug in condor provider for unknown jobs (#2161)
    
LSF Provider
^^^^^^^^^^^^

* Update LSF provider to make it more friendly for different LSF-based computers (#2149)

SLURM Provider
^^^^^^^^^^^^^^

* Improve docs and defaults for slurm partition and account parameters. (#2126)

Grid Engine Provider
^^^^^^^^^^^^^^^^^^^^

* missing queue from self - causes config serialisation failure (#2042)


Monitoring
^^^^^^^^^^

* Index task_hashsum to give cross-run query speedup (#2085)
* Fix monitoring "db locked" errors occuring at scale (#1917)
* Fix worker efficiency plot when tasks are still in progress (#2048)
* Fix use of previously removed reg_time monitoring field (#2020)
* Reorder debug message so it happens when the message is received, without necessarily blocking on the resource_msgs queue put (#2093)


General new features
^^^^^^^^^^^^^^^^^^^^

* Workflow-pluggable retry scoring (#2068) - When a task fails, instead of causing a retry "cost" of 1 (the previous behaviour), this PR allows that cost to be determined by a user specified function which is given some context about the failure.

General bug fixes
^^^^^^^^^^^^^^^^^

* Fix type error when job status output is large. (#2129)
* Fix a race condition in the local channel (#2115)
* Fix incorrect order of manager and interchange versions in error text (#2108)
* Fix to macos multiprocessing spawn and context issues (#2076)
* Tidy tasks_per_node in strategy (#2030)
* Fix and test wrong type handling for joinapp returns (#2063)
* FIX: os independent path (#2043)

Platform and packaging
^^^^^^^^^^^^^^^^^^^^^^

* Improve support for Windows (#2107)
* Reflect python 3.9 support in setup.py metadata (#2023)
* Remove python <3.6 handling from threadpoolexecutor (#2083)
* Remove breaking .[all] install target (#2069)

Internal tidying
^^^^^^^^^^^^^^^^

* Remove ipp logging hack in PR #204 (#2170)
* Remove BadRegistration exception definition which has been unused since PR #1671 (#2142)
* Remove AppFuture.__repr__, because superclass Future repr is sufficient (#2143)
* Make monitoring hub exit condition more explicit (#2131)
* Replace parsl's logging NullHandler with python's own NullHandler (#2114)
* Remove a commented out line of dead code in htex (#2116)
* Abstract more block handling from HighThroughputExecutor and share with WorkQueue (#2071)
* Regularise monitoring RESOURCE_INFO messages (#2117)
* Pull os x multiprocessing code into a single module (#2099)
* Describe monitoring protocols better (#2029)
* Remove task_id param from memo functions, as whole task record is available (#2080)
* remove irrelevant __main__ stub of local provider (#2026)
* remove unused weakref_cb (#2022)
* Remove unneeded task_id param from sanitize_and_wrap (#2081)
* Remove outdated IPP related comment in memoization (#2058)
* Remove unused AppBase status field (#2053)
* Do not unwrap joinapp future exceptions unnecessarily (#2084)
* Eliminate self.tasks[id] calls from joinapp callback (#2015)
* Looking at eliminating passing of task IDs and passing task records instead (#2016)
* Eliminate self.tasks[id] from launch_if_ready
* Eliminate self.tasks[id] calls from launch_task (#2061)
* Eliminate self.tasks[id] from app done callback (#2017)
* Make process_worker_pool pass mypy (#2052)
* Remove unused walltime from LocalProvider (#2057)
* Tidy human readable text/variable names around DependencyError (#2037)
* Replace old string formatting with f-strings in utils.py (#2055)

Documentation, error messages and human-readable text
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* Add a documentation chapter summarizing plugin points (#2066)
* Correct docstring for set_file_logger (#2156)
* Fix typo in two db error messages and make consistent with each other (#2152)
* Update slack join links to currently unexpired link (#2146)
* small typo fix in doc (#2134)
* Update CONTRIBUTING.rst (#2144)
* trying to fix broken link in GitHub (#2133)
* Add CITATION.cff file (#2100)
* Refresh the sanitize_and_wrap docstring (#2086)
* Rephrase ad-hoc config doc now that AdHocProvider (PR #1297) is implemented (#2096)
* Add research notice to readme (#2097)
* Remove untrue claim that parsl_resource_specification keys are case insensitive (#2095)
* Use zsh compatible install syntax (#2009)
* Remove documentation that interchange is walltime aware (#2082)
* Configure sphinx to put in full documentation for each method (#2094)
* autogenerate sphinx stubs rather than requiring manual update each PR (#2087)
* Update docstring for handle_app_update (#2079)
* fix a typo (#2024)
* Switch doc verb from invocated to invoked (#2088)
* Add documentation on meanings of states (#2075)
* Fix summary sentence of ScaleOutException (#2021)
* clarify that max workers is per node (#2056)
* Tidy up slurm state comment (#2035)
* Add nscc singapore example config (#2003)
* better formatting (#2039)
* Add missing f for an f-string (#2062)
* Rework __repr__ and __str__ for OptionalModuleMissing (#2025)
* Make executor bad state exception log use the exception (#2155)

CI/testing
^^^^^^^^^^

* Make changes for CI reliability (#2118)
* Make missing worker test cleanup DFK at end (#2153)
* Tidy bash error codes tests. (#2130)
* Upgrade CI to use recent ubuntu, as old version was deprecated (#2111)
* Remove travis config, replaced by GitHub Actions in PR #2078 (#2112)
* Fix CI broken by dependency package changes (#2105)
* Adding github actions for CI (#2078)
* Test combine() pattern in joinapps (#2054)
* Assert that there should be no doc stubs in version control (#2092)
* Add monitoring dependency to local tests (#2074)
* Put viz test in a script (#2019)
* Reduce the size of recursive fibonacci joinapp testing (#2110)
* Remove disabled midway test (#2028)


Parsl 1.1.0
-----------

Released on April 26th, 2021.

Parsl v1.1.0 includes 59 closed issues and 243 pull requests with contributions (code, tests, reviews and reports) from:

Akila Ravihansa Perera @ravihansa3000, Anna Woodard @annawoodard, @bakerjl, Ben Clifford @benclifford,
Daniel S. Katz @danielskatz, Douglas Thain @dthain, @gerrick, @JG-Quarknet, Joseph Moon @jmoon1506,
Kelly L. Rowland @kellyrowland, Lars Bilke @bilke, Logan Ward @WardLT, Kirill Nagaitsev @Loonride,
Marcus Schwarting @meschw04, Matt Baughman @mattebaughman, Mihael Hategan @hategan, @radiantone,
Rohan Kumar @rohankumar42, Sohit Miglani @sohitmiglani, Tim Shaffer @trshaffer,
Tyler J. Skluzacek @tskluzac, Yadu Nand Babuji @yadudoc, and Zhuozhao Li @ZhuozhaoLi

Deprecated and Removed features
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* Python 3.5 is no longer supported.
* Almost definitely broken Jetstream provider removed (#1821)

New Functionality
^^^^^^^^^^^^^^^^^

* Allow HTEX to set CPU affinity (#1853)

* New serialization system to replace IPP serialization (#1806)

* Support for Python 3.9

* @join_apps are a variation of @python_apps where an app can launch
  more apps and then complete only after the launched apps are also
  completed.

  These are described more fully in docs/userguide/joins.rst

* Monitoring:

  * hub.log is now named monitoring_router.log
  * Remove denormalised workflow duration from monitoring db (#1774)
  * Remove hostname from status table (#1847)
  * Clarify distinction between tasks and tries to run tasks (#1808)
  * Replace 'done' state with 'exec_done' and 'memo_done' (#1848)
  * Use repr instead of str for monitoring fail history (#1966)

* Monitoring visualization:

  * Make task list appear under .../task/ not under .../app/ (#1762)
  * Test that parsl-visualize does not return HTTP errors (#1700)
  * Generate Gantt chart from status table rather than task table timestamps (#1767)
  * Hyperlinks for app page to task pages should be on the task ID, not the app name (#1776)
  * Use real final state to color DAG visualization (#1812)

* Make task record garbage collection optional. (#1909)

* Make checkpoint_files = get_all_checkpoints() by default (#1918)


Parsl 1.0.0
-----------

Released on June 11th, 2020

Parsl v1.0.0 includes 59 closed issues and 243 pull requests with contributions (code, tests, reviews and reports) from:

Akila Ravihansa Perera @ravihansa3000, Aymen Alsaadi @AymenFJA, Anna Woodard @annawoodard,
Ben Clifford @benclifford, Ben Glick @benhg, Benjamin Tovar @btovar, Daniel S. Katz @danielskatz,
Daniel Smith @dgasmith, Douglas Thain @dthain, Eric Jonas @ericmjonas, Geoffrey Lentner @glentner,
Ian Foster @ianfoster, Kalpani Ranasinghe @kalpanibhagya, Kyle Chard @kylechard, Lindsey Gray @lgray,
Logan Ward @WardLT, Lyle Hayhurst @lhayhurst, Mihael Hategan @hategan, Rajini Wijayawardana @rajiniw95,
@saktar-unr, Tim Shaffer @trshaffer, Tom Glanzman @TomGlanzman, Yadu Nand Babuji @yadudoc and,
Zhuozhao Li @ZhuozhaoLi

Deprecated and Removed features
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Python3.5** is now marked for deprecation, and will not be supported after this release.
  Python3.6 will be the earliest Python3 version supported in the next release.

* **App** decorator deprecated in 0.8 is now removed `issue#1539 <https://github.com/Parsl/parsl/issues/1539>`_
  `bash_app` and `python_app` are the only supported App decorators in this release.

* **IPyParallelExecutor** is no longer a supported executor `issue#1565 <https://github.com/Parsl/parsl/issues/1565>`_


New Functionality
^^^^^^^^^^^^^^^^^

* `parsl.executors.WorkQueueExecutor` introduced in v0.9.0 is now in beta. `parsl.executors.WorkQueueExecutor` is designed as a drop-in replacement for `parsl.executors.HighThroughputExecutor`. Here are some key features:

  * Support for packaging the python environment and shipping it to the worker side. This mechanism addresses propagating python environments in  grid-like systems that lack shared-filesystems or cloud environments.
  * `parsl.executors.WorkQueueExecutor` supports resource function tagging and resource specification
  * Support for resource specification kwarg `issue#1675 <https://github.com/Parsl/parsl/issues/1675>`_


* Limited type-checking in Parsl internal components (as part of an ongoing effort)


* Improvements to caching mechanism including ability to mark certain arguments to be  not counted for memoization.

  * Normalize known types for memoization, and reject unknown types (#1291). This means that previous unreliable
    behaviour for some complex types such as dicts will become more reliable; and that other previous unreliable
    behaviour for other unknown complex types will now cause an error. Handling can be added for those types using
    parsl.memoization.id_for_memo.
  * Add ability to label some arguments in an app invocation as not memoized using the ignore_for_cache app keyword (PR 1568)

* Special keyword args: 'inputs', 'outputs' that are used to specify files no longer support strings
  and now require `File` objects. For example, the following snippet is no longer supported in v1.0.0:

   .. code-block:: python

      @bash_app
      def cat(inputs=(), outputs=()):
           return 'cat {} > {}'.format(inputs[0], outputs[0])

      concat = cat(inputs=['hello-0.txt'],
                   outputs=['hello-1.txt'])

   This is the new syntax:

   .. code-block:: python

      from parsl import File

      @bash_app
      def cat(inputs=(), outputs=()):
           return 'cat {} > {}'.format(inputs[0].filepath, outputs[0].filepath)

      concat = cat(inputs=[File('hello-0.txt')],
                   outputs=[File('hello-1.txt')])

   Since filenames are no longer passed to apps as strings, and the string filepath is required, it can
   be accessed from the File object using the ``filepath`` attribute.

   .. code-block:: python

      from parsl import File

      @bash_app
      def cat(inputs=(), outputs=()):
           return 'cat {} > {}'.format(inputs[0].filepath, outputs[0].filepath)


* New launcher: `parsl.launchers.WrappedLauncher` for launching tasks inside containers.

* `parsl.channels.SSHChannel` now supports a ``key_filename`` kwarg `issue#1639 <https://github.com/Parsl/parsl/issues/1639>`_

* Newly added Makefile wraps several frequent developer operations such as:

  * Run the test-suite: ``make test``

  * Install parsl: ``make install``

  * Create a virtualenv: ``make virtualenv``

  * Tag release and push to release channels: ``make deploy``

* Several updates to the `parsl.executors.HighThroughputExecutor`:

  * By default, the `parsl.executors.HighThroughputExecutor` will now use heuristics to detect and try all addresses
    when the workers connect back to the parsl master. An address can be configured manually using the
    ``HighThroughputExecutor(address=<address_string>)`` kwarg option.

  * Support for Mac OS. (`pull#1469 <https://github.com/Parsl/parsl/pull/1469>`_, `pull#1738 <https://github.com/Parsl/parsl/pull/1738>`_)

  * Cleaner reporting of version mismatches and automatic suppression of non-critical errors.

  * Separate worker log directories by block id `issue#1508 <https://github.com/Parsl/parsl/issues/1508>`_

* Support for garbage collection to limit memory consumption in long-lived scripts.

* All cluster providers now use ``max_blocks=1`` by default `issue#1730 <https://github.com/Parsl/parsl/issues/1730>`_ to avoid over-provisioning.

* New ``JobStatus`` class for better monitoring of Jobs submitted to batch schedulers.

Bug Fixes
^^^^^^^^^

* Ignore AUTO_LOGNAME for caching `issue#1642 <https://github.com/Parsl/parsl/issues/1642>`_
* Add batch jobs to PBS/torque job status table `issue#1650 <https://github.com/Parsl/parsl/issues/1650>`_
* Use higher default buffer threshold for serialization `issue#1654 <https://github.com/Parsl/parsl/issues/1654>`_
* Do not pass mutable default to ignore_for_cache `issue#1656 <https://github.com/Parsl/parsl/issues/1656>`_
* Several improvements and fixes to Monitoring
* Fix sites/test_ec2 failure when aws user opts specified `issue#1375 <https://github.com/Parsl/parsl/issues/1375>`_
* Fix LocalProvider to kill the right processes, rather than all processes owned by user `issue#1447 <https://github.com/Parsl/parsl/issues/1447>`_
* Exit htex probe loop with first working address `issue#1479 <https://github.com/Parsl/parsl/issues/1479>`_
* Allow slurm partition to be optional `issue#1501 <https://github.com/Parsl/parsl/issues/1501>`_
* Fix race condition with wait_for_tasks vs task completion `issue#1607 <https://github.com/Parsl/parsl/issues/1607>`_
* Fix Torque job_id truncation `issue#1583 <https://github.com/Parsl/parsl/issues/1583>`_
* Cleaner reporting for Serialization Errors `issue#1355 <https://github.com/Parsl/parsl/issues/1355>`_
* Results from zombie managers do not crash the system, but will be ignored `issue#1665 <https://github.com/Parsl/parsl/issues/1665>`_
* Guarantee monitoring will send out at least one message `issue#1446 <https://github.com/Parsl/parsl/issues/1446>`_
* Fix monitoring ctrlc hang `issue#1670 <https://github.com/Parsl/parsl/issues/1670>`_


Parsl 0.9.0
-----------

Released on October 25th, 2019

Parsl v0.9.0 includes 199 closed issues and pull requests with contributions (code, tests, reviews and reports) from:

Andrew Litteken @AndrewLitteken, Anna Woodard @annawoodard, Ben Clifford @benclifford,
Ben Glick @benhg, Daniel S. Katz @danielskatz, Daniel Smith @dgasmith,
Engin Arslan @earslan58, Geoffrey Lentner @glentner, John Hover @jhover
Kyle Chard @kylechard, TJ Dasso @tjdasso, Ted Summer @macintoshpie,
Tom Glanzman @TomGlanzman, Levi Naden @LNaden, Logan Ward @WardLT, Matthew Welborn @mattwelborn,
@MatthewBM, Raphael Fialho @rapguit, Yadu Nand Babuji @yadudoc, and Zhuozhao Li @ZhuozhaoLi


New Functionality
^^^^^^^^^^^^^^^^^

* Parsl will no longer do automatic keyword substitution in ``@bash_app`` in favor of deferring to Python's `format method <https://docs.python.org/3.1/library/stdtypes.html#str.format>`_
  and newer `f-strings <https://www.python.org/dev/peps/pep-0498/>`_. For example,

     .. code-block:: python

        # The following example worked until v0.8.0
        @bash_app
        def cat(inputs=(), outputs=()):
            return 'cat {inputs[0]} > {outputs[0]}' # <-- Relies on Parsl auto formatting the string

        # Following are two mechanisms that will work going forward from v0.9.0
        @bash_app
        def cat(inputs=(), outputs=()):
            return 'cat {} > {}'.format(inputs[0], outputs[0]) # <-- Use str.format method

        @bash_app
        def cat(inputs=(), outputs=()):
            return f'cat {inputs[0]} > {outputs[0]}' # <-- OR use f-strings introduced in Python3.6


* ``@python_app`` now takes a ``walltime`` kwarg to limit the task execution time.
* New file staging API `parsl.data_provider.staging.Staging` to support pluggable
  file staging methods. The methods implemented in 0.8.0 (HTTP(S), FTP and
  Globus) are still present, along with two new methods which perform HTTP(S)
  and FTP staging on worker nodes to support non-shared-filesystem executors
  such as clouds.
* Behaviour change for storage_access parameter. In 0.8.0, this was used to
  specify Globus staging configuration. In 0.9.0, if this parameter is
  specified it must specify all desired staging providers. To keep the same
  staging providers as in 0.8.0, specify:

    .. code-block:: python

      from parsl.data_provider.data_manager import default_staging
      storage_access = default_staging + [GlobusStaging(...)]

  ``GlobusScheme`` in 0.8.0 has been renamed `GlobusStaging` and moved to a new
  module, parsl.data_provider.globus

* `parsl.executors.WorkQueueExecutor`: a new executor that integrates functionality from `Work Queue <http://ccl.cse.nd.edu/software/workqueue/>`_ is now available.
* New provider to support for Ad-Hoc clusters `parsl.providers.AdHocProvider`
* New provider added to support LSF on Summit `parsl.providers.LSFProvider`
* Support for CPU and Memory resource hints to providers `(github) <https://github.com/Parsl/parsl/issues/942>`_.
* The ``logging_level=logging.INFO`` in `parsl.monitoring.MonitoringHub` is replaced with ``monitoring_debug=False``:

   .. code-block:: python

      monitoring=MonitoringHub(
                   hub_address=address_by_hostname(),
                   hub_port=55055,
                   monitoring_debug=False,
                   resource_monitoring_interval=10,
      ),

* Managers now have a worker watchdog thread to report task failures that crash a worker.
* Maximum idletime after which idle blocks can be relinquished can now be configured as follows:

    .. code-block:: python

       config=Config(
                    max_idletime=120.0 ,  # float, unit=seconds
                    strategy='simple'
       )

* Several test-suite improvements that have dramatically reduced test duration.
* Several improvements to the Monitoring interface.
* Configurable port on `parsl.channels.SSHChannel`.
* ``suppress_failure`` now defaults to True.
* `parsl.executors.HighThroughputExecutor` is the recommended executor, and ``IPyParallelExecutor`` is deprecated.
* `parsl.executors.HighThroughputExecutor` will expose worker information via environment variables: ``PARSL_WORKER_RANK`` and ``PARSL_WORKER_COUNT``

Bug Fixes
^^^^^^^^^

* ZMQError: Operation cannot be accomplished in current state bug `issue#1146 <https://github.com/Parsl/parsl/issues/1146>`_
* Fix event loop error with monitoring enabled `issue#532 <https://github.com/Parsl/parsl/issues/532>`_
* Tasks per app graph appears as a sawtooth, not as rectangles `issue#1032 <https://github.com/Parsl/parsl/issues/1032>`_.
* Globus status processing failure `issue#1317 <https://github.com/Parsl/parsl/issues/1317>`_.
* Sporadic globus staging error `issue#1170 <https://github.com/Parsl/parsl/issues/1170>`_.
* RepresentationMixin breaks on classes with no default parameters `issue#1124 <https://github.com/Parsl/parsl/issues/1124>`_.
* File ``localpath`` staging conflict `issue#1197 <https://github.com/Parsl/parsl/issues/1197>`_.
* Fix IndexError when using CondorProvider with strategy enabled `issue#1298 <https://github.com/Parsl/parsl/issues/1298>`_.
* Improper dependency error handling causes hang `issue#1285 <https://github.com/Parsl/parsl/issues/1285>`_.
* Memoization/checkpointing fixes for bash apps `issue#1269 <https://github.com/Parsl/parsl/issues/1269>`_.
* CPU User Time plot is strangely cumulative `issue#1033 <https://github.com/Parsl/parsl/issues/1033>`_.
* Issue requesting resources on non-exclusive nodes `issue#1246 <https://github.com/Parsl/parsl/issues/1246>`_.
* parsl + htex + slurm hangs if slurm command times out, without making further progress `issue#1241 <https://github.com/Parsl/parsl/issues/1241>`_.
* Fix strategy overallocations `issue#704 <https://github.com/Parsl/parsl/issues/704>`_.
* max_blocks not respected in SlurmProvider `issue#868 <https://github.com/Parsl/parsl/issues/868>`_.
* globus staging does not work with a non-default ``workdir`` `issue#784 <https://github.com/Parsl/parsl/issues/784>`_.
* Cumulative CPU time loses time when subprocesses end `issue#1108 <https://github.com/Parsl/parsl/issues/1108>`_.
* Interchange KeyError due to too many heartbeat missed `issue#1128 <https://github.com/Parsl/parsl/issues/1128>`_.



Parsl 0.8.0
-----------

Released on June 13th, 2019

Parsl v0.8.0 includes 58 closed issues and pull requests with contributions (code, tests, reviews and reports)

from: Andrew Litteken @AndrewLitteken, Anna Woodard @annawoodard, Antonio Villarreal @villarrealas,
Ben Clifford @benc, Daniel S. Katz @danielskatz, Eric Tatara @etatara, Juan David Garrido @garri1105,
Kyle Chard @@kylechard, Lindsey Gray @lgray, Tim Armstrong @timarmstrong, Tom Glanzman @TomGlanzman,
Yadu Nand Babuji @yadudoc, and Zhuozhao Li @ZhuozhaoLi


New Functionality
^^^^^^^^^^^^^^^^^

* Monitoring is now integrated into parsl as default functionality.
* ``parsl.AUTO_LOGNAME``: Support for a special ``AUTO_LOGNAME`` option to auto generate ``stdout`` and ``stderr`` file paths.
* `File` no longer behaves as a string. This means that operations in apps that treated a `File` as  a string
  will break. For example the following snippet will have to be updated:

  .. code-block:: python

     # Old style: " ".join(inputs) is legal since inputs will behave like a list of strings
     @bash_app
     def concat(inputs=(), outputs=(), stdout="stdout.txt", stderr='stderr.txt'):
         return "cat {0} > {1}".format(" ".join(inputs), outputs[0])

     # New style:
     @bash_app
     def concat(inputs=(), outputs=(), stdout="stdout.txt", stderr='stderr.txt'):
         return "cat {0} > {1}".format(" ".join(list(map(str,inputs))), outputs[0])

* Cleaner user app file log management.
* Updated configurations using `parsl.executors.HighThroughputExecutor` in the configuration section of the userguide.
* Support for OAuth based SSH with `parsl.channels.OAuthSSHChannel`.

Bug Fixes
^^^^^^^^^

* Monitoring resource usage bug `issue#975 <https://github.com/Parsl/parsl/issues/975>`_
* Bash apps fail due to missing dir paths `issue#1001 <https://github.com/Parsl/parsl/issues/1001>`_
* Viz server explicit binding fix `issue#1023 <https://github.com/Parsl/parsl/issues/1023>`_
* Fix sqlalchemy version warning `issue#997 <https://github.com/Parsl/parsl/issues/997>`_
* All workflows are called typeguard `issue#973 <https://github.com/Parsl/parsl/issues/973>`_
* Fix ``ModuleNotFoundError: No module named 'monitoring'`` `issue#971 <https://github.com/Parsl/parsl/issues/971>`_
* Fix sqlite3 integrity error `issue#920 <https://github.com/Parsl/parsl/issues/920>`_
* HTEX interchange check python version mismatch to the micro level `issue#857 <https://github.com/Parsl/parsl/issues/857>`_
* Clarify warning message when a manager goes missing `issue#698 <https://github.com/Parsl/parsl/issues/698>`_
* Apps without a specified DFK should use the global DFK in scope at call time, not at other times. `issue#697 <https://github.com/Parsl/parsl/issues/697>`_


Parsl 0.7.2
-----------

Released on Mar 14th, 2019

New Functionality
^^^^^^^^^^^^^^^^^

* Monitoring: Support for reporting monitoring data to a local sqlite database is now available.
* Parsl is switching to an opt-in model for anonymous usage tracking. Read more here: :ref:`label-usage-tracking`.
* `bash_app` now supports specification of write modes for ``stdout`` and ``stderr``.
* Persistent volume support added to `parsl.providers.KubernetesProvider`.
* Scaling recommendations from study on Bluewaters is now available in the userguide.


Parsl 0.7.1
-----------

Released on Jan 18th, 2019

New Functionality
^^^^^^^^^^^^^^^^^

* parsl.executors.LowLatencyExecutor: a new executor designed to address use-cases with tight latency requirements
  such as model serving (Machine Learning), function serving and interactive analyses is now available.
* New options in `parsl.executors.HighThroughputExecutor`:
     * ``suppress_failure``: Enable suppression of worker rejoin errors.
     * ``max_workers``: Limit workers spawned by manager
* Late binding of DFK, allows apps to pick DFK dynamically at call time. This functionality adds safety
  to cases where a new config is loaded and a new DFK is created.

Bug fixes
^^^^^^^^^

* A critical bug in `parsl.executors.HighThroughputExecutor` that led to debug logs overflowing channels and terminating
  blocks of resource is fixed `issue#738 <https://github.com/Parsl/parsl/issues/738>`_


Parsl 0.7.0
-----------

Released on Dec 20st, 2018

Parsl v0.7.0 includes 110 closed issues with contributions (code, tests, reviews and reports)
from: Alex Hays @ahayschi, Anna Woodard @annawoodard, Ben Clifford @benc, Connor Pigg @ConnorPigg,
David Heise @daheise, Daniel S. Katz @danielskatz, Dominic Fitzgerald @djf604, Francois Lanusse @EiffL,
Juan David Garrido @garri1105, Gordon Watts @gordonwatts, Justin Wozniak @jmjwozniak,
Joseph Moon @jmoon1506, Kenyi Hurtado @khurtado, Kyle Chard @kylechard, Lukasz Lacinski @lukaszlacinski,
Ravi Madduri @madduri, Marco Govoni @mgovoni-devel, Reid McIlroy-Young @reidmcy, Ryan Chard @ryanchard,
@sdustrud, Yadu Nand Babuji @yadudoc, and Zhuozhao Li @ZhuozhaoLi

New functionality
^^^^^^^^^^^^^^^^^


* `parsl.executors.HighThroughputExecutor`: a new executor intended to replace the ``IPyParallelExecutor`` is now available.
  This new executor addresses several limitations of ``IPyParallelExecutor`` such as:

  * Scale beyond the ~300 worker limitation of IPP.
  * Multi-processing manager supports execution on all cores of a single node.
  * Improved worker side reporting of version, system and status info.
  * Supports failure detection and cleaner manager shutdown.

  Here's a sample configuration for using this executor locally:

   .. code-block:: python

        from parsl.providers import LocalProvider
        from parsl.channels import LocalChannel

        from parsl.config import Config
        from parsl.executors import HighThroughputExecutor

        config = Config(
            executors=[
                HighThroughputExecutor(
                    label="htex_local",
                    cores_per_worker=1,
                    provider=LocalProvider(
                        channel=LocalChannel(),
                        init_blocks=1,
                        max_blocks=1,
                    ),
                )
            ],
        )

   More information on configuring is available in the :ref:`configuration-section` section.

* ExtremeScaleExecutor - a new executor targeting supercomputer scale (>1000 nodes) workflows is now available.

  Here's a sample configuration for using this executor locally:

   .. code-block:: python

        from parsl.providers import LocalProvider
        from parsl.channels import LocalChannel
        from parsl.launchers import SimpleLauncher

        from parsl.config import Config
        from parsl.executors import ExtremeScaleExecutor

        config = Config(
            executors=[
                ExtremeScaleExecutor(
                    label="extreme_local",
                    ranks_per_node=4,
                    provider=LocalProvider(
                        channel=LocalChannel(),
                        init_blocks=0,
                        max_blocks=1,
                        launcher=SimpleLauncher(),
                    )
                )
            ],
            strategy=None,
        )

  More information on configuring is available in the :ref:`configuration-section` section.


* The libsubmit repository has been merged with Parsl to reduce overheads on maintenance with respect to documentation,
  testing, and release synchronization. Since the merge, the API has undergone several updates to support
  the growing collection of executors, and as a result Parsl 0.7.0+ will not be backwards compatible with
  the standalone libsubmit repos. The major components of libsubmit are now available through Parsl, and
  require the following changes to import lines to migrate scripts to 0.7.0:

    * ``from libsubmit.providers import <ProviderName>``  is now ``from parsl.providers import <ProviderName>``
    * ``from libsubmit.channels import <ChannelName>``  is now ``from parsl.channels import <ChannelName>``
    * ``from libsubmit.launchers import <LauncherName>``  is now ``from parsl.launchers import <LauncherName>``


    .. warning::
       This is a breaking change from Parsl v0.6.0

* To support resource-based requests for workers and to maintain uniformity across interfaces, ``tasks_per_node`` is
  no longer a **provider** option. Instead, the notion of ``tasks_per_node`` is defined via executor specific options,
  for eg:

    * ``IPyParallelExecutor`` provides ``workers_per_node``
    * `parsl.executors.HighThroughputExecutor` provides ``cores_per_worker`` to allow for worker launches to be determined based on
      the number of cores on the compute node.
    * ExtremeScaleExecutor uses ``ranks_per_node`` to specify the ranks to launch per node.

    .. warning::
       This is a breaking change from Parsl v0.6.0


* Major upgrades to the monitoring infrastructure.
    * Monitoring information can now be written to a SQLite database, created on the fly by Parsl
    * Web-based monitoring to track workflow progress


* Determining the correct IP address/interface given network firewall rules is often a nuisance.
  To simplify this, three new methods are now supported:

    * ``parsl.addresses.address_by_route``
    * ``parsl.addresses.address_by_query``
    * ``parsl.addresses.address_by_hostname``

* `parsl.launchers.AprunLauncher` now supports ``overrides`` option that allows arbitrary strings to be added
  to the aprun launcher call.

* `DataFlowKernel` has a new method ``wait_for_current_tasks()``

* `DataFlowKernel` now uses per-task locks and an improved mechanism to handle task completions
  improving performance for workflows with large number of tasks.


Bug fixes (highlights)
^^^^^^^^^^^^^^^^^^^^^^


* Ctlr+C should cause fast DFK cleanup `issue#641 <https://github.com/Parsl/parsl/issues/641>`_
* Fix to avoid padding in ``wtime_to_minutes()`` `issue#522 <https://github.com/Parsl/parsl/issues/522>`_
* Updates to block semantics `issue#557 <https://github.com/Parsl/parsl/issues/557>`_
* Updates ``public_ip`` to ``address`` for clarity `issue#557 <https://github.com/Parsl/parsl/issues/557>`_
* Improvements to launcher docs `issue#424 <https://github.com/Parsl/parsl/issues/424>`_
* Fixes for inconsistencies between stream_logger and file_logger `issue#629 <https://github.com/Parsl/parsl/issues/629>`_
* Fixes to DFK discarding some un-executed tasks at end of workflow `issue#222 <https://github.com/Parsl/parsl/issues/222>`_
* Implement per-task locks to avoid deadlocks `issue#591 <https://github.com/Parsl/parsl/issues/591>`_
* Fixes to internal consistency errors `issue#604 <https://github.com/Parsl/parsl/issues/604>`_
* Removed unnecessary provider labels `issue#440 <https://github.com/Parsl/parsl/issues/440>`_
* Fixes to `parsl.providers.TorqueProvider` to work on NSCC `issue#489 <https://github.com/Parsl/parsl/issues/489>`_
* Several fixes and updates to monitoring subsystem `issue#471 <https://github.com/Parsl/parsl/issues/471>`_
* DataManager calls wrong DFK `issue#412 <https://github.com/Parsl/parsl/issues/412>`_
* Config isn't reloading properly in notebooks `issue#549 <https://github.com/Parsl/parsl/issues/549>`_
* Cobalt provider ``partition`` should be ``queue`` `issue#353 <https://github.com/Parsl/parsl/issues/353>`_
* bash AppFailure exceptions contain useful but un-displayed information `issue#384 <https://github.com/Parsl/parsl/issues/384>`_
* Do not CD to engine_dir `issue#543 <https://github.com/Parsl/parsl/issues/543>`_
* Parsl install fails without kubernetes config file `issue#527 <https://github.com/Parsl/parsl/issues/527>`_
* Fix import error `issue#533  <https://github.com/Parsl/parsl/issues/533>`_
* Change Local Database Strategy from Many Writers to a Single Writer `issue#472 <https://github.com/Parsl/parsl/issues/472>`_
* All run-related working files should go in the rundir unless otherwise configured `issue#457 <https://github.com/Parsl/parsl/issues/457>`_
* Fix concurrency issue with many engines accessing the same IPP config `issue#469 <https://github.com/Parsl/parsl/issues/469>`_
* Ensure we are not caching failed tasks `issue#368 <https://github.com/Parsl/parsl/issues/368>`_
* File staging of unknown schemes fails silently `issue#382 <https://github.com/Parsl/parsl/issues/382>`_
* Inform user checkpointed results are being used `issue#494 <https://github.com/Parsl/parsl/issues/494>`_
* Fix IPP + python 3.5 failure `issue#490 <https://github.com/Parsl/parsl/issues/490>`_
* File creation fails if no executor has been loaded `issue#482 <https://github.com/Parsl/parsl/issues/482>`_
* Make sure tasks in ``dep_fail`` state are retried `issue#473 <https://github.com/Parsl/parsl/issues/473>`_
* Hard requirement for CMRESHandler `issue#422 <https://github.com/Parsl/parsl/issues/422>`_
* Log error Globus events to stderr `issue#436 <https://github.com/Parsl/parsl/issues/436>`_
* Take 'slots' out of logging `issue#411 <https://github.com/Parsl/parsl/issues/411>`_
* Remove redundant logging `issue#267 <https://github.com/Parsl/parsl/issues/267>`_
* Zombie ipcontroller processes - Process cleanup in case of interruption `issue#460 <https://github.com/Parsl/parsl/issues/460>`_
* IPyparallel failure when submitting several apps in parallel threads `issue#451 <https://github.com/Parsl/parsl/issues/451>`_
* `parsl.providers.SlurmProvider` + `parsl.launchers.SingleNodeLauncher` starts all engines on a single core `issue#454 <https://github.com/Parsl/parsl/issues/454>`_
* IPP ``engine_dir`` has no effect if indicated dir does not exist `issue#446 <https://github.com/Parsl/parsl/issues/446>`_
* Clarify AppBadFormatting error `issue#433 <https://github.com/Parsl/parsl/issues/433>`_
* confusing error message with simple configs `issue#379 <https://github.com/Parsl/parsl/issues/379>`_
* Error due to missing kubernetes config file `issue#432 <https://github.com/Parsl/parsl/issues/432>`_
* ``parsl.configs`` and ``parsl.tests.configs`` missing init files `issue#409 <https://github.com/Parsl/parsl/issues/409>`_
* Error when Python versions differ `issue#62 <https://github.com/Parsl/parsl/issues/62>`_
* Fixing ManagerLost error in HTEX/EXEX `issue#577 <https://github.com/Parsl/parsl/issues/577>`_
* Write all debug logs to rundir by default in HTEX/EXEX `issue#574 <https://github.com/Parsl/parsl/issues/574>`_
* Write one log per HTEX worker `issue#572 <https://github.com/Parsl/parsl/issues/572>`_
* Fixing ManagerLost error in HTEX/EXEX `issue#577 <https://github.com/Parsl/parsl/issues/577>`_


Parsl 0.6.1
-----------

Released on July 23rd, 2018.

This point release contains fixes for `issue#409 <https://github.com/Parsl/parsl/issues/409>`_


Parsl 0.6.0
-----------

Released July 23rd, 2018.

New functionality
^^^^^^^^^^^^^^^^^

* Switch to class based configuration `issue#133 <https://github.com/Parsl/parsl/issues/133>`_

  Here's a the config for using threads for local execution

  .. code-block:: python

    from parsl.config import Config
    from parsl.executors.threads import ThreadPoolExecutor

    config = Config(executors=[ThreadPoolExecutor()])

  Here's a more complex config that uses SSH to run on a Slurm based cluster

  .. code-block:: python

    from libsubmit.channels import SSHChannel
    from libsubmit.providers import SlurmProvider

    from parsl.config import Config
    from parsl.executors.ipp import IPyParallelExecutor
    from parsl.executors.ipp_controller import Controller

    config = Config(
        executors=[
            IPyParallelExecutor(
                provider=SlurmProvider(
                    'westmere',
                    channel=SSHChannel(
                        hostname='swift.rcc.uchicago.edu',
                        username=<USERNAME>,
                        script_dir=<SCRIPTDIR>
                    ),
                    init_blocks=1,
                    min_blocks=1,
                    max_blocks=2,
                    nodes_per_block=1,
                    tasks_per_node=4,
                    parallelism=0.5,
                    overrides=<SPECIFY_INSTRUCTIONS_TO_LOAD_PYTHON3>
                ),
                label='midway_ipp',
                controller=Controller(public_ip=<PUBLIC_IP>),
            )
        ]
    )

* Implicit Data Staging `issue#281 <https://github.com/Parsl/parsl/issues/281>`_

  .. code-block:: python

    # create an remote Parsl file
    inp = File('ftp://www.iana.org/pub/mirror/rirstats/arin/ARIN-STATS-FORMAT-CHANGE.txt')

    # create a local Parsl file
    out = File('file:///tmp/ARIN-STATS-FORMAT-CHANGE.txt')

    # call the convert app with the Parsl file
    f = convert(inputs=[inp], outputs=[out])
    f.result()

* Support for application profiling `issue#5 <https://github.com/Parsl/parsl/issues/5>`_

* Real-time usage tracking via external systems `issue#248 <https://github.com/Parsl/parsl/issues/248>`_, `issue#251 <https://github.com/Parsl/parsl/issues/251>`_

* Several fixes and upgrades to tests and testing infrastructure `issue#157 <https://github.com/Parsl/parsl/issues/157>`_, `issue#159 <https://github.com/Parsl/parsl/issues/159>`_,
  `issue#128 <https://github.com/Parsl/parsl/issues/128>`_, `issue#192 <https://github.com/Parsl/parsl/issues/192>`_,
  `issue#196 <https://github.com/Parsl/parsl/issues/196>`_

* Better state reporting in logs `issue#242 <https://github.com/Parsl/parsl/issues/242>`_

* Hide DFK `issue#50 <https://github.com/Parsl/parsl/issues/50>`_

  * Instead of passing a config dictionary to the DataFlowKernel, now you can call ``parsl.load(Config)``
  * Instead of having to specify the ``dfk`` at the time of ``App`` declaration, the DFK is a singleton loaded
    at call time :

    .. code-block:: python

        import parsl
        from parsl.tests.configs.local_ipp import config
        parsl.load(config)

        @App('python')
        def double(x):
            return x * 2

        fut = double(5)
        fut.result()

* Support for better reporting of remote side exceptions `issue#110 <https://github.com/Parsl/parsl/issues/110>`_


Bug Fixes
^^^^^^^^^

* Making naming conventions consistent `issue#109 <https://github.com/Parsl/parsl/issues/109>`_

* Globus staging returns unclear error bug `issue#178 <https://github.com/Parsl/parsl/issues/178>`_

* Duplicate log-lines when using IPP `issue#204 <https://github.com/Parsl/parsl/issues/204>`_

* Usage tracking with certain missing network causes 20s startup delay. `issue#220 <https://github.com/Parsl/parsl/issues/220>`_

* ``task_exit`` checkpointing repeatedly truncates checkpoint file during run bug `issue#230 <https://github.com/Parsl/parsl/issues/230>`_

* Checkpoints will not reload from a run that was Ctrl-C'ed `issue#232 <https://github.com/Parsl/parsl/issues/232>`_

* Race condition in task checkpointing `issue#234 <https://github.com/Parsl/parsl/issues/234>`_

* Failures not to be checkpointed `issue#239 <https://github.com/Parsl/parsl/issues/239>`_

* Naming inconsitencies with ``maxThreads``, ``max_threads``, ``max_workers`` are now resolved `issue#303 <https://github.com/Parsl/parsl/issues/303>`_

* Fatal not a git repository alerts `issue#326 <https://github.com/Parsl/parsl/issues/326>`_

* Default ``kwargs`` in bash apps unavailable at command-line string format time `issue#349 <https://github.com/Parsl/parsl/issues/349>`_

* Fix launcher class inconsistencies `issue#360 <https://github.com/Parsl/parsl/issues/360>`_

* Several fixes to AWS provider `issue#362 <https://github.com/Parsl/parsl/issues/362>`_
     * Fixes faulty status updates
     * Faulty termination of instance at cleanup, leaving zombie nodes.


Parsl 0.5.1
-----------

Released. May 15th, 2018.

New functionality
^^^^^^^^^^^^^^^^^


* Better code state description in logging `issue#242 <https://github.com/Parsl/parsl/issues/242>`_

* String like behavior for Files `issue#174 <https://github.com/Parsl/parsl/issues/174>`_

* Globus path mapping in config `issue#165 <https://github.com/Parsl/parsl/issues/165>`_


Bug Fixes
^^^^^^^^^

* Usage tracking with certain missing network causes 20s startup delay. `issue#220 <https://github.com/Parsl/parsl/issues/220>`_

* Checkpoints will not reload from a run that was Ctrl-C'ed `issue#232 <https://github.com/Parsl/parsl/issues/232>`_

* Race condition in task checkpointing `issue#234 <https://github.com/Parsl/parsl/issues/234>`_

* ``task_exit`` checkpointing repeatedly truncates checkpoint file during run `issue#230 <https://github.com/Parsl/parsl/issues/230>`_

* Make ``dfk.cleanup()`` not cause kernel to restart with Jupyter on Mac `issue#212 <https://github.com/Parsl/parsl/issues/212>`_

* Fix automatic IPP controller creation on OS X `issue#206 <https://github.com/Parsl/parsl/issues/206>`_

* Passing Files breaks over IPP `issue#200 <https://github.com/Parsl/parsl/issues/200>`_

* `repr` call after `AppException` instantiation raises `AttributeError` `issue#197 <https://github.com/Parsl/parsl/issues/197>`_

* Allow `DataFuture` to be initialized with a `str` file object `issue#185 <https://github.com/Parsl/parsl/issues/185>`_

* Error for globus transfer failure `issue#162 <https://github.com/Parsl/parsl/issues/162>`_


Parsl 0.5.2
-----------

Released. June 21st, 2018.
This is an emergency release addressing `issue#347 <https://github.com/Parsl/parsl/issues/347>`_

Bug Fixes
^^^^^^^^^

* Parsl version conflict with libsubmit 0.4.1 `issue#347 <https://github.com/Parsl/parsl/issues/347>`_


Parsl 0.5.0
-----------

Released. Apr 16th, 2018.

New functionality
^^^^^^^^^^^^^^^^^

* Support for Globus file transfers `issue#71 <https://github.com/Parsl/parsl/issues/71>`_

  .. caution::
     This feature is available from Parsl ``v0.5.0`` in an ``experimental`` state.

* PathLike behavior for Files `issue#174 <https://github.com/Parsl/parsl/issues/174>`_
    * Files behave like strings here :

  .. code-block:: python

      myfile = File("hello.txt")
      f = open(myfile, 'r')


* Automatic checkpointing modes `issue#106 <https://github.com/Parsl/parsl/issues/106>`_

  .. code-block:: python

        config = {
            "globals": {
                "lazyErrors": True,
                "memoize": True,
                "checkpointMode": "dfk_exit"
            }
        }

* Support for containers with docker `issue#45 <https://github.com/Parsl/parsl/issues/45>`_

  .. code-block:: python

       localDockerIPP = {
            "sites": [
                {"site": "Local_IPP",
                 "auth": {"channel": None},
                 "execution": {
                     "executor": "ipp",
                     "container": {
                         "type": "docker",     # <----- Specify Docker
                         "image": "app1_v0.1", # <------Specify docker image
                     },
                     "provider": "local",
                     "block": {
                         "initBlocks": 2,  # Start with 4 workers
                     },
                 }
                 }],
            "globals": {"lazyErrors": True}        }

  .. caution::
     This feature is available from Parsl ``v0.5.0`` in an ``experimental`` state.

* Cleaner logging `issue#85 <https://github.com/Parsl/parsl/issues/85>`_
    * Logs are now written by default to ``runinfo/RUN_ID/parsl.log``.
    * ``INFO`` log lines are more readable and compact

* Local configs are now packaged  `issue#96 <https://github.com/Parsl/parsl/issues/96>`_

  .. code-block:: python

     from parsl.configs.local import localThreads
     from parsl.configs.local import localIPP


Bug Fixes
^^^^^^^^^
* Passing Files over IPP broken `issue#200 <https://github.com/Parsl/parsl/issues/200>`_

* Fix ``DataFuture.__repr__`` for default instantiation `issue#164 <https://github.com/Parsl/parsl/issues/164>`_

* Results added to appCache before retries exhausted `issue#130 <https://github.com/Parsl/parsl/issues/130>`_

* Missing documentation added for Multisite and Error handling `issue#116 <https://github.com/Parsl/parsl/issues/116>`_

* TypeError raised when a bad stdout/stderr path is provided. `issue#104 <https://github.com/Parsl/parsl/issues/104>`_

* Race condition in DFK `issue#102 <https://github.com/Parsl/parsl/issues/102>`_

* Cobalt provider broken on Cooley.alfc `issue#101 <https://github.com/Parsl/parsl/issues/101>`_

* No blocks provisioned if parallelism/blocks = 0 `issue#97 <https://github.com/Parsl/parsl/issues/97>`_

* Checkpoint restart assumes rundir `issue#95 <https://github.com/Parsl/parsl/issues/95>`_

* Logger continues after cleanup is called `issue#93 <https://github.com/Parsl/parsl/issues/93>`_


Parsl 0.4.1
-----------

Released. Feb 23rd, 2018.


New functionality
^^^^^^^^^^^^^^^^^

* GoogleCloud provider support via libsubmit
* GridEngine provider support via libsubmit


Bug Fixes
^^^^^^^^^
* Cobalt provider issues with job state `issue#101 <https://github.com/Parsl/parsl/issues/101>`_
* Parsl updates config inadvertently `issue#98 <https://github.com/Parsl/parsl/issues/98>`_
* No blocks provisioned if parallelism/blocks = 0 `issue#97 <https://github.com/Parsl/parsl/issues/97>`_
* Checkpoint restart assumes rundir bug `issue#95 <https://github.com/Parsl/parsl/issues/95>`_
* Logger continues after cleanup called enhancement `issue#93 <https://github.com/Parsl/parsl/issues/93>`_
* Error checkpointing when no cache enabled `issue#92 <https://github.com/Parsl/parsl/issues/92>`_
* Several fixes to libsubmit.


Parsl 0.4.0
-----------

Here are the major changes included in the Parsl 0.4.0 release.

New functionality
^^^^^^^^^^^^^^^^^

* Elastic scaling in response to workflow pressure. `issue#46 <https://github.com/Parsl/parsl/issues/46>`_
  Options ``minBlocks``, ``maxBlocks``, and ``parallelism`` now work and controls workflow execution.

  Documented in: :ref:`label-elasticity`

* Multisite support, enables targetting apps within a single workflow to different
  sites `issue#48 <https://github.com/Parsl/parsl/issues/48>`_

     .. code-block:: python

          @App('python', dfk, sites=['SITE1', 'SITE2'])
          def my_app(...):
             ...

* Anonymized usage tracking added. `issue#34 <https://github.com/Parsl/parsl/issues/34>`_

  Documented in: :ref:`label-usage-tracking`

* AppCaching and Checkpointing `issue#43 <https://github.com/Parsl/parsl/issues/43>`_

     .. code-block:: python

          # Set cache=True to enable appCaching
          @App('python', dfk, cache=True)
          def my_app(...):
              ...


          # To checkpoint a workflow:
          dfk.checkpoint()

   Documented in: :ref:`label-checkpointing`, :ref:`label-appcaching`

* Parsl now creates a new directory under ``./runinfo/`` with an incrementing number per workflow
  invocation

* Troubleshooting guide and more documentation

* PEP8 conformance tests added to travis testing `issue#72 <https://github.com/Parsl/parsl/issues/72>`_


Bug Fixes
^^^^^^^^^

* Missing documentation from libsubmit was added back
  `issue#41 <https://github.com/Parsl/parsl/issues/41>`_

* Fixes for ``script_dir`` | ``scriptDir`` inconsistencies `issue#64 <https://github.com/Parsl/parsl/issues/64>`_
    * We now use ``scriptDir`` exclusively.

* Fix for caching not working on jupyter notebooks `issue#90 <https://github.com/Parsl/parsl/issues/90>`_

* Config defaults module failure when part of the option set is provided `issue#74 <https://github.com/Parsl/parsl/issues/74>`_

* Fixes for network errors with usage_tracking `issue#70 <https://github.com/Parsl/parsl/issues/70>`_

* PEP8 conformance of code and tests with limited exclusions `issue#72 <https://github.com/Parsl/parsl/issues/72>`_

* Doc bug in recommending ``max_workers`` instead of ``maxThreads`` `issue#73 <https://github.com/Parsl/parsl/issues/70>`_




Parsl 0.3.1
-----------

This is a point release with mostly minor features and several bug fixes

* Fixes for remote side handling
* Support for specifying IPythonDir for IPP controllers
* Several tests added that test provider launcher functionality from libsubmit
* This upgrade will also push the libsubmit requirement from 0.2.4 -> 0.2.5.


Several critical fixes from libsubmit are brought in:

* Several fixes and improvements to Condor from @annawoodard.
* Support for Torque scheduler
* Provider script output paths are fixed
* Increased walltimes to deal with slow scheduler system
* Srun launcher for slurm systems
* SSH channels now support file_pull() method
   While files are not automatically staged, the channels provide support for bi-directional file transport.

Parsl 0.3.0
-----------

Here are the major changes that are included in the Parsl 0.3.0 release.


New functionality
^^^^^^^^^^^^^^^^^

* Arguments to DFK has changed:

    # Old
    dfk(executor_obj)

    # New, pass a list of executors
    dfk(executors=[list_of_executors])

    # Alternatively, pass the config from which the DFK will
    #instantiate resources
    dfk(config=config_dict)

* Execution providers have been restructured to a separate repo: `libsubmit <https://github.com/Parsl/libsubmit>`_

* Bash app styles have changes to return the commandline string rather than be assigned to the special keyword ``cmd_line``.
  Please refer to `RFC #37 <https://github.com/Parsl/parsl/issues/37>`_ for more details. This is a **non-backward** compatible change.

* Output files from apps are now made available as an attribute of the AppFuture.
  Please refer `#26 <Output files from apps #26>`_ for more details. This is a **non-backward** compatible change ::

    # This is the pre 0.3.0 style
    app_fu, [file1, file2] = make_files(x, y, outputs=['f1.txt', 'f2.txt'])

    #This is the style that will be followed going forward.
    app_fu = make_files(x, y, outputs=['f1.txt', 'f2.txt'])
    [file1, file2] = app_fu.outputs

* DFK init now supports auto-start of IPP controllers

* Support for channels via libsubmit. Channels enable execution of commands from execution providers either
  locally, or remotely via ssh.

* Bash apps now support timeouts.

* Support for cobalt execution provider.


Bug fixes
^^^^^^^^^
* Futures have inconsistent behavior in bash app fn body `#35 <https://github.com/Parsl/parsl/issues/35>`_
* Parsl dflow structure missing dependency information `#30 <https://github.com/Parsl/parsl/issues/30>`_


Parsl 0.2.0
-----------

Here are the major changes that are included in the Parsl 0.2.0 release.

New functionality
^^^^^^^^^^^^^^^^^

* Support for execution via IPythonParallel executor enabling distributed execution.
* Generic executors

Parsl 0.1.0
-----------

Here are the major changes that are included in the Parsl 0.1.0 release.

New functionality
^^^^^^^^^^^^^^^^^

* Support for Bash and Python apps
* Support for chaining of apps via futures handled by the DataFlowKernel.
* Support for execution over threads.
* Arbitrary DAGs can be constructed and executed asynchronously.

Bug Fixes
^^^^^^^^^

* Initial release, no listed bugs.


Historical: Libsubmit Changelog
===============================

.. note::
   As of Parsl 0.7.0 the libsubmit repository has been merged into Parsl
   and nothing more will appear in this changelog.

Libsubmit 0.4.1
---------------

Released. June 18th, 2018.
This release folds in massive contributions from @annawoodard.

New functionality
^^^^^^^^^^^^^^^^^

* Several code cleanups, doc improvements, and consistent naming

* All providers have the initialization and actual start of resources decoupled.



Libsubmit 0.4.0
---------------

Released. May 15th, 2018.
This release folds in contributions from @ahayschi, @annawoodard, @yadudoc

New functionality
^^^^^^^^^^^^^^^^^

* Several enhancements and fixes to the AWS cloud provider (#44, #45, #50)

* Added support for python3.4


Bug Fixes
^^^^^^^^^

* Condor jobs left in queue with X state at end of completion  `issue#26 <https://github.com/Parsl/libsubmit/issues/26>`_

* Worker launches on Cori seem to fail from broken ENV `issue#27 <https://github.com/Parsl/libsubmit/issues/27>`_

* EC2 provider throwing an exception at initial run `issue#46 <https://github.com/Parsl/parsl/issues/46>`_
