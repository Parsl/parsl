Changelog
=========

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

* Checkpoints will not reload from a run that was Ctrl-C'ed `issue#220 <https://github.com/Parsl/parsl/issues/220>`_

* Race condition in task checkpointing `issue#234 <https://github.com/Parsl/parsl/issues/234>`_

* `task_exit` checkpointing repeatedly truncates checkpoint file during run `issue#230 <https://github.com/Parsl/parsl/issues/230>`_

* Make `dfk.cleanup()` not cause kernel to restart with Jupyter on Mac `issue#212 <https://github.com/Parsl/parsl/issues/212>`_

* Fix automatic IPP controller creation on OS X `issue#206 <https://github.com/Parsl/parsl/issues/206>`_

* Passing Files breaks over IPP `issue#200 <https://github.com/Parsl/parsl/issues/200>`_

* `repr` call after `AppException` instantiation raises `AttributeError` `issue#197 <https://github.com/Parsl/parsl/issues/197>`_

* Allow `DataFuture` to be initialized with a `str` file object `issue#185 <https://github.com/Parsl/parsl/issues/185>`_

* Error for globus transfer failure `issue#162 <https://github.com/Parsl/parsl/issues/162>`_


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

* Fix `DataFuture.__repr__` for default instantiation `issue#164 <https://github.com/Parsl/parsl/issues/164>`_

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
  Options `minBlocks`, `maxBlocks`, and `parallelism` now work and controls workflow execution.

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

* Parsl now creates a new directory under `./runinfo/` with an incrementing number per workflow
  invocation

* Troubleshooting guide and more documentation

* PEP8 conformance tests added to travis testing `issue#72 <https://github.com/Parsl/parsl/issues/72>`_


Bug Fixes
^^^^^^^^^

* Missing documentation from libsubmit was added back
  `issue#41 <https://github.com/Parsl/parsl/issues/41>`_

* Fixes for `script_dir` | `scriptDir` inconsistencies `issue#64 <https://github.com/Parsl/parsl/issues/64>`_
    * We now use `scriptDir` exclusively.

* Fix for caching not working on jupyter notebooks `issue#90 <https://github.com/Parsl/parsl/issues/90>`_

* Config defaults module failure when part of the option set is provided `issue#74 <https://github.com/Parsl/parsl/issues/74>`_

* Fixes for network errors with usage_tracking `issue#70 <https://github.com/Parsl/parsl/issues/70>`_

* PEP8 conformance of code and tests with limited exclusions `issue#72 <https://github.com/Parsl/parsl/issues/72>`_

* Doc bug in recommending `max_workers` instead of `maxThreads` `issue#73 <https://github.com/Parsl/parsl/issues/70>`_




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

* Bash app styles have changes to return the commandline string rather than be assigned to the special keyword `cmd_line`.
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
