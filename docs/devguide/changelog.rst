Changelog
=========


Parsl 0.7.0
-----------

Released on Dec 20st, 2018

Parsl v0.7.0 includes 110 closed issues with contributions (code, tests, reviews and reports)
from: Alex Hays @ahayschi, Anna Woodard @annawoodard, Ben Clifford @benc, Connor Pigg @ConnorPigg,
David Heise @daheise, Daniel S. Katz @danielskatz, Dominic Fitzgerald @djf604, Francois Lanusse @EiffL,
Juan David Garrido @garri1105, Gordon Watts @gordonwatts, Justin Wozniak @jmjwozniak,
Joseph Moon @jmoon1506, Kenyi Hurtado @khurtado, Kyle Chard @kylechard, Lukasz Lacinski @lukaszlacinski,
Ravi Madduri @madduri, Marco Govoni @mgovoni-devel, Reid McIlroy-Young @reidmcy, Ryan Chard @ryanchard,
@sdustrud, Yadu Nand Babuji @yadudoc and Zhuozhao Li @ZhuozhaoLi

New functionality
^^^^^^^^^^^^^^^^^


* `HighThroughputExecutor`: a new executor intended to replace the `IPyParallelExecutor` is now available.
  This new executor addresses several limitations of `IPyParallelExecutor` such as:

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

* `ExtremeScaleExecutor` a new executor targeting supercomputer scale (>1000 nodes) workflows is now available.

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

    * `IPyParallelExecutor` provides ``workers_per_node``
    * `HighThroughputExecutor` provides ``cores_per_worker`` to allow for worker launches to be determined based on
      the number of cores on the compute node.
    * `ExtremeScaleExecutor` uses ``ranks_per_node`` to specify the ranks to launch per node.

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

* `AprunLauncher` now supports ``overrides`` option that allows arbitrary strings to be added
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
* Fixes to `TorqueProvider` to work on NSCC `issue#489 <https://github.com/Parsl/parsl/issues/489>`_
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
* Make sure tasks in `dep_fail` state are retried `issue#473 <https://github.com/Parsl/parsl/issues/473>`_
* Hard requirement for CMRESHandler `issue#422 <https://github.com/Parsl/parsl/issues/422>`_
* Log error Globus events to stderr `issue#436 <https://github.com/Parsl/parsl/issues/436>`_
* Take 'slots' out of logging `issue#411 <https://github.com/Parsl/parsl/issues/411>`_
* Remove redundant logging `issue#267 <https://github.com/Parsl/parsl/issues/267>`_
* Zombie ipcontroller processes - Process cleanup in case of interruption `issue#460 <https://github.com/Parsl/parsl/issues/460>`_
* IPyparallel failure when submitting several apps in parallel threads `issue#451 <https://github.com/Parsl/parsl/issues/451>`_
* `SlurmProvider` + `SingleNodeLauncher` starts all engines on a single core `issue#454 <https://github.com/Parsl/parsl/issues/454>`_
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

* `task_exit` checkpointing repeatedly truncates checkpoint file during run bug `issue#230 <https://github.com/Parsl/parsl/issues/230>`_

* Checkpoints will not reload from a run that was Ctrl-C'ed `issue#232 <https://github.com/Parsl/parsl/issues/232>`_

* Race condition in task checkpointing `issue#234 <https://github.com/Parsl/parsl/issues/234>`_

* Failures not to be checkpointed `issue#239 <https://github.com/Parsl/parsl/issues/239>`_

* Naming inconsitencies with `maxThreads`, `max_threads`, `max_workers` are now resolved `issue#303 <https://github.com/Parsl/parsl/issues/303>`_

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

* `task_exit` checkpointing repeatedly truncates checkpoint file during run `issue#230 <https://github.com/Parsl/parsl/issues/230>`_

* Make `dfk.cleanup()` not cause kernel to restart with Jupyter on Mac `issue#212 <https://github.com/Parsl/parsl/issues/212>`_

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


Libsubmit Changelog
===================

As of Parsl 0.7.0 the libsubmit repository has been merged into Parsl.

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
