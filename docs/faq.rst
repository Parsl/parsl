FAQ
---

How can I debug a Parsl script?
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Parsl interfaces with the Python logger and automatically logs Parsl-related messages a ``runinfo`` directory.
The ``runinfo`` directory will be created in the folder from which you run the Parsl script
and it will contain a series of subfolders for each time you run the code.
Your latest run will be the largest number.

Alternatively, you can configure the file logger to write to an output file.

.. code-block:: python

   import logging
   import parsl

   # Emit log lines to the screen
   parsl.set_stream_logger()

   # Write log to file, specify level of detail for logs
   parsl.set_file_logger(FILENAME, level=logging.DEBUG)

.. note::
   Parsl's logging will not capture STDOUT/STDERR from the apps themselves.
   Follow instructions below for application logs.


How can I view outputs and errors from apps?
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Parsl apps include keyword arguments for capturing stderr and stdout in files.

.. code-block:: python

   @bash_app
   def hello(msg, stdout=None):
       return 'echo {}'.format(msg)

   # When hello() runs the STDOUT will be written to 'hello.txt'
   hello('Hello world', stdout='hello.txt')

How can I make an App dependent on multiple inputs?
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

You can pass any number of futures in to a single App either as positional arguments
or as a list of futures via the special keyword ``inputs=()``.
The App will wait for all inputs to be satisfied before execution.


Can I pass any Python object between apps?
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

This depends on the executor in use. The :py:class:`parsl.executors.threads.ThreadPoolExecutor`
can receive and return any Python object. Other executors will serialize their
parameters and return values, so only objects which Parsl knows how to
serialize can be passed.

Parsl knows how to serialize objects using the Pickle and Dill
libraries.

Pickle provides a list of objects that it knows how to serialize:
`What can be pickled and unpickled? <https://docs.python.org/3/library/pickle.html#what-can-be-pickled-and-unpickled>`_.

Dill can serialize much more than Pickle, documented in the
`dill documentation <https://dill.readthedocs.io/en/latest/>`_.

For objects that can't be pickled, use object specific methods
to write the object into a file and use files to communicate between apps.

How do I specify where apps should be run?
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Parsl's multi-executor support allows you to define the executor (including local threads)
on which an App should be executed. For example:

.. code-block:: python

     @python_app(executors=['SuperComputer1'])
     def BigSimulation(...):
         ...

     @python_app(executors=['GPUMachine'])
     def Visualize (...)
         ...

Workers do not connect back to Parsl
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

If you are running via ssh to a remote system from your local machine, or from the
login node of a cluster/supercomputer, it is necessary to have a public IP to which
the workers can connect back. While our remote execution systems can identify the
IP address automatically in certain cases, it is safer to specify the address explicitly.
Parsl provides a few heuristic based address resolution methods that could be useful,
however with complex networks some trial and error might be necessary to find the
right address or network interface to use.



For `parsl.executors.HighThroughputExecutor` the address is specified in the :class:`~parsl.config.Config`
as shown below :

.. code-block:: python

    # THIS IS A CONFIG FRAGMENT FOR ILLUSTRATION
    from parsl.config import Config
    from parsl.executors import HighThroughputExecutor
    from parsl.addresses import address_by_route, address_by_query, address_by_hostname
    config = Config(
        executors=[
            HighThroughputExecutor(
                label='ALCF_theta_local',
                address='<AA.BB.CC.DD>'          # specify public ip here
                # address=address_by_route()     # Alternatively you can try this
                # address=address_by_query()     # Alternatively you can try this
                # address=address_by_hostname()  # Alternatively you can try this
            )
        ],
    )


.. note::
   Another possibility that can cause workers not to connect back to Parsl is an incompatibility between
   the system and the pre-compiled bindings used for pyzmq. As a last resort, you can try:
   ``pip install --upgrade --no-binary pyzmq pyzmq``, which forces re-compilation.

For the `parsl.executors.HighThroughputExecutor`, ``address`` is a keyword argument
taken at initialization. Here is an example for the `parsl.executors.HighThroughputExecutor`:

.. code-block:: python

    # THIS IS A CONFIG FRAGMENT FOR ILLUSTRATION
    from parsl.config import Config
    from parsl.executors import HighThroughputExecutor
    from parsl.addresses import address_by_route, address_by_query, address_by_hostname

    config = Config(
        executors=[
            HighThroughputExecutor(
                label='NERSC_Cori',
                address='<AA.BB.CC.DD>'          # specify public ip here
                # address=address_by_route()     # Alternatively you can try this
                # address=address_by_query()     # Alternatively you can try this
                # address=address_by_hostname()  # Alternatively you can try this
            )
        ],
    )


.. note::
   On certain systems such as the Midway RCC cluster at UChicago, some network interfaces have an active
   intrusion detection system that drops connections that persist beyond a specific duration (~20s).
   If you get repeated ``ManagerLost`` exceptions, it would warrant taking a closer look at networking.

.. _pyversion:

parsl.errors.ConfigurationError
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The Parsl configuration model underwent a major and non-backward compatible change in the transition to v0.6.0.
Prior to v0.6.0 the configuration object was a python dictionary with nested dictionaries and lists.
The switch to a class based configuration allowed for well-defined options for each specific component being
configured as well as transparency on configuration defaults. The following traceback indicates that the old
style configuration was passed to Parsl v0.6.0+ and requires an upgrade to the configuration.

.. code-block::

   File "/home/yadu/src/parsl/parsl/dataflow/dflow.py", line 70, in __init__
       'Expected `Config` class, received dictionary. For help, '
   parsl.errors.ConfigurationError: Expected `Config` class, received dictionary. For help,
   see http://parsl.readthedocs.io/en/stable/stubs/parsl.config.Config.html

For more information on how to update your configuration script, please refer to:
:ref:`configuration-section`.

   
Remote execution fails with SystemError(unknown opcode)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

When running with Ipyparallel workers, it is important to ensure that the Python version
on the client side matches that on the side of the workers. If there's a mismatch,
the apps sent to the workers will fail with the following error:
``ipyparallel.error.RemoteError: SystemError(unknown opcode)``

.. caution::
   It is **required** that both the parsl script and all workers are set to use python
   with the same Major.Minor version numbers. For example, use Python3.5.X on both local
   and worker side.

Parsl complains about missing packages
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

If ``parsl`` is cloned from a Github repository and added to the ``PYTHONPATH``, it is
possible to miss the installation of some dependent libraries. In this configuration,
``parsl`` will raise errors such as:

``ModuleNotFoundError: No module named 'ipyparallel'``

You should usually install parsl using a package managment tool such as ``pip`` or
``conda``, ideally in a restricted environment such a virtualenv or a conda
environment.

For instance, with conda, follow this `cheatsheet <https://conda.io/docs/_downloads/conda-cheatsheet.pdf>`_ to create a virtual environment:

.. code-block:: bash

   # Activate an environmentconda install
   source activate <my_env>

   # Install packages:
   conda install <dill, boto3...>


How do I run code that uses Python2.X?
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Modules or code that require Python2.X cannot be run as python apps,
however they may be run via bash apps. The primary limitation with
python apps is that all the inputs and outputs including the function
would be mangled when being transmitted between python interpreters with
different version numbers (also see :ref:`pyversion`)

Here is an example of running a python2.7 code as a bash application:

.. code-block:: python

   @bash_app
   def python_27_app (arg1, arg2 ...):
       return '''conda activate py2.7_env  # Use conda to ensure right env
       python2.7 my_python_app.py -arg {0} -d {1}
       '''.format(arg1, arg2)

Parsl hangs
^^^^^^^^^^^

There are a few common situations in which a Parsl script might hang:

1. Circular Dependency in code:
   If an ``app`` takes a list as an ``input`` argument and the future returned
   is added to that list, it creates a circular dependency that cannot be resolved.
   This situation is described in `issue 59 <https://github.com/Parsl/parsl/issues/59>`_ in more detail.

2. Workers requested are unable to contact the Parsl client due to one or
   more issues listed below:

   * Parsl client does not have a public IP (e.g. laptop on wifi).
     If your network does not provide public IPs, the simple solution is to
     ssh over to a machine that is public facing. Machines provisioned from
     cloud-vendors setup with public IPs are another option.

   * Parsl hasn't autodetected the public IP. See `Workers do not connect back to Parsl`_ for more details.

   * Firewall restrictions that block certain port ranges.
     If there is a certain port range that is **not** blocked, you may specify
     that via configuration:

     .. code-block:: python

        from parsl.config import Config
        from parsl.providers import SlurmProvider
        from parsl.executors import HighThroughputExecutor

        config = Config(
            executors=[
                HighThroughputExecutor(
                    label='htex',
                    provider=SlurmProvider(),
                    worer_port_range=('50000,55000'),
                    interchange_port_range=('50000,55000')
                )
            ],
        )


How can I start a Jupyter notebook over SSH?
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Run

.. code-block:: bash

    jupyter notebook --no-browser --ip=`/sbin/ip route get 8.8.8.8 | awk '{print $NF;exit}'`

for a Jupyter notebook, or 

.. code-block:: bash

    jupyter lab --no-browser --ip=`/sbin/ip route get 8.8.8.8 | awk '{print $NF;exit}'`

for Jupyter lab (recommended). If that doesn't work, see `these instructions <https://techtalktone.wordpress.com/2017/03/28/running-jupyter-notebooks-on-a-remote-server-via-ssh/>`_.

How can I sync my conda environment and Jupyter environment?
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Run::

   conda install nb_conda

Now all available conda environments (for example, one created by following the instructions `in the quickstart guide <quickstart.rst#installation-using-conda>`_) will automatically be added to the list of kernels.

.. _label_serialization_error:

Addressing SerializationError
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

As of v1.0.0, Parsl will raise a `SerializationError` when it encounters an object that Parsl cannot serialize.
This applies to objects passed as arguments to an app, as well as objects returned from the app.

Parsl uses dill and pickle to serialize Python objects
to/from functions. Therefore, Python apps can only use input and output objects that can be serialized by
dill or pickle. For example the following data types are known to have issues with serializability :

* Closures
* Objects of complex classes with no ``__dict__`` or ``__getstate__`` methods defined
* System objects such as file descriptors, sockets and locks (e.g threading.Lock)

If Parsl raises a `SerializationError`, first identify what objects are problematic with a quick test:

.. code-block:: python

   import pickle
   # If non-serializable you will get a TypeError
   pickle.dumps(YOUR_DATA_OBJECT)

If the data object simply is complex, please refer `here <https://docs.python.org/3/library/pickle.html#handling-stateful-objects>`_ for more details
on adding custom mechanisms for supporting serialization.



How do I cite Parsl?
^^^^^^^^^^^^^^^^^^^^

To cite Parsl in publications, please use the following:

Babuji, Y., Woodard, A., Li, Z., Katz, D. S., Clifford, B., Kumar, R., Lacinski, L., Chard, R., Wozniak, J., Foster, I., Wilde, M., and Chard, K., Parsl: Pervasive Parallel Programming in Python. 28th ACM International Symposium on High-Performance Parallel and Distributed Computing (HPDC). 2019. https://doi.org/10.1145/3307681.3325400

or

.. code-block:: latex

    @inproceedings{babuji19parsl,
      author       = {Babuji, Yadu and
                      Woodard, Anna and
                      Li, Zhuozhao and
                      Katz, Daniel S. and
                      Clifford, Ben and
                      Kumar, Rohan and
                      Lacinski, Lukasz and
                      Chard, Ryan and 
                      Wozniak, Justin and
                      Foster, Ian and 
                      Wilde, Mike and
                      Chard, Kyle},
      title        = {Parsl: Pervasive Parallel Programming in Python},
      booktitle    = {28th ACM International Symposium on High-Performance Parallel and Distributed Computing (HPDC)},
      doi          = {10.1145/3307681.3325400},
      year         = {2019},
      url          = {https://doi.org/10.1145/3307681.3325400}
    }


How can my tasks survive ``WorkerLost`` and ``ManagerLost`` at the end of a batch job?
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

When a batch job ends, pilot workers will be terminated by the batch system,
and any tasks running there will fail. With `HighThroughputExecutor`,
this failure will be reported as a `parsl.executors.high_throughput.errors.WorkerLost` or
`parsl.executors.high_throughput.errors.ManagerLost` in the task future.

To mitigate against this:

* use retries by setting ``retries=`` in `parsl.config.Config`.
* if you only want to retry on certain errors such as `WorkerLost` and `ManagerLost`,
  use ``retry_handler`` in `parsl.config.Config` to implement that policy.
* avoid sending tasks to batch jobs that will expire soon. With `HighThroughputExecutor`,
  set drain_period to a little longer than you expect your tasks to take.
  With `WorkQueueExecutor`, you can configure individual expected task duration using
  a ``parsl_resource_specification`` and specify a worker ``--wall-time`` using the
  ``worker_options`` parameter to the `WorkQueueExecutor`.
