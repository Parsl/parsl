FAQ
---

How can I debug a Parsl script?
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Parsl interfaces with the Python logger. To enable logging of parsl's
progress to stdout turn on the logger as follows. Alternatively, you
can configure the file logger to write to an output file.

.. code-block:: python

   from parsl import *
   import logging

   # Emit log lines to the screen
   parsl.set_stream_logger()

   # Write log to file, specify level of detail for logs
   parsl.set_file_logger(FILENAME, level=logging.DEBUG)

.. note::
   Parsl's logging will not capture STDOUT/STDERR from the apps's themselves.
   Follow instructions below for application logs.


How can I view outputs and errors from Apps?
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Parsl Apps include keyword arguments for capturing stderr and stdout in files.

.. code-block:: python

   @app('bash', dfk)
   def hello (msg, stdout=None):
       return 'echo {}'.format(msg)

   # When hello() runs the STDOUT will be written to 'hello.txt'
   hello('Hello world', stdout='hello.txt')

How can I make an App dependent on multiple inputs?
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

You can pass any number of futures in to a single App either as positional arguments
or as a list of futures via the special keyword `inputs=[]`.
The App will wait for all inputs to be satisfied before execution.


Can I pass any Python object between Apps?
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

No. Unfortunately, only picklable objects can be passed between Apps.
For objects that can't be pickled it is recommended to use object specific methods
to write the object into a file and use files to communicate between Apps.

How do I specify where Apps should be run?
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Parsl's multi-site support allows you to define the site (including local threads)
on which an App should be executed. For example :

.. code-block:: python

     @app('python', dfk, sites=['SuperComputer1'])
     def BigSimulation(...):
         ...

     @app('python', dfk, sites=['GPUMachine'])
     def Visualize (...)
         ...

Workers do not connect back to Parsl
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

If you are running via ssh to a remote system from your local machine, or from the
login node of a cluster/supercomputer, it is necessary to have a public IP to which
the workers could connect back. While on certain systems our pilot job system, ipyparallel
can identify the IP address automatically it is safer to specify the address explicitly.

Here's how you specify the address in the config dictionary passed to the DataFlowKernel:

.. code-block:: python

    multiNode = {
        "sites": [{
            "site": "ALCF_Theta_Local",
            "auth": {
                "channel": "ssh",
                "scriptDir": "/home/{}/parsl_scripts/".format(USERNAME)
            },
            "execution": {
                "executor": "ipp",
                "provider": '<SCHEDULER>'
                "block": { # Define the block
                    ...
                }
            },
        }],
        "globals": {
            "lazyErrors": True,
    },
        "controller": {
        "publicIp": '<AA.BB.CC.DD>'  # <--- SPECIFY PUBLIC IP HERE
        }
    }


.. _pyversion:

Remote execution fails with SystemError(unknown opcode)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

When running with Ipyparallel workers, it is important to ensure that the Python version
on the client side matches that on the side of the workers. If there's a mismatch,
the apps sent to the workers will fail with the following error:
``ipyparallel.error.RemoteError: SystemError(unknown opcode)``

.. note::
   It is recommended that both the parsl script and all workers are set to use python
   with the same Major.Minor version numbers. For eg. use Python3.5.X on both local
   and worker side.

Parsl complains about missing packages
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

If ``parsl`` is cloned from a github repository and added to the ``PYTHONPATH``, it is
possible to miss the installation of some dependent libraries. In this configuration,
``parsl`` will raise errors such as :

``ModuleNotFoundError: No module named 'ipyparallel'``

In this situation, please install the required packages. If you are on a machine with
sudo privileges you could install the packages for all users, or if you choose install
to a virtual environment using packages such as virtualenv and conda.

For instance with conda, follow this `cheatsheet <https://conda.io/docs/_downloads/conda-cheatsheet.pdf>`_ to create a virtual environment :

.. code-block:: bash

   # Activate an environmentconda install
   source active <my_env>

   # Install packages:
   conda install <ipyparallel, dill, boto3...>


zmq.error.ZMQError: Invalid argument
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

If you are making the transition from Parsl v0.3.0 to v0.4.0
and you run into this error, please check your config structure.
In v0.3.0, ``config['controller']['publicIp'] = '*'`` was commonly
used to specify that the IP address should be autodetected.
This has changed in v0.4.0 and setting ``'publicIp' = '*'`` results
in an error with a traceback that looks like this :

.. code-block:: python

   File "/usr/local/lib/python3.5/dist-packages/ipyparallel/client/client.py", line 483, in __init__
   self._query_socket.connect(cfg['registration'])
   File "zmq/backend/cython/socket.pyx", line 528, in zmq.backend.cython.socket.Socket.connect (zmq/backend/cython/socket.c:5971)
   File "zmq/backend/cython/checkrc.pxd", line 25, in zmq.backend.cython.checkrc._check_rc (zmq/backend/cython/socket.c:10014)
   zmq.error.ZMQError: Invalid argument

In v0.4.0, the controller block defaults to detecting the IP address
automatically, and if that does not work for you, you can specify the
IP address explicitly like this : ``config['controller']['publicIp'] = 'IP.ADD.RES.S'``

How do I run code that uses Python2.X?
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Modules or code that requires Python2.X cannot be run as python apps,
however they may be run via bash apps. The primary limitation here with
python apps are that all the inputs and outputs including the function
would be mangled when being transmitted between python interpretors with
different version numbers (also see :ref:`pyversion`)

Here's an example of running a python2.7 code as a bash application:

.. code-block:: python

   @app('bash', dfk)
   def python_27_app (arg1, arg2 ...):
       return '''conda activate py2.7_env  # Use conda to ensure right env
       python2.7 my_python_app.py -arg {0} -d {1}
       '''.format(arg1, arg2)
