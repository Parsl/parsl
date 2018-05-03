Container Support
=================

There are two broad models for app execution with containers:

1. Workers are launched inside containers; a single container can be re-used for several ``Apps``.
2. Each ``App`` is launched inside a fresh container.

This document describes the first case. In this model, the ``Apps`` are executed on a worker that is launched within a container.
For simplicity we focus on `Docker <https://docs.docker.com/>`_ although the same methods can be extended
to supported other container systems such as `Singularity <http://singularity.lbl.gov/>`_,
`Shifter <https://www.nersc.gov/research-and-development/user-defined-images/>`_ etc.

.. caution::
   This feature is available from Parsl ``v0.5.0`` in an ``experimental`` state.
   We request feedback and feature enhancement requests via `github <https://github.com/Parsl/parsl/issues>`_.

Docker
------

The following section describes creating a pool of containers, each with a worker
that executes specific ``Apps``. Most of the immediately following sections can be skimmed
if you have experience working with containers.

Installing Docker
^^^^^^^^^^^^^^^^^

To install Docker please ensure you have sudo privileges and follow instructions
`here <https://docs.docker.com/install/>`_.

Once installed make sure that Docker is installed:

.. code-block:: bash

   # Get the Docker version
   docker --version

   # Get Docker info/stats
   docker info

   # Do a quick check with hello-world
   docker run hello-world


Creating an Image
^^^^^^^^^^^^^^^^^

Please note that the following instructions are tested on Ubuntu 16.04. If you are on a different
operating system, every command that is not a Docker command might need to be tweaked for your
specific system. Such cases will be noted explicitly.

1. Pull an image with the latest python.

   .. code-block:: bash

      # Get a basic python image
      docker pull python

2. Construct a new python image with your modifications by creating a file called ``Dockerfile`` with
   the following contents. Every command in the container definition is assumed to be running in Ubuntu.

   .. code-block:: bash

      # Use an official Python runtime as a parent image
      FROM python:3.6

      # Set the working directory to /home
      WORKDIR /home

      # Install any needed packages specified in requirements.txt
      RUN pip3 install parsl


3. Once your updates are made, create a Docker image from the Dockerfile.

   .. code-block:: bash

      docker build -t parslbase_v0.1 .

4. Make sure your user has privileges to launch and manage Docker by adding yourself
   to the ``docker`` group. The following command assumes an Ubuntu machine.

   .. code-block:: bash

      sudo usermod -a -G docker $USER


5. Ensure that you are running ``Python3.6.X``. If you need python3.5, make sure that
   the container built in the previous steps install and setup the python version that
   match the host machine's environment.

   .. code-block:: bash

      # This command should return Python 3.6 or higher.
      python3 -V

6. Set up apps. Check the following directories for two simple apps:

   * ``parsl/docker/app1``
   * ``parsl/docker/app2``

   These container scripts are setup such that, when they are built they copy the application
   python code over to ``/home``, which will be the ``cwd`` when app invocations
   are made. Each of these `appN.py` scripts contain the definition of a ``predict(List)``
   function.

7. Build the test applications as Docker images:
   We assume you are in the top level of the Parsl repository.

   .. code-block:: bash

      # Docker build app1
      cd docker/app1
      docker build -t app1_v0.1 .

      # Docker build the next app
      cd ../app2
      docker build -t app2_v0.1 .

      # Check the new images:
      docker images list


Parsl Config
^^^^^^^^^^^^

Now that we have a Docker image available locally, we will create a ``site`` that
uses such an image to launch containers. ``Apps`` will execute in this environment.

Here is a Parsl configuration using one of the Docker images created in the previous section.

.. code-block:: python

        local_docker_IPP = {
            "sites": [
                {
                    "site": "pool_app1",
                    "auth": {
                        "channel": None
                    },
                    "execution": {
                        "executor": "ipp",
                        "container": {
                            "type": "docker",  # Specify Docker
                            "image": "app1_v0.1",  # Specify docker image
                        },
                        "provider": "local",
                        "block": {
                            "initBlocks": 2,  # Start with 4 workers
                        },
                    }
                }
            ],
            "globals": {
                "lazyErrors": True
            }
        }

For workflows with multiple apps which require different docker images, a new site should be
created for each of the images that will be used. In the Parsl workflow definition the ``App``
decorator can then be tagged with the ``sites`` keyword argument to ensure that apps execute
on the specific sites with the right container image.

.. caution::
   If you have specific modules or python packages that are imported from relative paths,
   the workers in the container will not have these available unless explicitly copied in.

   .. code-block:: bash

       $ DOCKER_CWD=$(docker image inspect --format='{{{{.Config.WorkingDir}}}}' {2})
       $ docker cp -a . $DOCKER_ID:$DOCKER_CWD

How this works
^^^^^^^^^^^^^^

.. code-block:: bash

                           +-----local/Kubernetes/slurm... ---
                           |
   +----- Parsl--------+   |    +---------site1------------------+
   |                   |   |    |           ...                  |
   |                   |   |    | +-------App1Container--------+ |
   | App1(sites=pool1)----------+-+--------app1.py             | |
   |                   |   |    | |         +-----predict()    | |
   |       X           |   |    | +----------------------------+ |
   |      / \          |   |    +--------------------------------+
   |     Y...Y         |   |
   |      \ /          |   |    +---------site2------------------+
   |       Z           |   |    |           ...                  |
   |                   |   |    | +-------App2Container--------+ |
   | App2(sites=pool2)----------+-+------- app2.py             | |
   |                   |   |    | |         +-----predict()    | |
   |                   |   |    | +----------------------------+ |
   +-------------------+   |    +--------------------------------+
                           |
                           +------------------- -- -


The diagram above illustrates the various components and how the interact with
each other to act as a fast model serving system. In this model, each site in the Parsl
config definition can only serve one container image. Parsl launches multiple blocks
matching the definition of the site, and each block will contain one container instantiated
with a worker running inside. In the examples given above, the worker is launched in the
working directory which also contains some application code:``app1.py``.

The application codes ``app1.py`` and ``app2.py`` in our example Docker images, both
contain a simple python function ``predict()`` that takes a list of numbers (floats/ints) applies
a simple arithmetic operation and returns a corresponding list.

Here's the contents of ``app1.py``:

.. code-block:: python

    def predict(list_items):
        """Returns the double of the items"""
        return [i*2 for i in list_items]

A snippet of the Parsl code that imports the ``app1.py`` file and calls ``predict()`` on a site
that specifies the right container image ``app1_v0.1`` is below :

.. code-block:: python

    @App('python', dfk, sites=['pool_app1'], cache=True)
    def app_1(data):
        import app1
        return app1.predict(data)

    x = app_1([1,2,3])

    # The print statement prints [2,4,6] once the results are available
    print(x.result())
