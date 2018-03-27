Container Support
=================

This document describes workflow execution within containers. In the model described here,
the ``Apps`` are executed on a worker that is launched within a container. For simplicity
we focus on `Docker <https://docs.docker.com/>`_ although the same methods can be extended
to supported other container systems such as `Singularity <http://singularity.lbl.gov/>`_,
`Shifter <https://www.nersc.gov/research-and-development/user-defined-images/>`_ etc.


Installing Docker
-----------------

To install docker please ensure you have sudo privileges and follow instructions
`here <https://docs.docker.com/install/>`_.

Once installed make sure that docker is installed :

.. code-block:: bash

   # Get the docker version
   docker --version

   # Get docker info/stats :
   docker info

   # Do a quick check with hello-world
   docker run hello-world


Getting an image with Python3.6
-------------------------------

.. code-block:: bash

   # Get a basic python image
   docker pull python

Construct a new python image with your modifications from this Dockerfile:

.. code-block:: bash

    # Use an official Python runtime as a parent image
    FROM python:3.6

    # Set the working directory to /home
    WORKDIR /home

    # Install any needed packages specified in requirements.txt
    RUN pip3 install parsl


Once your updates are made, create a Docker image from the Dockerfile:

.. code-block:: bash

    # Use an official Python runtime as a parent image
    FROM python:3.6

Make sure your user has privileges to launch and manage Docker by adding yourself
to the ``docker`` group:

.. code-block:: bash

    sudo usermod -a -G docker $USER
