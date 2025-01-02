Encryption
----------

Users can encrypt traffic between the Parsl DFK and ``HighThroughputExecutor`` instances by setting its ``encrypted``
initialization argument to ``True``.

For example,

.. code-block:: python

    from parsl.config import Config
    from parsl.executors import HighThroughputExecutor

    config = Config(
        executors=[
            HighThroughputExecutor(
                encrypted=True
            )
        ]
    )

Under the hood, we use `CurveZMQ <http://curvezmq.org/>`_ to encrypt all communication channels
between the executor and related nodes.

Encryption performance
^^^^^^^^^^^^^^^^^^^^^^

CurveZMQ depends on `libzmq <https://github.com/zeromq/libzmq>`_ and  `libsodium <https://github.com/jedisct1/libsodium>`_,
which `pyzmq <https://github.com/zeromq/pyzmq>`_ (a Parsl dependency) includes as part of its
installation via ``pip``. This installation path should work on most systems, but users have
reported significant performance degradation as a result.

If you experience a significant performance hit after enabling encryption, we recommend installing
``pyzmq`` with conda:

.. code-block:: bash

    conda install conda-forge::pyzmq

Alternatively, you can `install libsodium <https://doc.libsodium.org/installation>`_, then
`install libzmq <https://zeromq.org/download/>`_, then build ``pyzmq`` from source:

.. code-block:: bash

    pip3 install parsl --no-binary pyzmq
