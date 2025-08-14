Advanced Configuration
======================

Executors are designed to function adequately with the default options,
but there are several cases where the executors needed to be tuned for specific systems.


.. contents::
   :local:
   :depth: 1


Connecting Parsl to Workers
---------------------------

.. note::

    Skip this section if:

    1. Using the ``ThreadPoolExecutor`` and ``GlobusComputeExecutor``.
    2. Plan to run all tasks on the same computer as the Parsl script.

Parsl workers launched on remote nodes need to know the address of the node running the Parsl script.
Parsl will detect all addresses associated with the node running Parsl, and this auto-configuration works
for many users.

However, some computing systems disallow applications from attempting to access external networks
(which Parsl may break during auto-configuration) or
auto-detection will result in selecting a lower-performance network.
In such cases, determine the proper IP address for the main node using the tools in :mod:`parsl.addresses`.

The most common case is to use :meth:`~parsl.addresses.address_by_interface` to determine the
address on the network associated with a certain network interface.
For example, the compute nodes on the Polaris supercomputer connect to the login nodes over the
``bond0`` interface.
Configure a Parsl application where the main process runs on the login node by
acquiring the address through that interface:

.. code-block:: python

    config = Config(
        executors=[
            HighThroughputExecutor(
                address=address_by_interface("bond0"),
                provider=PBSProProvider(
                    launcher=MpiExecLauncher(bind_cmd="--cpu-bind", overrides="--depth=64 --ppn 1"),
                    account='ACCT',
                    queue='debug',
                    select_options="ngpus=4",
                ),
            ),
        ],
    )
