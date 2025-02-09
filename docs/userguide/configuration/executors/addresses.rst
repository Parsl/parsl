Connecting Parsl to Workers
===========================

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
