.. _label-execution:

Providers
=========

The **Provider** defines how to acquire then start Parsl workers on remote resources.
Providers may, for example, interface with queuing system of a computer cluster,
provision virtual machines from a cloud computing vendor,
or manage containers via Kubernetes.

Define a Provider by first selecting the `appropriate interface <providers.html>`_,
specifying how to `launch workers on newly-acquired resources <launchers.html>`_
then configuring how Parsl will adjust `amount of resources according to demand <elasticity.html>`_.

.. toctree::
   :maxdepth: 2

   providers
   launchers
   elasticity
