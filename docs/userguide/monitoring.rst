Monitoring
==========

Parsl aims to make the task of running parallel workflows easy by providing monitoring and diagnostic
capabilities to help track the state of your workflow, down to the individual applications being
executed on remote machines. To enable Parsl's monitoring feature for your workflow, you will need
a few additional packages.

Installation
------------

Parsl's monitoring model relies on writing workflow progress to a sqlite database and using separate tools
that query this database to create avweb-based dashboard for the workflow.

To enable workflow monitoring support install::

    $ pip install parsl[monitoring]

Monitoring configuration
------------------------

Here's an example configuration that logs monitoring information to a local sqlite database:: 

.. code-block:: python

    import parsl
    from parsl.monitoring.monitoring import MonitoringHub
    from parsl.config import Config
    from parsl.executors import HighThroughputExecutor
    from parsl.addresses import address_by_hostname

    import logging

    config = Config(
        executors=[
            HighThroughputExecutor(
                label="local_htex",
                cores_per_worker=1,
                max_workers=4,
                address=address_by_hostname(),
            )
        ],
        monitoring=MonitoringHub(
            hub_address=address_by_hostname(),
            logging_level=logging.INFO,
            resource_monitoring_interval=10,
        ),
        strategy=None
    )



Visualization
-------------

Install the visualization server::

   $ pip install git+https://github.com/Parsl/viz_server.git

Run the `parsl-visualize` utility in the directory with the
`monitoring.db` sqlite file to launch a web page for the workflow visualization::

   $ parsl-visualize sqlite:///<absolute-path-to-db>

For example, if `monitoring.db` is in `/tmp` (as `/tmp/monitoring.db`), run `parsl-visualize` as follows::

   $ parsl-visualize sqlite:////tmp/monitoring.db

This starts a visualization web server on `127.0.0.1:8080` by default. If you are running on a machine with a web browser, you can access viz_server in the browser via `127.0.0.1:8080`. If you are running on the login node of a cluster, to access viz_server in a local machine's browser, you need an ssh tunnel from your local machine to the cluster::

   $ ssh -L 50000:127.0.0.1:8080 username@cluster_address

This binds your local machine's port 50000 to the remote cluster's port 8080. This allows you to access viz_server directly on your local machine's browser via `127.0.0.1:50000`. 

.. warning:: Below is an alternative to host the viz_server, which may violate the cluster's security policy. Please check with your cluster admin before doing this.
If the cluster allows you to host a web server on its public IP address with a specific port (i.e., open to Internet via `public_IP:55555`), you can run::

   $ parsl-visualize -e --port 55555 sqlite:///<absolute-path-to-db>

.. warning:: Please note that visualization support is in `alpha` state
