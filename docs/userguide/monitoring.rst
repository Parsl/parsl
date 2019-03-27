Monitoring
==========

Parsl aims to make the task of running parallel workflows easy by providing monitoring and diagnostic
capabilities to help track the state of your workflow down to the individual applications being
executed on remote machines. To enable Parsl's monitoring feature for your workflow you will need
a few additional packages.

Installation
------------

Parsl's monitoring model relies on writing workflow progress to a sqlite database and separate tools
that query this database to create web-based dashboard for the workflow.

To enable workflow monitoring support install::

    $ pip install parsl[monitoring]

Monitoring configuration
------------------------

Here's an example configuration that logs monitoring information to a local sqlite database.

.. code-block:: python

    config = Config(
        executors=[
            HighThroughputExecutor(
                label="local_htex",
                cores_per_worker=1,
                address=address_by_hostname(),
            )
        ],
        monitoring=MonitoringHub(
            hub_address=address_by_hostname(),
            hub_port=55055,
            logging_level=logging.INFO,
            resource_monitoring_interval=10,
        ),
        strategy=None
    )



Visualization
-------------

Install the visualization server::

   $ pip install git+https://github.com/Parsl/viz_server.git

Once `viz_server` is installed, you can run the utility `parsl-visualize` in the directory with the
monitoring.db sqlite file to launch a web page with the workflow visualization.

.. warning:: Please note that visualization support is in `alpha` state
