12. Monitoring and Visualization
================================

Parsl provides tools to help you monitor your tasks and see how they're doing. This is important because it helps you understand how your work is progressing and find any problems that might come up.

Monitoring Configuration
-------------------------

Parsl can be set up to automatically record information about your tasks, like their status (e.g., running, finished, failed) and how much computer resources they're using (e.g., memory, processing power). This information is saved in a special file that you can look at later. To turn on monitoring, you need to add a few lines to your Parsl setup. This tells Parsl to start recording information and where to save it.

Visualization Tools
-------------------

Parsl has a tool called `parsl-visualize` that lets you see the information that Parsl has recorded. This tool creates a webpage where you can see graphs and charts that show you how your tasks are doing. To use `parsl-visualize`, you need to install it first. Then, you can run it from your computer's command line. It will open a webpage where you can see the information Parsl has collected.

Workflow Summary and Page
-------------------------

The `parsl-visualize` tool gives you a summary of your entire workflow. This shows you when the workflow started and finished, how many tasks were run, and how many of them were successful. You can also click on each task to see more details about it, like when it started and finished, how long it took, and what resources it used.

Practical Tutorial: Setting Up Monitoring and Visualization
-----------------------------------------------------------

1. **Install `parsl-visualize`**: Run the following command in your terminal:

   .. code-block:: bash

      pip install 'parsl[monitoring,visualization]'

2. **Turn on monitoring in your Parsl configuration**: Add the following lines to your configuration file:

   .. code-block:: python

      from parsl.monitoring.monitoring import MonitoringHub
      from parsl.addresses import address_by_hostname

      config = Config(
          # ... (your existing configuration)
          monitoring=MonitoringHub(
              hub_address=address_by_hostname(),
              hub_port=55055,  # Choose a port number
              resource_monitoring_interval=10,  # Record resource usage every 10 seconds
          ),
      )

3. **Run your Parsl script**: Parsl will now start recording information about your tasks.

4. **Open the visualization dashboard**: After your script finishes (or while it's running), run the following command in your terminal:

   .. code-block:: bash

      parsl-visualize

   This will open a web page where you can see your workflow visualization.
