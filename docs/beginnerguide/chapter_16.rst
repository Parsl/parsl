16. Extending Parsl with Plugins
================================

Parsl is designed to be flexible and extensible. You can customize its behavior and add new features by writing your own plugins. Plugins are like add-ons that you can plug into Parsl to make it do what you want.

Overview of Plugins
-------------------

Parsl plugins are Python classes that inherit from specific base classes provided by Parsl. These base classes define the interfaces that plugins need to implement to interact with different parts of Parsl.

Executors
~~~~~~~~~

Executors are responsible for running your Parsl apps on the available resources. You can write custom executors to support new types of resources or to implement different scheduling strategies.

Providers, Launchers, and Channels
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

- **Providers**: Providers are used to access and manage computing resources. You can write custom providers to support new types of resources, such as cloud platforms or specialized hardware.
- **Launchers**: Launchers are responsible for starting the worker processes that execute your Parsl apps. You can write custom launchers to customize how workers are launched on different types of resources.
- **Channels**: Channels are used to communicate with remote resources. You can write custom channels to support different communication protocols or implement authentication mechanisms.

File Staging Plugins
~~~~~~~~~~~~~~~~~~~~

Parsl can automatically transfer files between the computer where you submit your Parsl script and the computers where your apps run. You can write custom file staging plugins to support different file transfer protocols or to customize how files are transferred.

Default stdout/stderr Name Generation
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Parsl can automatically generate names for the files where the standard output and error of your apps are stored. You can write a plugin to customize this naming scheme.

Memoization/Checkpointing
~~~~~~~~~~~~~~~~~~~~~~~~~

Parsl can cache the results of your apps to avoid re-executing them with the same inputs. You can write a plugin to customize how Parsl caches results or to implement different caching strategies.

Dependency Resolution
~~~~~~~~~~~~~~~~~~~~~~

Parsl automatically determines the dependencies between your apps and ensures that they are executed in the correct order. You can write a plugin to customize how Parsl resolves dependencies.

Invoking Other Asynchronous Components
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Parsl can interact with other libraries or frameworks that use futures. You can write a plugin to integrate Parsl with these components.

Practical Tutorial: Writing Your Own Plugin
-------------------------------------------

Here's a simplified explanation of a custom Parsl plugin, based on an example provided in the documentation:

Understanding the Plugin
~~~~~~~~~~~~~~~~~~~~~~~~

This plugin is designed to work with the HighThroughputExecutor, which is used for running Parsl apps on clusters or supercomputers. The plugin modifies the executor's behavior by adding a timestamp to the name of each task's log file. This can be helpful for tracking when tasks are executed.

.. code-block:: python

   from parsl.executors import HighThroughputExecutor
   from parsl.app.errors import AppException
   import os

   class TimeStampedHtex(HighThroughputExecutor):
       """
       Plugin to HighThroughputExecutor that adds a timestamp to logfile names.
       """

       def _get_launch_command(self, *args, **kwargs):
           import datetime
           time_string = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
           kwargs['worker_logdir_root'] = os.path.join(kwargs['worker_logdir_root'], time_string)
           return super()._get_launch_command(*args, **kwargs)

Explanation
~~~~~~~~~~~

- **Inheritance**: The plugin class `TimeStampedHtex` inherits from `HighThroughputExecutor`. This means it gets all the features of the `HighThroughputExecutor` and can add its own modifications.
- **_get_launch_command Method**: This method is responsible for creating the command that launches the worker processes. The plugin overrides this method to add a timestamp to the `worker_logdir_root` argument. This argument specifies the directory where the worker logs will be stored.
- **Timestamp Generation**: The plugin uses the `datetime` module to get the current time and format it as a string (e.g., "2024-06-09_12-34-56").
- **Modifying the Log Directory**: The plugin appends the timestamp to the original `worker_logdir_root` path, creating a new directory for each run of the executor.
- **Calling the Parent Method**: Finally, the plugin calls the `_get_launch_command` method of the parent class (`super()._get_launch_command`) to get the rest of the launch command.

Using the Plugin
----------------

To use this plugin, you would simply replace `HighThroughputExecutor` with `TimeStampedHtex` in your Parsl configuration:

.. code-block:: python

   from parsl.config import Config
   from parsl.providers import SlurmProvider

   config = Config(
       executors=[
           TimeStampedHtex(
               # ... (rest of your executor configuration)
           )
       ]
   )

Now, when you run your Parsl script, the worker logs will be stored in directories with timestamps in their names, making it easier to track the logs for different runs.
