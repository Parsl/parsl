4. Working with Apps
====================

In Parsl, an app is a self-contained piece of code that performs a specific task. It can be a Python function, a Bash script, or an MPI application. Apps are the building blocks of Parsl workflows, and they can be executed in parallel across multiple computers or processors.

Python Apps
-----------

Python apps are the most common type of app in Parsl. They are simply Python functions that you decorate with the `@python_app` decorator to tell Parsl they can be run in parallel.

Creating Python Apps
^^^^^^^^^^^^^^^^^^^^

To create a Python app, you define and decorate a Python function with `@python_app`. 

Here's an example:

.. code-block:: python

   from parsl import python_app

   @python_app
   def add(x, y):
       return x + y

This app takes two numbers as input and returns their sum.

Rules for Function Contents
^^^^^^^^^^^^^^^^^^^^^^^^^^^

There are a few rules you need to follow when writing Python apps:

1. Import libraries within the function: If your app uses external libraries, you must import them inside the function, not at the top of your script. This is because the app will be executed in a separate process, and it needs access to all the libraries it uses.
2. Avoid global variables: Don't rely on global variables inside your app. Instead, pass all the data your app needs as arguments. This ensures that each app runs independently and doesn't interfere with others.
3. Return values, don't modify inputs: If you need to modify data, return the modified data as the app's result. Don't try to modify the input arguments directly, as these changes won't be visible outside the app.

Using Functions from Modules
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

You can also use functions defined in Python modules as Parsl apps. To do this, you first need to import the function from the module and then decorate it with `@python_app`.

.. code-block:: python

   from my_module import my_function

   my_app = python_app(my_function)

Inputs and Outputs Handling
^^^^^^^^^^^^^^^^^^^^^^^^^^^

Python apps can take any Python object as input and return any Python object as output, as long as the objects can be serialized using `pickle` or `dill`. These libraries allow Parsl to convert Python objects into a format that can be sent over a network to other computers.

You can pass files as inputs to Python apps using the `parsl.File` class. This class represents a file that can be on the local or remote file system. Parsl will automatically handle the transfer of files between different systems as needed.

Troubleshooting Serialization Errors
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

If you encounter a `SerializationError`, it means that Parsl cannot convert an object into a format that can be sent over a network. Parsl first attempts to serialize objects using `pickle`; if that fails, it falls back to `dill`. However, there are cases where an object can be serialized by `pickle` but not by `dill`, which can lead to a `SerializationError`.

To troubleshoot this error, you can try the following:

- **Check serializability**: Use `pickle.dumps(YOUR_DATA_OBJECT)` to check if the object can be serialized with `pickle`. If it raises a `TypeError`, the object is not serializable.
- **Simplify your objects**: If possible, simplify the objects you're passing to or returning from your app. For example, if you're passing a large dictionary, try passing only the keys or values you need.
- **Use files**: If you can't simplify your objects, you can use files to pass data between apps. This more general approach can handle any type of data, but it may be less efficient than passing objects directly.
- **Implement custom serialization**: If you need to pass complex objects not supported by `pickle` or `dill`, you can implement custom serialization methods for those objects. This requires more advanced Python knowledge but gives you full control over how your objects are serialized.
- **Check for dynamically generated functions**: If the function being serialized has been dynamically generated or wrapped by Parsl, `pickle` may try to serialize it by storing its qualified name, assuming it can be imported remotely. However, this can only succeed if the function is defined in a module available to the remote worker. To avoid this issue, you can define your functions in a separate module and import them into your Parsl script.

Special Keyword Arguments
^^^^^^^^^^^^^^^^^^^^^^^^^

Parsl provides two special keyword arguments that you can use in your Python apps:

1. `inputs`: A list of `AppFuture` or `File` objects that represent the inputs to the app. Parsl will wait for all the inputs to be ready before executing the app.
2. `outputs`: A list of `File` objects representing the app's outputs. Parsl will track the creation of these files and make them available to other apps as needed.

Execution Options
^^^^^^^^^^^^^^^^^

Various options allow you to control how Parsl executes your Python apps. For example, you can specify which executors the app can run on, whether to cache the app's results and how long the app is allowed to run before it times out.

Practical Example: Data Analysis with Python Apps
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: python

   import parsl
   from parsl import python_app, File

   parsl.load(config)

   @python_app
   def load_data(filename):
       import pandas as pd
       return pd.read_csv(filename.filepath)

   @python_app
   def process_data(df):
       return df.groupby('column_name').mean()

   @python_app
   def save_results(df, filename):
       df.to_csv(filename.filepath)

   input_file = File("my_data.csv")
   df = load_data(input_file)
   results = process_data(df)
   output_file = File("results.csv")
   save_results(results, output_file)

In this example, we have three Python apps: `load_data`, `process_data`, and `save_results`. The `load_data` app reads a CSV file into a Pandas DataFrame. The `process_data` app performs some analysis on the DataFrame. The `save_results` app saves the results to a new CSV file. To check if this script worked, you should see a new file called "results.csv" in your working directory.

Bash Apps
---------

Bash apps allow you to run shell commands or scripts in parallel using Parsl. They are defined as Python functions that return the command string to be executed.

Creating Bash Apps
^^^^^^^^^^^^^^^^^^

To create a Bash app, you define a Python function that returns a string containing the shell command you want to run. You then decorate the function with `@bash_app`.

.. code-block:: python

   from parsl import bash_app

   @bash_app
   def my_command(arg1, arg2):
       return f"my_program {arg1} {arg2}"

This app will run the command `my_program arg1 arg2` when called.

Rules for Function Contents
^^^^^^^^^^^^^^^^^^^^^^^^^^^

The rules for Bash apps are similar to those for Python apps:

1. No global variables: Pass all necessary data as arguments to the function.
2. Return the command string: The function must return a string containing the command to be executed.

Inputs and Outputs Handling
^^^^^^^^^^^^^^^^^^^^^^^^^^^

Bash apps can take any Python object as input if it can be converted to a string. However, they can only return results as files. You can specify the output files using the `outputs` keyword argument.

Special Keyword Arguments
^^^^^^^^^^^^^^^^^^^^^^^^^

In addition to the `inputs` and `outputs` keyword arguments, Bash apps also support the following:

1. `stdout`: The file to which standard output should be redirected.
2. `stderr`: The file to which standard error should be redirected.

Execution Options
^^^^^^^^^^^^^^^^^

Bash apps have the same execution options as Python apps.

Practical Example: Automating System Tasks with Bash Apps
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: python

   from parsl import bash_app, File

   @bash_app
   def compress_file(filename, outputs=[]):
       return f"gzip {filename.filepath} -c > {outputs[0].filepath}"

   input_file = File("my_file.txt")
   output_file = File("my_file.txt.gz")
   compress_file(input_file, outputs=[output_file])

This Bash app compresses a file using `gzip`. The compressed file is saved to the location specified by the `output_file` object. To check if this script worked, you should see a new file called "my_file.txt.gz" in your working directory.

MPI Apps
--------

MPI (Message Passing Interface) is a standard for parallel programming that allows processes to communicate with each other by sending and receiving messages. Parsl can be used to run MPI applications in parallel, but it requires some additional configuration.

Background on MPI
^^^^^^^^^^^^^^^^^

MPI is a powerful parallel programming tool but can be complex. Parsl simplifies the process of running MPI applications by handling the details of launching and managing MPI processes.

Writing and Configuring MPI Apps
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

To run an MPI app with Parsl, you need to:

1. **Enable MPI mode**: In your executor configuration, set the `enable_mpi_mode` option to `True`. This tells Parsl that you'll be running MPI applications and to adjust their behavior accordingly.
2. **Specify an MPI launcher**: Choose an MPI launcher compatible with your system (e.g., mpirun, srun). The launcher is responsible for starting MPI processes on allocated resources.
3. **Define your MPI app**: Write a Bash app that invokes your MPI application. This app should include the necessary commands to set up the MPI environment and launch the application with the correct parameters.
4. **Specify resource requirements**: Tell Parsl how many processes and nodes your MPI app needs. The MPI launcher uses this information to allocate the appropriate resources.

Practical Example: Running MPI Workflows
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Here's an example of how to define an MPI app:

.. code-block:: python

   from parsl import bash_app

   @bash_app
   def my_mpi_app(resource_specification):
       return f"$PARSL_MPI_PREFIX my_mpi_program"

This Bash app will run your MPI program (`my_mpi_program`) using the `PARSL_MPI_PREFIX` environment variable, which Parsl sets up automatically based on the resource specification you provide. The `resource_specification` is a dictionary that you pass to the app when you call it, and it should include the following keys:

- `nodes_per_block`: The number of nodes to use for this MPI application.
- `tasks_per_node`: The number of MPI processes to run on each node.

Parsl will then use this information to construct the appropriate MPI launch command and execute your app in parallel across the specified resources.
