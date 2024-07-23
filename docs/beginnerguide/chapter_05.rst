5. Managing Futures
===================

Introduction to Futures
-----------------------

In Parsl, a future is a placeholder for the result of a task that has yet to be finished. Think of it as a ticket for a meal at a restaurant: you get the ticket immediately, but you have to wait for the meal to be prepared. Similarly, when you call a Parsl app, you get a future immediately, but you have to wait for the task to complete before accessing the result.
Futures allow you to write code without waiting for each task to finish before moving on to the next one. This can make your code more efficient, especially if you have many tasks that can be run in parallel.

Parsl provides two types of futures:

- **AppFutures**: Represent the execution of a Parsl app. You can use an AppFuture to check the status of a task, wait for it to finish, and get the result or any exceptions that occurred.
- **DataFutures**: Represent files produced by a Parsl app. They allow you to track the creation of output files and ensure they are ready before being used as inputs to other tasks.

AppFutures
----------

An AppFuture is a Python object that represents the execution of a Parsl app. When you call a Parsl app, it returns an AppFuture immediately, even if the app still needs to finish running. You can then use the AppFuture to interact with the app and its results.

Here are some of the things you can do with an AppFuture:

- **Check the app status**: You can use the `.done()` method to check if the app has finished running. This method returns True if the app has finished and False otherwise.
- **Wait for the app to finish**: You can use the `.result()` method to wait for the app to finish and get its result. This method will block your code until the app is done.
- **Get the result**: Once the app has finished, you can use the `.result()` method to get its result. If the app raises an exception, the `.result()` method will raise the same exception.
- **Cancel the app**: If you need to stop an app before it finishes, use the `.cancel()` method. This will attempt to cancel the app, but it's not guaranteed to succeed if the app has already started running.

DataFutures
-----------

A DataFuture is a special type of future representing a file produced by a Parsl app. When you call a Parsl app that produces output files, it returns an AppFuture whose `.outputs` attribute is a list of DataFutures. Each DataFuture in the list represents one of the output files.
You can use DataFutures in the same way as AppFutures, but they have some additional methods that are specific to files:

- **Get the filename**: You can use the `.filepath` property to get the path to the file the DataFuture represents.
- **Wait for the file to be created**: You can use the `.result()` method to wait for the file to be created and get a File object representing the file.

Passing Python Objects
----------------------

Parsl apps can communicate by passing Python objects as arguments and return values. Parsl uses the `dill` and `pickle` libraries to serialize Python objects, converting them into a format that can be sent over a network to other computers.
Most standard Python data types, such as numbers, strings, lists, and dictionaries, can be serialized by `dill` or `pickle`. However, some objects, such as those referencing external resources (e.g., open files or network connections), cannot be serialized.

SerializationError: Understanding Pickle vs. Dill
-------------------------------------------------

If Parsl encounters an object that it cannot serialize, it will raise a `SerializationError`. This can happen if you're trying to pass an unsupported object type or the object is too complex to be serialized. Parsl first attempts to serialize objects using `pickle`, and if that fails, it falls back to `dill`. Dill is a more powerful serialization library than `pickle`, and it can handle a wider range of object types. However, `dill` can also be slower than `pickle`, and it may not be available on all systems.

If you encounter a `SerializationError`, you can try the following troubleshooting steps:

- **Check if the object can be serialized using `pickle.dumps()`**: If `pickle.dumps()` raises a `TypeError`, the object is not serializable by `pickle`.
- **Simplify your objects**: If possible, try to simplify the objects you're passing to or returning from your app. For example, if you're passing a large dictionary, you could try passing only the keys or values that you need.
- **Use files**: If you can't simplify your objects, you can try using files to pass data between apps. This is a more general approach that can handle any type of data, but it may be less efficient than passing objects directly.
- **Implement custom serialization**: If you need to pass complex objects that are not supported by `pickle` or `dill`, you can implement custom serialization methods for those objects. This requires more advanced Python knowledge, but it gives you full control over how your objects are serialized.
- **Check for dynamically generated functions**: If the function being serialized has been dynamically generated or wrapped by Parsl, `pickle` may try to serialize it by storing its qualified name, assuming it can be imported remotely. However, this can fail if the function is not defined in a module that is available on the remote worker. To avoid this issue, you can try to define your functions in a separate module and import them into your Parsl script.

Staging Data Files
------------------

Parsl apps can take and return data files. A file may be passed as an input argument to an app or returned from an app after execution. Parsl provides support to automatically transfer (stage) files between the main Parsl program, worker nodes, and external data storage systems.
Input files can be passed as regular arguments, or a list of them may be specified in the special `inputs` keyword argument to an app invocation. Inside an app, the `.filepath` attribute of a `File` object can be read to determine where on the execution-side file system the input file has been placed. Output `File` objects must also be passed in at app invocation, through the `outputs` parameter. In this case, the `File` object specifies where Parsl should place output after execution. Inside an app, the `.filepath` attribute of an output `File` provides the path at which the corresponding output file should be placed so that Parsl can find it after execution.
If the output from an app is to be used as the input to a subsequent app, then a `DataFuture` that represents whether the output file has been created must be extracted from the first app's `AppFuture`, and that must be passed to the second app. This causes app executions to be properly ordered, in the same way that passing `AppFutures` to subsequent apps causes execution ordering based on an app returning. In a Parsl program, file handling is split in two. Files are named in an execution-location independent manner using `File` objects. Executors are configured to stage those files in and out of execution locations using instances of the `Staging` interface.

Parsl Files
-----------

Parsl uses a custom `File` class to provide a location-independent way of referencing and accessing files. Parsl files are defined by specifying the URL scheme and a path to the file. Thus a file may represent an absolute path on the submit-side file system or a URL to an external file.
The scheme defines the protocol via which the file may be accessed. Parsl supports the following schemes: `file`, `ftp`, `http`, `https`, and `globus`. If no scheme is specified, Parsl will default to the `file` scheme.

Staging Providers
-----------------

Parsl is able to transparently stage files between at-rest locations and execution locations by specifying a list of `Staging` instances for an executor. These staging instances define how to transfer files in and out of an execution location. This list should be supplied as the `storage_access` parameter to an executor when it is constructed.
Parsl includes several staging providers for moving files using the schemes defined above. By default, Parsl executors are created with three common staging providers: the `NoOpFileStaging` provider for local and shared file systems and the `HTTP(S)` and `FTP` staging providers for transferring files to and from remote storage locations.

Local/Shared File Systems (NoOpFileStaging)
-------------------------------------------

The `NoOpFileStaging` provider assumes that files specified either with a path or with the `file` URL scheme are available both on the submit and execution side. This occurs, for example, when there is a shared file system. In this case, files will not be moved, and the `File` object simply presents the same file path to the Parsl program and any executing tasks.

FTP, HTTP, HTTPS Staging
------------------------

Files named with the `ftp`, `http`, or `https` URL scheme will be staged in using HTTP GET or anonymous FTP commands. These commands will be executed as a separate Parsl task that will complete before the corresponding app executes. These providers cannot be used to stage output files.

Globus Staging
--------------

The `Globus` staging provider is used to transfer files that can be accessed using Globus. A file using the `Globus` scheme must specify the UUID of the Globus endpoint and a path to the file on the endpoint, for example:

.. code-block:: python

   File('globus://037f054a-15cf-11e8-b611-0ac6873fc732/unsorted.txt')

Note: A Globus endpoint's UUID can be found in the Globus Manage Endpoints page.

There must also be a Globus endpoint available with access to an execute-side file system because Globus file transfers happen between two Globus endpoints.

Globus Configuration
---------------------

To manage where files are staged, users must configure the default `working_dir` on a remote location. This information is specified in the `ParslExecutor` via the `working_dir` parameter in the `Config` instance. 

For example:

.. code-block:: python

   from parsl.config import Config
   from parsl.executors import HighThroughputExecutor

   config = Config(
       executors=[
           HighThroughputExecutor(
               working_dir="/home/user/data"
           )
       ]
   )

Parsl requires knowledge of the Globus endpoint that is associated with an executor. This is done by specifying the `endpoint_name` (the UUID of the Globus endpoint that is associated with the system) in the configuration.

In some cases, for example, when using a Globus shared endpoint or when a Globus endpoint is mounted on a supercomputer, the path seen by Globus is not the same as the local path seen by Parsl. In this case, the configuration may optionally specify a mapping between the `endpoint_path` (the common root path seen in Globus) and the `local_path` (the common root path on the local file system), as in the following. In most cases, `endpoint_path` and `local_path` are the same and do not need to be specified.

.. code-block:: python

   from parsl.config import Config
   from parsl.executors import HighThroughputExecutor
   from parsl.data_provider.globus import GlobusStaging
   from parsl.data_provider.data_manager import default_staging

   config = Config(
       executors=[
           HighThroughputExecutor(
               working_dir="/home/user/parsl_script",
               storage_access=default_staging + [GlobusStaging(
                   endpoint_uuid="7d2dc622-2edb-11e8-b8be-0ac6873fc732",
                   endpoint_path="/",
                   local_path="/home/user"
               )]
           )
       ]
   )

Globus Authorization
--------------------

To transfer files with Globus, the user must first authenticate. The first time Globus is used with Parsl on a computer, the program will prompt the user to follow an authentication and authorization procedure involving a web browser. Users can authorize out of band by running the `parsl-globus-auth` utility. This is useful, for example, when running a Parsl program in a batch system where it will be unattended.

.. code-block:: bash

   $ parsl-globus-auth

rsync Staging
-------------

The `rsync` utility can be used to transfer files in the `file` scheme in configurations where workers cannot access the submit-side file system directly, such as when executing on an AWS EC2 instance or on a cluster without a shared file system. However, the submit-side file system must be exposed using `rsync`.

rsync Configuration
^^^^^^^^^^^^^^^^^^^

`rsync` must be installed on both the submit and worker side. It can usually be installed by using the operating system package manager: for example, by `apt-get install rsync`.

An `RSyncStaging` option must then be added to the Parsl configuration file, as in the following. The parameter to `RSyncStaging` should describe the prefix to be passed to each `rsync` command to connect from workers to the submit-side host. This will often be the username and public IP address of the submitting system.

.. code-block:: python

   from parsl.data_provider.rsync import RSyncStaging
   from parsl.config import Config
   from parsl.executors import HighThroughputExecutor

   config = Config(
       executors=[
           HighThroughputExecutor(
               storage_access=[
                   HTTPInTaskStaging(), 
                   FTPInTaskStaging(), 
                   RSyncStaging("benc@example.com")  
               ],
               # ... other executor configuration options
           )
       ]
   )

rsync Authorization
^^^^^^^^^^^^^^^^^^^

The `rsync` staging provider delegates all authentication and authorization to the underlying `rsync` command. This command must be correctly authorized to connect back to the submit-side system. The form of this authorization will depend on the systems in question.

The following example installs an ssh key from the submit-side file system and turns off host key checking, in the `worker_init` initialization of an EC2 instance. The ssh key must have sufficient privileges to run `rsync` over ssh on the submit-side system.

.. code-block:: python

   with open("rsync-callback-ssh", "r") as f:
       private_key = f.read()

   ssh_init = """
   mkdir .ssh
   chmod go-rwx .ssh
   cat > .ssh/id_rsa <<EOF
   {private_key}
   EOF
   cat > .ssh/config <<EOF
   Host 
       StrictHostKeyChecking no
   EOF
   chmod go-rwx .ssh/id_rsa
   chmod go-rwx .ssh/config
   """.format(private_key=private_key)

   config = Config(
       executors=[
           HighThroughputExecutor(
               storage_access=[HTTPInTaskStaging(), FTPInTaskStaging(), RSyncStaging("benc@" + public_ip)],
               provider=AWSProvider(
                   ...,
                   worker_init=ssh_init
                   ...
               )
           )
       ]
   )

Practical Tutorial: Managing and Using Futures
----------------------------------------------

Let's illustrate how to manage and use futures in a Parsl script:

.. code-block:: python

   import parsl
   from parsl import python_app
   from parsl.data_provider.files import File  # Import the File class

   # ... (your config loading) ...

   @python_app
   def generate_file(filename, content):
       with open(filename, 'w') as f:
           f.write(content)
       return File(filename)  # Return a Parsl File object

   @python_app
   def process_file(file):
       with open(file.filepath, 'r') as f:  # Use file.filepath
           content = f.read()
       return content.upper()

   # Create the output file (Parsl will handle staging)
   output_file = File("output.txt")
   file_future = generate_file(output_file.filepath, "Hello, world!")

   # Chain the processing task
   result_future = process_file(file_future)

   # Get the result and print
   result = result_future.result()
   print(result)

In this example, the `generate_file` app creates a file and returns a `DataFuture`. The `process_file` app takes the `DataFuture` as input, waits for the file to be created, reads its contents, and returns the uppercase version of the contents. The main program then waits for the `result_future` to complete and prints the result.
