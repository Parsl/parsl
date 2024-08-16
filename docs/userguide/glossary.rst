Glossary of Parsl Terms
=======================

This glossary defines terms based on their usage within Parsl. By defining our terminology, we hope to create understanding across our community and reduce confusion. When asking for or providing support to fellow Parsl users, please use these terms as defined.

Our glossary is organized alphabetically in English. Feel free to contribute terms and definitions to this list that will benefit Parsl users.

.. _glossary:

.. _appglossary:
   
**App:**
----------

In Parsl, an app is a small, self-contained program that does a specific job. It's a piece of code, such as a Python function or a Bash script, that can run separately from your main program. Think of it as a mini-tool within your larger toolbox.

.. _appfutureglossary:

**AppFuture:**
-----------------

An AppFuture is a placeholder for the result of an app that runs in the background. It's like a ticket you get when you order food at a restaurant – you get the ticket right away, but you have to wait for the food to be ready. Similarly, you get an AppFuture immediately when you start an app, but you have to wait for the app to finish before you can see the results.

.. _bashappglossary:

**Bash App:**
---------------
   
A Bash app is a special kind of app in Parsl that lets you run commands from your computer's terminal (like the ones you type in the command prompt or shell). It's a way to use Parsl to automate tasks that you would normally do manually in the terminal.

.. _blockglossary:

**Block:**
------------

A block is like a group of computers loaned to you for doing work. Parsl will manage passing out work to each of them to do. This way, you can get your work done faster by using multiple computers at the same time.

.. _checkpointingglossary:

**Checkpointing:**
---------------------

Checkpointing is like saving your progress in a video game. If something goes wrong, you can restart from the last saved point (checkpoint) instead of starting all over again. In Parsl, checkpointing lets you save the state of your work so that you can resume it later if there's an interruption.

.. _concurrencyglossary:

**Concurrency:**
-------------------

Concurrency means doing multiple things at the same time. In Parsl, it means that your apps can run simultaneously, even if they're not on the same computer. This can make your programs run much faster.

.. _configurationglossary:

**Configuration:**
---------------------

Configuration sets up the rules for how Parsl should work. It's like adjusting the settings on your phone – you can choose how you want things to look and behave. In Parsl, you can configure things like how many computers to use, where to store data, and how to handle errors.

.. _datafutureglossary:

**DataFuture:**
------------------

A DataFuture is a placeholder for a file that an app is creating. It's like a receipt for a package you're expecting – you get the receipt right away, but you have to wait for the package to arrive. Similarly, you get a DataFuture immediately when an app starts creating a file, but you have to wait for the file to be finished before you can use it.

.. _dfkglossary:

**DataFlowKernel (DFK):**
------------------------------

The DataFlowKernel is like the brain of Parsl. It's the part that controls how your apps run and how they share information. It's like the conductor of an orchestra, making sure that all the musicians play together in harmony.

.. _elasticityglossary:

**Elasticity:**
-----------------

Elasticity means being able to stretch or shrink. In Parsl, it means that you can easily add or remove blocks of computers as needed. This is useful if your workload changes – you can use more computers when you have a lot of work to do and fewer computers when you don't.

.. _executionproviderglossary:

**Execution Provider:**
--------------------------

An execution provider is like a bridge between Parsl and the computers you want to use. It's the part that knows how to talk to different types of computers, like your laptop, a cluster, or a cloud service.

.. _executorglossary:

**Executor:**
----------------

An executor is like a manager for your apps. It's the part that decides which app should run on which computer and when. It's like a traffic controller, directing the flow of apps to make sure they all get where they need to go.

.. _futureglossary:

**Future:**
-------------

A future is a placeholder for the result of a task that hasn't finished yet. Both AppFuture and DataFuture are types of Futures. You can use the ``.result()`` method to get the actual result when it's ready.

.. _jobglossary:

**Job:**
---------

A job in Parsl refers to a unit of work that is submitted to an execution environment (like a cluster or cloud) for processing. It's like a task that needs to be done, such as running a script or processing data, and it can consist of one or more tasks that are executed on a compute resource.

.. _launcherglossary:

**Launcher:**
----------------

A launcher in Parsl is responsible for placing the workers onto each computer, preparing them to run the apps. It’s like a bus driver who brings the players to the stadium, ensuring they are ready to start, but not directly involved in telling them what to do once they arrive.

.. _managerglossary:

**Manager:**
--------------

A manager in Parsl is responsible for overseeing the execution of tasks on specific compute resources. It's like a supervisor who ensures that all workers (or workers within a block) are carrying out their tasks correctly and efficiently.

.. _memoizationglossary:

**Memoization:**
-------------------

Memoization is like remembering something so you don't have to do it again. In Parsl, if you are using memoization and you run an app with the same inputs multiple times, Parsl will remember the result from the first time and give it to you again instead of running the app again. This can save a lot of time.

.. _mpiappglossary:    

**MPI App:**
---------------

An MPI app is a special kind of app that uses a technology called Message Passing Interface (MPI) to communicate between different computers. It's like a walkie-talkie that lets different apps talk to each other.

.. _nodeglossary:

**Node:**
------------

A node in Parsl is like a workstation in a factory. It's the physical or virtual machine where work gets done. Each node provides the computational power needed to run tasks, and it can host several workers who carry out the tasks.

.. _parallelismglossary:

**Parallelism:**
-------------------

Parallelism means doing multiple things at the same time. In Parsl, it means that your apps can run simultaneously on different computers. This can make your programs run much faster.

.. _parslscriptglossary:    

**Parsl Script:**
---------------------

A Parsl script is a file that contains the instructions for how to run your apps in parallel. It's like a recipe that tells you what ingredients to use and how to combine them.

.. _pluginglossary:

**Plugin:**
---------------

A plugin is an add-on for Parsl. It's a piece of code that you can add to Parsl to give it new features or change how it works. It's like an extra tool that you can add to your toolbox.

.. _pythonappglossary: 

**Python App:**
------------------

A Python app is a special kind of app in Parsl that's written in the Python programming language. It's a way to use Parsl to run your Python code in parallel.

.. _resourceglossary:

**Resource:**
---------------

A resource in Parsl refers to any computational asset that can be used to execute tasks, such as CPU cores, memory, or entire nodes. It's like the tools and materials you need to get a job done.

.. _serializationglossary:    

**Serialization:**
--------------------

Serialization is like packing your belongings into a suitcase so you can take them on a trip. In Parsl, it means converting your data into a format that can be sent over a network to another computer.

.. _stagingglossary:    

**Staging:**
---------------

Staging is like setting the stage for a play. In Parsl, it means preparing the data that your apps need before they start running. This can involve things like copying files to the right location or converting them into the right format.

.. _taskglossary:

**Task:**
------------

A task in Parsl is the smallest unit of work that can be executed. It's like a single step in a larger process, where each task is part of a broader workflow or job.

.. _threadglossary:    

**Thread:**
-------------

A thread is like a smaller part of a program that can run independently. It's like a worker in a factory who can do their job at the same time as other workers.

.. _workerglossary:

**Worker:**
-------------

A worker in Parsl is like an employee in the factory who does the actual work. Workers run on nodes and are responsible for executing the tasks assigned to them. Multiple workers can work on a single node, sharing the node’s resources to get the job done efficiently.

.. _workflowglossary:    

**Workflow:**
----------------

A workflow is like a series of steps that you follow to complete a task. In Parsl, it's a way to describe how your apps should run and how they depend on each other. It's like a flowchart that shows you the order in which things need to happen.
