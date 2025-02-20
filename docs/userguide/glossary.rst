Glossary
========

This glossary defines terms based on their usage within Parsl. By defining our terminology, we hope to create understanding across our community and reduce confusion. When asking for or providing support to fellow Parsl users, please use these terms as defined.

Our glossary is organized alphabetically in English. Feel free to contribute terms and definitions to this list that will benefit Parsl users.

.. _glossary:

.. _appglossary:
   
App
---

In Parsl, an app is a small, self-contained program that does a specific job. It's a piece of code, such as a Python function or a Bash script, that can run separately from your main program. Think of it as a mini-tool within your larger toolbox.

.. _appfutureglossary:

AppFuture
---------

An AppFuture is a placeholder for the result of an app that runs in the background. It's like a ticket you get when you order food at a restaurant – you get the ticket right away, but you have to wait for the food to be ready. Similarly, you get an AppFuture immediately when you start an app, but you have to wait for the app to finish before you can see the results.

.. _bashappglossary:

Bash App
--------
   
A Bash app is a special kind of app in Parsl that lets you run commands from your computer's terminal (like the ones you type in the command prompt or shell). It's a way to use Parsl to automate tasks that you would normally do manually in the terminal.

.. _blockglossary:

Block
-----

A block is a group of resources, such as nodes or computational units, allocated for executing tasks. Parsl manages the distribution of work across these resources to expedite task completion.

.. _checkpointingglossary:

Checkpointing
-------------

Checkpointing is like saving your progress in a video game. If something goes wrong, you can restart from the last saved point instead of starting over. In Parsl, checkpointing saves the state of your work so you can resume it later if interrupted.

.. _concurrencyglossary:

Concurrency
-----------

Concurrency means doing multiple things at the same time. In Parsl, it enables your apps to run in parallel across different resources, significantly speeding up program execution. It's like a chef preparing multiple dishes in a single kitchen, switching between all of them quickly.

.. _configurationglossary:

Configuration
-------------

Configuration sets up the rules for how Parsl should work. It's like adjusting the settings on your phone – you can choose how you want things to look and behave. In Parsl, you can configure things like how many resources to use, where to store data, and how to handle errors.

.. _datafutureglossary:

DataFuture
----------

A DataFuture is a placeholder for a file that an app is creating. It's like a receipt for a package you're expecting – you get the receipt right away, but you have to wait for the package to arrive. Similarly, you get a DataFuture immediately when an app starts creating a file, but you have to wait for the file to be finished before you can use it.

.. _dfkglossary:

DataFlowKernel (DFK)
--------------------

The DataFlowKernel is like the brain of Parsl. It's the part that controls how your apps run and how they share information. It's like the conductor of an orchestra, making sure that all the musicians play together in harmony.

.. _elasticityglossary:

Elasticity
----------

Elasticity refers to the ability to scale resources up or down as needed. In Parsl, it allows you to add or remove blocks of computational resources based on workload demands.

.. _executionproviderglossary:

Execution Provider
------------------

An execution provider acts as a bridge between Parsl and the resources you want to use, such as your laptop, a cluster, or a cloud service. It handles communication with these resources to execute tasks.

.. _executorglossary:

Executor
--------

An executor is a manager that determines which app runs on which resource and when. It directs the flow of apps to ensure efficient task execution. It's like a traffic controller, directing the flow of apps to make sure they all get where they need to go.

.. _futureglossary:

Future
------

A future is a placeholder for the result of a task that hasn't finished yet. Both AppFuture and DataFuture are types of Futures. You can use the ``.result()`` method to get the actual result when it's ready.

.. _jobglossary:

Job
---

A job in Parsl is a unit of work submitted to an execution environment (such as a cluster or cloud) for processing. It can consist of one or more apps executed on computational resources.

.. _launcherglossary:

Launcher
--------

A launcher in Parsl is responsible for placing the workers onto each computer, preparing them to run the apps. It’s like a bus driver who brings the players to the stadium, ensuring they are ready to start, but not directly involved in telling them what to do once they arrive.

.. _managerglossary:

Manager
-------

A manager in Parsl is responsible for overseeing the execution of tasks on specific compute resources. It's like a supervisor who ensures that all workers (or workers within a block) are carrying out their tasks correctly and efficiently.

.. _memoizationglossary:

Memoization
-----------

Memoization is like remembering something so you don't have to do it again. In Parsl, if you are using memoization and you run an app with the same inputs multiple times, Parsl will remember the result from the first time and give it to you again instead of running the app again. This can save a lot of time.

.. _mpiappglossary:    

MPI App
-------

An MPI app is a specialized app that uses the Message Passing Interface (MPI) for communication, which can occur both across nodes and within a single node. MPI enables different parts of the app to communicate and coordinate their activities, similar to how a walkie-talkie allows different teams to stay in sync.

.. _nodeglossary:

Node
----

A node in Parsl is like a workstation in a factory. It's a physical or virtual machine that provides the computational power needed to run tasks. Each node can host several workers that execute tasks.

.. _parallelismglossary:

Parallelism
-----------

Parallelism means doing multiple things at the same time but not necessarily in the same location or using the same resources. In Parsl, it involves running apps simultaneously across different nodes or computational resources, accelerating program execution. Unlike concurrency which is like a chef preparing multiple dishes in a single kitchen, parallelism is like multiple chefs preparing different dishes in separate kitchens, at the same time.

.. _parslscriptglossary:    

Parsl Script
------------

A Parsl script is a Python program that uses the Parsl library to define and run apps in parallel. It's like a recipe that tells you what ingredients to use and how to combine them.

.. _pluginglossary:

Plugin
------

A plugin is an add-on for Parsl. It's a piece of code that you can add to Parsl to give it new features or change how it works. It's like an extra tool that you can add to your toolbox.

.. _pythonappglossary: 

Python App
----------

A Python app is a special kind of app in Parsl that's written as a Python function. It's a way to use Parsl to run your Python code in parallel.

.. _resourceglossary:

Resource
--------

A resource in Parsl refers to any computational asset that can be used to execute tasks, such as CPU cores, memory, or entire nodes. It's like the tools and materials you need to get a job done. Resources, often grouped in nodes or clusters, are essential for processing workloads.

.. _serializationglossary:    

Serialization
-------------

Serialization is like packing your belongings into a suitcase so you can take them on a trip. In Parsl, it means converting your data into a format that can be sent over a network to another computer.

.. _stagingglossary:    

Staging
-------

Staging in Parsl involves moving data to the appropriate location before an app starts running and can also include moving data back after the app finishes. This process ensures that all necessary data is available where it needs to be for the app to execute properly and that the output data is returned to a specified location once the execution is complete.

.. _taskglossary:

Task
----

A task in Parsl is the execution of an app, it is the smallest unit of work that can be executed. It's like a single step in a larger process, where each task is part of a broader workflow or job.

.. _threadglossary:    

Thread
------

A thread is like a smaller part of a program that can run independently. It's like a worker in a factory who can do their job at the same time as other workers. Threads are commonly used for parallelism within a single node.

.. _workerglossary:

Worker
------

A worker in Parsl is an independent process that runs on a node to execute tasks. Unlike threads, which share resources within a single process, workers operate as separate entities, each potentially handling different tasks on the same or different nodes.

.. _workflowglossary:    

Workflow
--------

A workflow is like a series of steps that you follow to complete a task. In Parsl, it's a way to describe how your apps should run and how they depend on each other, like a flowchart that shows you the order in which things need to happen. A workflow is typically expressed in a Parsl script, which is a Python program that leverages the Parsl library to orchestrate these tasks in a structured manner.
