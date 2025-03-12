Roadmap
=======

**OVERVIEW**

While we follow best practices in software development processes (e.g., CI, flake8, code review), there are opportunities to make our code more maintainable and   accessible. This roadmap, written in the fall of 2023, covers our major activities planned through 2025 to increase efficiency, productivity, user experience, and community building.

Features and improvements are documented via GitHub
`issues <https://github.com/Parsl/parsl/issues>`_ and `pull requests <https://github.com/Parsl/parsl/pulls>`_.


Code Maintenance
----------------

* **Type Annotations and Static Type Checking**: Add static type annotations throughout the codebase and add typeguard checks.
* **Release Process**: `Improve the overall release process <https://github.com/Parsl/parsl/issues?q=is%3Aopen+is%3Aissue+label%3Arelease_process>`_ to synchronize docs and code releases, automatically produce changelog documentation.
* **Components Maturity Model**: Defines the `component maturity model <https://github.com/Parsl/parsl/issues/2554>`_ and tags components with their appropriate maturity level.
* **Define and Document Interfaces**: Identify and document interfaces via which `external components <https://parsl.readthedocs.io/en/stable/userguide/advanced/plugins.html>`_ can augment the Parsl ecosystem.
* **Distributed Testing Process**: All tests should be run against all possible schedulers, using different executors, on a variety of remote systems. Explore the use of containerized schedulers and remote testing on real systems.

New Features and Integrations
-----------------------------

* **Enhanced MPI Support**: Extend Parsl’s MPI model with MPI apps and runtime support capable of running MPI apps in different environments (MPI flavor and launcher).
* **Serialization Configuration**: Enable users to select what serialization methods are used and enable users to supply their own serializer.
* **PSI/J integration**: Integrate PSI/J as a common interface for schedulers.
* **Internal Concurrency Model**: Revisit and rearchitect the concurrency model to reduce areas that are not well understood and reduce the likelihood of errors.
* **Common Model for Errors**: Make Parsl errors self-describing and understandable by users.
* **Plug-in Model for External Components**: Extend Parsl to implement interfaces defined above. 
* **User Configuration Validation Tool**: Provide tooling to help users configure Parsl and diagnose and resolve errors.
* **Anonymized Usage Tracking**: Usage tracking is crucial for our data-oriented approach to understand the adoption of Parsl, which components are used, and where errors occur. This allows us to prioritize investment in components, progress components through the maturity levels, and identify bugs. Revisit prior usage tracking and develop a service that enables users to control tracking information.
* **Support for Globus Compute**: Enable execution of Parsl tasks using Globus Compute as an executor.
* **Update Globus Data Management**: Update Globus integration to use the new Globus Connect v5 model (i.e., needing specific scopes for individual endpoints).
* **Performance Measurement**: Improve ability to measure performance metrics and report to users.
* **Enhanced Debugging**: Application-level `logging <https://github.com/Parsl/parsl/issues/1984>`_ to understand app execution. 

Tutorials, Training, and User Support
-------------------------------------

* **Configuration and Debugging**: Tutorials showing how to configure Parsl for different resources and debug execution. 
* **Functional Serialization 101**: Tutorial describing how serialization works and how you can integrate custom serializers. 
* **ProxyStore Data Management**: Tutorial showing how you can use ProxyStore to manage data for both inter and intra-site scenarios.
* **Open Dev Calls on Zoom**: The internal core team holds an open dev call/office hours every other Thursday to help users troubleshoot issues, present and share their work, connect with each other, and provide community updates.
* **Project Documentation**: is maintained and updated in `Read the Docs <https://parsl.readthedocs.io/en/stable/index.html>`_.

Longer-term Objectives
----------------------

* **Globus Compute Integration**: Once Globus Compute supports multi-tenancy, Parsl will be able to use it to run remote tasks on initially one and then later multiple resources.
* **Multi-System Optimization**: Once Globus Compute integration is complete, it is best to use multiple systems for multiple tasks as part of a single workflow.
* **HPC Checkpointing and Job Migration**: As new resources become available, HPC tasks will be able to be checkpointed and moved to the system with more resources.
