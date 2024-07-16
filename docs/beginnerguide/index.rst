Table of Contents
=================

Chapter 1 - Introduction to Parsl  
------------------------------------------------

- Overview of Parsl
- What is Parsl and what can it be used for?
- Script vs Workflow
- Key Features and Benefits
- Understanding Concurrency and Parallelism
- Concurrency vs. Parallelism
- How Parsl Facilitates Parallel Computing
- Glossary of Terms
- Practical Example: Setting Up Your First Parsl Workflow
- Infographic: Script vs Workflow

Chapter 2 - Getting Started with Parsl
------------------------------------------------

- Installation and Setup
- System Requirements and Dependencies
- Installing Parsl
- Basic Configuration
- First Steps
- Writing a Parsl Script
- Parsl Script Basic Workflow
- Practical Tutorial: Hello World with Parsl
- Getting Started Tutorial
- Diagram: Basic Parsl Script Flow
- Practical Example: Setting up your first Parsl Workflow
- Link to Quickstart

Chapter 3 - Core Concepts 
------------------------------------------------

- Core Concepts
- Parsl and Concurrency
- Introduction to Futures
- Understanding AppFutures and DataFutures
- Parsl and Execution
- Execution Providers, Executors, and Launchers
- Blocks and Elasticity
- Parsl and Communication
- Parameter Passing
- File Passing Mechanisms
- Interactive Tutorial: Running Your First Parallel Task
- Visual Guide: How Parsl Manages Concurrency

Chapter 4 - Working with Apps 
------------------------------------------------

- Working with Apps
- Python Apps
- Creating Python Apps
- Rules for Function Contents
- Using Functions from Modules
- Inputs and Outputs Handling
- Special Keyword Arguments
- Execution Options
- Limitations
- Practical Example: Data Analysis with Python Apps
- Bash Apps
- Creating Bash Apps
- Rules for Function Contents
- Inputs and Outputs Handling
- Special Keyword Arguments
- Execution Options
- Practical Example: Automating System Tasks with Bash Apps
- MPI Apps
- Background on MPI
- Writing and Configuring MPI Apps
- Practical Example: Running MPI Workflows
- Flowchart: App Creation Process

Chapter 5 - Managing Futures 
------------------------------------------------

- Managing Futures
- Introduction to Futures
- AppFutures
- DataFutures
- Passing Python Objects
- SerializationError: Understanding Pickle vs. Dill
- Staging Data Files
- Parsl Files
- Staging Providers
- Local/Shared File Systems (NoOpFileStaging)
- FTP, HTTP, HTTPS Staging
- Globus Staging
- rsync Staging
- Practical Tutorial: Managing and Using Futures
- Infographic: Data Staging in Parsl

Chapter 6 - Configuration 
------------------------------------------------

- Configuration
- Creating and Using Config Objects
- Configuring for Different Environments
- Heterogeneous Resources
- Accelerators
- Multi-Threaded Applications
- Ad-Hoc Clusters
- Encryption and Performance
- Steps for configuring Parsl on 10 common supercomputer setups
- Diagram: Configuration Options Overview

Chapter 7 - Parsl Configuration Examples 
------------------------------------------------

- Parsl Configuration Examples for Common Environments
- Cloud
- General Configuration
- AWS
- Step-by-Step Configuration
- Tuning Tips
- HPC
- General Configuration
- NERSC
- Step-by-Step Configuration
- Tuning Tips

Chapter 8 - Execution Environment 
------------------------------------------------

- Execution Environment
- Memory Environment
- File System Environment
- Service Environment
- Summary of Environments
- Interactive Tutorial: Setting Up Your Execution Environment
- Visual Guide: Understanding Execution Environments

Chapter 9 - Error Handling 
------------------------------------------------

- Error Handling
- Understanding Exceptions
- Retries and Lazy Fail
- Implementing Retry Handlers
- Practical Example: Error Handling in Parsl Scripts
- Flowchart: Error Handling Workflow

Chapter 10 - Memoization and Checkpointing 
------------------------------------------------

- Memoization and Checkpointing
- App Caching
- App Equivalence
- Invocation Equivalence
- Ignoring Arguments
- Caveats
- Checkpointing
- Creating Checkpoints
- Resuming from Checkpoints
- Practical Tutorial: Using Memoization and Checkpointing
- Infographic: How Checkpointing Works

Chapter 11 - Performance Optimization
------------------------------------------------

- Performance Optimization
- Prefetching
- GPU and CPU Affinity
- File Transfers
- Collecting and Utilizing Optimization Techniques

Chapter 12 - Monitoring and Visualization 
------------------------------------------------

- Monitoring and Visualization
- Monitoring Configuration
- Visualization Tools
- Workflow Summary and Page
- Practical Tutorial: Setting Up Monitoring and Visualization
- Visual Guide: Monitoring Your Parsl Workflows

Chapter 13 - Example Parallel Patterns 
------------------------------------------------

- Example Parallel Patterns
- Bag of Tasks
- Sequential Workflows
- Parallel Workflows
- Parallel Workflows with Loops
- MapReduce
- Caching Expensive Initialization Between Tasks
- Practical Example: Implementing MapReduce with Parsl
- Infographic: Common Parallel Patterns
- Lifted Operators
- Lifted [] Operator
- Lifted . Operator
- Join Apps
- Using Sub-Workflows
- Integrating Futures from Other Components
- Practical Example: Advanced Parsl Techniques
- Diagram: Advanced Parsl Workflow Structures

Chapter 14 - Structuring Parsl Programs 
------------------------------------------------

- Structuring Parsl Programs

Chapter 15 - Usage Statistics Collection 
------------------------------------------------

- Usage Statistics Collection
- Purpose of Data Collection
- Opt-In Mechanism
- Data Collected and Usage
- Sending and Utilizing Data
- Providing Feedback
- Practical Guide: Enabling Usage Statistics Collection
- Visual Guide: How Usage Data is Collected and Used

Chapter 16 - Extending Parsl with Plugins 
------------------------------------------------

- Extending Parsl with Plugins
- Overview of Plugins
- Executors
- Providers, Launchers, and Channels
- File Staging Plugins
- Default stdout/stderr Name Generation
- Memoization/Checkpointing
- Dependency Resolution
- Invoking Other Asynchronous Components
- Practical Tutorial: Writing Your Own Plugin
- Diagram: Plugin Architecture

Chapter 17 - Measuring Performance 
------------------------------------------------

- Measuring Performance with parsl-perf
- Introduction to parsl-perf
- Setting Up and Using parsl-perf
- Analyzing Performance Metrics
- Practical Example: Performance Tuning with parsl-perf
- Infographic: Performance Measurement Tools

Chapter 18 - Further Help 
------------------------------------------------

- Further Help and Resources
- Community Forums and Support Channels
- Additional Documentation and Tutorials
- Example Code and Configuration Files
- Visual Guide: Navigating Parsl Resources

Chapter 19 - FAQ 
------------------------------------------------

- FAQ
- Parsl vs. Globus Compute Workflows
- Common User Questions and Solutions
