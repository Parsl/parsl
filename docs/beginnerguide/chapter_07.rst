7. Parsl Configuration Examples for Common Environments
=======================================================

Cloud
-----

General Configuration
^^^^^^^^^^^^^^^^^^^^^

Parsl can be configured to run on various cloud platforms, including Amazon Web Services (AWS) and Google Cloud Platform (GCP). The general steps for configuring Parsl on a cloud platform are as follows:

1. **Choose a provider**: Select the appropriate provider for your cloud platform (e.g., `AWSProvider` for AWS, `GoogleCloudProvider` for GCP).
2. **Setup authentication**: Configure the provider with your cloud credentials, such as access keys or service account information.
3. **Specify instance details**: Choose the type of virtual machines you want to use, the region or zone where you want to launch them, and any other relevant options.
4. **Configure the executor**: Use the `HighThroughputExecutor` to manage the execution of your Parsl apps on the cloud instances.
5. **Load the configuration**: Load the configuration into Parsl using `parsl.load(config)`.

AWS
^^^

Step-by-Step Configuration
^^^^^^^^^^^^^^^^^^^^^^^^^^^

Install Parsl with AWS dependencies:

.. code-block:: bash

   python3 -m pip install 'parsl[aws]'

Create a Config object:

.. code-block:: python

   from parsl.config import Config
   from parsl.providers import AWSProvider
   from parsl.executors import HighThroughputExecutor

   config = Config(
       executors=[
           HighThroughputExecutor(
               label='ec2_single_node',
               provider=AWSProvider(
                   image_id='YOUR_AMI_ID',  # Replace with your AMI ID
                   region='YOUR_AWS_REGION',  # Replace with your AWS region
                   key_name='YOUR_KEY_NAME',  # Replace with your key name
                   nodes_per_block=1,
                   init_blocks=1,
                   max_blocks=1,
               ),
           )
       ]
   )

Load the configuration:

.. code-block:: python

   parsl.load(config)

Tuning Tips
^^^^^^^^^^^

- **Choose the right instance type**: Select an instance type that matches your workload's requirements for CPU, memory, and storage.
- **Use spot instances**: Spot instances can be significantly cheaper than on-demand instances, but they can be interrupted by AWS if the spot price exceeds your bid.
- **Optimize data transfer**: If your workflow involves transferring large amounts of data, consider using a data transfer service like Globus to improve performance.

HPC
---

General Configuration
^^^^^^^^^^^^^^^^^^^^^

Configuring Parsl for High-Performance Computing (HPC) environments typically involves the following steps:

1. **Load Modules**: Load the necessary modules for Parsl and your Python environment. This often includes commands like `module load python` and `module load parsl`.
2. **Choose a provider**: Select the provider that matches your HPC system's scheduler (e.g., `SlurmProvider` for Slurm, `TorqueProvider` for Torque/PBS).
3. **Setup authentication**: If required, configure the provider with your HPC credentials.
4. **Specify resource details**: Set the number of nodes per block, the walltime, and any other scheduler-specific options.
5. **Configure the executor**: Use the `HighThroughputExecutor` to manage the execution of your Parsl apps on the HPC nodes.
6. **Load the configuration**: Load the configuration into Parsl using `parsl.load(config)`.

NERSC (Perlmutter)
^^^^^^^^^^^^^^^^^^

Step-by-Step Configuration
^^^^^^^^^^^^^^^^^^^^^^^^^^

Load Modules: Load the required modules for Parsl and your Python environment.

Create a Config object:

.. code-block:: python

   from parsl.config import Config
   from parsl.executors import HighThroughputExecutor
   from parsl.providers import SlurmProvider
   from parsl.launchers import SrunLauncher
   from parsl.addresses import address_by_interface

   config = Config(
       executors=[
           HighThroughputExecutor(
               label="NERSC_HTEX",
               working_dir="/global/homes/<YOUR_USERNAME>",  # Use your home directory
               address=address_by_interface('ib0'),
               provider=SlurmProvider(
                   partition="regular",  # Replace with your partition
                   account="YOUR_PROJECT_ALLOCATION",  # Replace with your project ID
                   launcher=SrunLauncher(),
                   walltime="00:30:00",  # Adjust walltime as needed
                   nodes_per_block=2,  # Adjust nodes per block as needed
               ),
           )
       ]
   )

Load the configuration:

.. code-block:: python

   parsl.load(config)

Tuning Tips
^^^^^^^^^^^

- **Choose the right partition**: NERSC offers different partitions with varying resource limits. Choose the partition that best suits your needs.
- **Specify constraints**: If you need specific types of nodes (e.g., GPUs), use the constraint option in the `SlurmProvider`.
- **Monitor resource use**: Keep an eye on your resource use to avoid exceeding your allocation.
