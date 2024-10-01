.. _label-usage-tracking:

Usage Statistics Collection
===========================

Parsl uses an **Opt-in** model for usage tracking, allowing users to decide if they wish to participate. Usage statistics are crucial for improving software reliability and help focus development and maintenance efforts on the most used components of Parsl. The collected data is used solely for enhancements and reporting and is not shared in its raw form outside of the Parsl team.

Why are we doing this?
----------------------

The Parsl development team relies on funding from government agencies. To sustain this funding and advocate for continued support, it is essential to show that the research community benefits from these investments.

By opting in to share usage data, you actively support the ongoing development and maintenance of Parsl. (See:ref:`What is sent? <what-is-sent>` below).

Opt-In Model
------------

We use an **opt-in model** for usage tracking to respect user privacy and provide full control over shared information. We hope that developers and researchers will choose to send us this information. The reason is that we need this data - it is a requirement for funding.

Choose the data you share with Usage Tracking Levels.

**Usage Tracking Levels:**

* **Level 1:** Only basic information such as Python version, Parsl version, and platform name (Linux, MacOS, etc.)
* **Level 2:** Level 1 information and configuration information including provider, executor, and launcher names.
* **Level 3:** Level 2 information and workflow execution details, including the number of applications run, failures, and execution time.

By enabling usage tracking, you support Parsl's development. 

**To opt-in, set** ``usage_tracking`` **to the desired level (1, 2, or 3) in the configuration object** (``parsl.config.Config``) **.**

Example:

.. code-block:: python3

    config = Config(
        executors=[
            HighThroughputExecutor(
                ...
            )
        ],
        usage_tracking=3
    )

.. _what-is-sent:

What is sent?
-------------

The data collected depends on the tracking level selected:

* **Level 1:** Only basic information such as Python version, Parsl version, and platform name (Linux, MacOS, etc.)
* **Level 2:** Level 1 information and configuration information including provider, executor, and launcher names.
* **Level 3:** Level 2 information and workflow execution details, including the number of applications run, failures, and execution time.

**Example Messages:**

- At launch:

  .. code-block:: json

    {
       "correlator":"6bc7484e-5693-48b2-b6c0-5889a73f7f4e",
       "parsl_v":"1.3.0-dev",
       "python_v":"3.12.2",
       "platform.system":"Darwin",
       "tracking_level":3,
       "components":[
          {
             "c":"parsl.config.Config",
             "executors_len":1,
             "dependency_resolver":false
          },
          "parsl.executors.threads.ThreadPoolExecutor"
       ],
       "start":1727156153
    }

- On closure (Tracking Level 3 only):

  .. code-block:: json

    {
       "correlator":"6bc7484e-5693-48b2-b6c0-5889a73f7f4e",
       "execution_time":31,
       "components":[
          {
             "c":"parsl.dataflow.dflow.DataFlowKernel",
             "app_count":3,
             "app_fails":0
          },
          {
             "c":"parsl.config.Config",
             "executors_len":1,
             "dependency_resolver":false
          },
          "parsl.executors.threads.ThreadPoolExecutor"
       ],
       "end":1727156156
    }

**All messages sent are logged in the** ``parsl.log`` **file, ensuring complete transparency.**

How is the data sent?
---------------------

Data is sent using **UDP** to minimize the impact on workflow performance. While this may result in some data loss, it significantly reduces the chances of usage tracking affecting the software's operation.

The data is processed through AWS CloudWatch to generate a monitoring dashboard, providing valuable insights into usage patterns.

When is the data sent?
----------------------

Data is sent twice per run:

1. At the start of the script.
2. Upon script completion (for Tracking Level 3).

What will the data be used for?
-------------------------------

The data will help the Parsl team understand Parsl usage and make development and maintenance decisions, including:

* Focus development and maintenance on the most-used components of Parsl.
* Determine which Python versions to continue supporting.
* Track the age of Parsl installations.
* Assess how long it takes for most users to adopt new changes.
* Track usage statistics to report to funders.

Usage Statistics Dashboard
--------------------------

The collected data is aggregated and displayed on a publicly accessible dashboard. This dashboard provides an overview of how Parsl is being used across different environments and includes metrics such as:

* Total workflows executed over time
* Most-used Python and Parsl versions
* Most common platforms and executors and more

`Find the dashboard here <https://cloudwatch.amazonaws.com/dashboard.html?dashboard=Parsl-Usage-Tracking-Stats&context=eyJSIjoidXMtZWFzdC0xIiwiRCI6ImN3LWRiLTA0Njc5ODQ4MjQwNiIsIlUiOiJ1cy1lYXN0LTFfNW41R1BwYVd0IiwiQyI6IjN2bzJmbzAxYnI1dm92YjY2dGEwcmo2dmNkIiwiSSI6InVzLWVhc3QtMTplMjYyZGZkMy05NjI2LTQ4YTMtYjBkOC1jYWYwYWU1NzA4M2EiLCJPIjoiYXJuOmF3czppYW06OjA0Njc5ODQ4MjQwNjpyb2xlL3NlcnZpY2Utcm9sZS9DV0RCU2hhcmluZy1QdWJsaWNSZWFkT25seUFjY2Vzcy1UTlBOMk5COSIsIk0iOiJQdWJsaWMifQ==&start=PT3H&end=null>`_

Leaderboard
-----------

**Opting in to usage tracking also allows you to participate in the Parsl Leaderboard.
To participate in the leaderboard, you can deanonymize yourself using the** ``project_name`` **parameter in the parsl configuration object** (``parsl.config.Config``) **.**

`Find the Parsl Leaderboard here <https://cloudwatch.amazonaws.com/dashboard.html?dashboard=Parsl-Usage-Tracking-Stats&context=eyJSIjoidXMtZWFzdC0xIiwiRCI6ImN3LWRiLTA0Njc5ODQ4MjQwNiIsIlUiOiJ1cy1lYXN0LTFfNW41R1BwYVd0IiwiQyI6IjN2bzJmbzAxYnI1dm92YjY2dGEwcmo2dmNkIiwiSSI6InVzLWVhc3QtMTplMjYyZGZkMy05NjI2LTQ4YTMtYjBkOC1jYWYwYWU1NzA4M2EiLCJPIjoiYXJuOmF3czppYW06OjA0Njc5ODQ4MjQwNjpyb2xlL3NlcnZpY2Utcm9sZS9DV0RCU2hhcmluZy1QdWJsaWNSZWFkT25seUFjY2Vzcy1UTlBOMk5COSIsIk0iOiJQdWJsaWMifQ==&start=PT3H&end=null>`_

Example:

.. code-block:: python3

    config = Config(
        executors=[
            HighThroughputExecutor(
                ...
            )
        ],
        usage_tracking=3,
        project_name="my-test-project"
    )

Every run of parsl with usage tracking **Level 1** or **Level 2** earns you **1 point**. And every run with usage tracking **Level 3**, earns you **2 points**.
 
Feedback
--------

Please send us your feedback at parsl@googlegroups.com. Feedback from our user communities will be 
useful in determining our path forward with usage tracking in the future.

**Please consider turning on usage tracking to support the continued development of Parsl.**
