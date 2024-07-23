15. Usage Statistics Collection
===============================

Parsl includes a system to gather anonymous information about how people use it. This helps the Parsl team understand how the software is being used and improve it.

Purpose of Data Collection
--------------------------

The Parsl team uses this information to:

- See how many people are using Parsl.
- Understand how Parsl is typically used (e.g., what kinds of tasks are run, how long they take).
- Find and fix problems in the software.

Opt-In Mechanism
----------------

Parsl only collects this information if given permission. You can choose to share this information by setting `PARSL_TRACKING=true` in your computer's environment or by setting `usage_tracking=True` in your Parsl configuration.

Data Collected and Usage
------------------------

The information Parsl collects includes:

- Your computer's internet address (IP address).
- Unique code every time you run Parsl.
- The start and end times of your Parsl runs.
- The number of executors you used.
- The number of errors that occurred.
- The versions of Parsl and Python you are using.
- Your computer's operating system and its version.

This information is only used to improve Parsl and is not shared with anyone outside the Parsl team.

Sending and Utilizing Data
--------------------------

The information is sent over the internet using a method called UDP. This method is fast and doesn't slow down your Parsl programs. The data is sent twice: once when you start Parsl and once when it finishes.

The Parsl team uses the collected data to create reports showing how Parsl is being used. They also use it to find and fix software bugs.

Providing Feedback
------------------

If you have any questions or concerns about Parsl's usage statistics collection, you can contact the Parsl team at parsl@googlegroups.com.
