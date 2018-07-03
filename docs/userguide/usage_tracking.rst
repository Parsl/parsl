.. _label-usage-tracking:

Usage Statistics Collection
===========================

Parsl sends usage statistics back to the Parsl development team to measure worldwide usage and and improve
reliability and usability.


Why are we doing this?
----------------------

The Parsl development team receives support from government funding agencies. For the team to continue to
receive such funding, and for the agencies themselves to argue for funding, both the team and the agencies
must be able to demonstrate that the scientific community is benefiting from these investments. To this end,
we want to provide generic usage data about such things as the following:

* How many people use Parsl
* Average job length
* Parsl exit codes

To this end, we have added support to Parsl that allows installations to send us generic usage statistics.
By participating in this project, you help justify continuing support for the software on which you rely.
The data sent is as generic as possible (see :ref:`What is sent? <_what-is-sent>` below).

Opt-Out
-------

We have chosen opt-out collection rather than opt-in. The reason is that we need this data - it is a
requirement for funding. We believe have set a good balance between the benefits to the project and the
users by showing that Parsl works and is in use, which helps the project continue, and the costs to users
of providing generic information. To keep the cost low, we need to require zero additional effort.
By not opting out, and allowing these statistics to be reported back, you are explicitly supporting the
further development of Parsl.

If you must opt out of usage reporting, set ``PARSL_TRACKING=false`` in your environment.


.. _what-is-sent:

What is sent?
-------------

* Anonymized user ID
* Anonymized hostname
* Anonymized Parsl script ID
* Start and end times
* Parsl exit code
* Count of executors used

How is the data sent?
---------------------

The data is sent via UDP. While this may cause us to lose some data, it drastically reduces the possibility
that the usage statistics reporting will adversely affect the operation of the software.


When is the data sent?
----------------------

The data is sent twice per run, once when Parsl starts a script, and once when the script is completed.


What will the data be used for?
-------------------------------

The data will be used for answering questions such as:

* How many unique users are using Parsl?
* To determine patterns of usage - is activity increasing or decreasing?

We will also try and mine the data to answer operational questions such as:

* What percentage of the jobs run complete successfully?
* Of the ones that fail, what is the most common fault code returned?

Feedback
--------

Please send us your feedback at parsl@googlegroups.com. Feedback from our user communities will be
useful in determining our path forward with this in the future. We do ask that if you have concerns
or objections, please be specific in your feedback. For example, while saying "Our site has a policy
against sending such data" is good information for us to know in the future, a link to such a policy
would be even better.

