Changelog
=========

Libsubmit 0.4.1
---------------

Released. June 18th, 2018.
This release folds in massive contributions from @annawoodard.

New functionality
^^^^^^^^^^^^^^^^^

* Several code cleanups, doc improvements, and consistent naming

* All providers have the initialization and actual start of resources decoupled.



Libsubmit 0.4.0
---------------

Released. May 15th, 2018.
This release folds in contributions from @ahayschi, @annawoodard, @yadudoc

New functionality
^^^^^^^^^^^^^^^^^

* Several enhancements and fixes to the AWS cloud provider (#44, #45, #50)

* Added support for python3.4


Bug Fixes
^^^^^^^^^

* Condor jobs left in queue with X state at end of completion  `issue#26 <https://github.com/Parsl/libsubmit/issues/26>`_

* Worker launches on Cori seem to fail from broken ENV `issue#27 <https://github.com/Parsl/libsubmit/issues/27>`_

* EC2 provider throwing an exception at initial run `issue#46 <https://github.com/Parsl/parsl/issues/46>`_

