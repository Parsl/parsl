Packaging
---------

Currently packaging is managed by Yadu.

Here are the steps:

.. code:: bash

   # Create a new git tag :
   git tag <MAJOR>.<MINOR>.<BUG_REV>
   # Push tag to github :
   git push origin <TAG_NAME>

   # Depending on permission all of the following might have to be run as root.
   sudo su

   # Make sure to have twine installed
   pip3 install twine

   # Create a source distribution
   python3 setup.py sdist

   # Create a wheel package, which is a prebuilt package
   python3 setup.py bdist_wheel

   # Upload the package with twine
   twine upload dist/*
