Packaging
---------

Currently packaging is managed by @annawoodard and @yadudoc.

Steps to release

1. Update the version number in ``parsl/parsl/version.py``
2. Check the following files to confirm new release information
   * ``parsl/setup.py``
   * ``requirements.txt``
   * ``parsl/docs/devguide/changelog.rst``
   * ``parsl/README.rst``

3. Commit and push the changes to github
4. Run the ``tag_and_release.sh`` script. This script will verify that
   version number matches the version specified.

   .. code:: bash

      ./tag_and_release.sh <VERSION_FOR_TAG>


Here are the steps that is taken by the ``tag_and_release.sh`` script:

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
