Packaging
---------

Currently packaging is managed by Yadu.

Here are the steps:

.. code:: bash

      # Depending on permission all of the following might have to be run as root.
      sudo su

      # Make sure to have twine installed
      pip3 install twine

      # Create a source distribution
      python3 setup.py sdist

      # Create a wheel package, which is a prebuilt package
      python3 setup.py bdist_wheel

      # Upload the package with twine
      # This step will ask for username and password for the PyPi account.
      twine upload dist/*
