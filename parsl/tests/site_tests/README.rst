Site Testing
============

This doc will cover two items:

1. Running the site tests on a site
2. How to prep a new site for site testing


The site tests are setup with the assumption that there is a conda installation
available either through modules available on the system, or from a user's personal
installation.

* `conda_setup.sh` will create a unique conda env mapped to `parsl/parsl_<GIT_HASH>` and
  leave a script `~/setup_parsl_test_env.sh` that will activate this env.

* `site_config_selector.py` is a facade that checks the submit node's hostname and picks a matching
  configuration from `parsl/parsl/tests/configs`.

* The tests in this folder are designed to use the site_config_selector and runs test against
  that config.


Running tests
-------------

Assuming the site tests are supported on your site, here's how you run tests::

  1. Checkout parsl into a *shared filesystem* with the right git tag/hash for testing
     * If you are running tests on a system where the $HOME is not mounted on compute nodes
       checkout to $WORK / $SCRATCH or the equivalent. For eg, Comet and Stampede2
  2. `cd parsl/tests/site_tests`
  3. `bash conda_setup.sh`
  4. Update parsl/parsl/tests/configs/local_user_opts.py with user specific options.
  5. Now check if the setup script is available at `~/setup_parsl_test_env.sh`
  6. Go back to the parsl root dir
  7. Reinstall parsl, if you've made config changes in step 3 or 4.
        >>> pip install .
  8. Run tests with `make site_test`


Adding a new site
-----------------

* We want to pick a python environment that a user is most likely to use on the site.
   1. Specialized python builds for the system (for eg, Summit)
   2. Anaconda available via modules
   3. User's conda installation
* Add a new block to `conda_setup.sh` that installs a fresh environment and writes out
  the activation commands to `~/setup_parsl_test_env.sh`
* Add a site config to `parsl/tests/configs/<SITE.py>` and add your local user options
  to `parsl/tests/configs/local_user_opts.py`. For eg, `here's mine<https://gist.github.com/yadudoc/b71765284d2db0706c4f43605dd8b8d6>`_
  Make sure that the site config uses the `fresh_config` pattern.
  Please ensure that the site config uses:
    * max_workers = 1
    * init_blocks = 1
    * min_blocks = 0

* Add this site config to `parsl/tests/site_tests/site_config_selector.py`
* Reinstall parsl, using `pip install .`
* Test a single test: `python3 test_site.py -d` to confirm that the site works correctly.
* Once tests are passing run the whole site_test with `make site_test`


Shared filesystem option
------------------------

There is a new env variable "SHARED_FS_OPTIONS" to pass markers to pytest to skip certain tests.

Tests that rely on stdout/stderr side-effects between apps that work on with a shared-FS can be deselected with `-k "not issue363"`

When there's a shared-FS, the default NoOpStaging works. However, when there's no shared-FS some tests
that uses File objects require a staging provider (eg. rsync). These tests can be turned off with
`-k "not staging_required"`

These can also be combined as `-k "not issue363 and not staging_required"`
