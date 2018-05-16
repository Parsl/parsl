Setting up globus endpoint
==========================

Setup a Globus personal endpoint or activate a shared endpoint, following instructions here:
`Globus docs <https://docs.globus.org/faq/globus-connect-endpoints/>`_

Once an endpoint is setup, get the endpoint UUID for the endpoint from `Globus.org <https://www.globus.org/app/endpoints>`_.

You can track the file staging event here: `transfer activity <https://www.globus.org/app/activity>`_.

Running the tests
=================

1. Ensure that the endpoint is active.
2. Edit the ``user_opts.py`` script with the Globus endpoint.
3. Run the tests
   ``pytest test_globus``

