Setting up globus endpoint
==========================

Setup a globus personal endpoint or activate a shared endpoint, following instructions here:
`Globus docs <https://docs.globus.org/faq/globus-connect-endpoints/>`_

Once an endpoint is setup, get the endpoint UUID for the endpoint from `Globus.org <https://www.globus.org/app/endpoints>`_.

You can track the file staging event here : `transfer activity <https://www.globus.org/app/activity>`_.

Running the tests
=================


1. Ensure that the endpoint is active.
2. Edit the ``test_globus/setup.sh`` script and update the ``GLOBUS_ENDPOINT`` variable with the local endpoint.
3. Source the setup script : ``source test_globus/setup.sh``
4. Run the tests
   ``nosetests test_globus``

