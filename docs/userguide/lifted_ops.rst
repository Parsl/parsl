.. _label-liftedops:

Lifted [] operator
==================

When an app returns a complex structure such as a ``dict`` or a ``list``,
it is sometimes useful to pass an element of that structure to a subsequent
task, without waiting for that subsequent task to complete.

To help with this, Parsl allows the ``[]`` operator to be used on and
`AppFuture`. This operator will return return another `AppFuture` that will
complete after the initial future, with the result of `[]` on the value
of the initial future.

The end result is that this assertion will hold:

.. code-block:: python

    fut = my_app()
    assert fut['x'].result() == fut.result()[x]

but more concurrency will be available, as execution of the main workflow
code will not stop to wait for ``result()`` to complete on the initial
future.
