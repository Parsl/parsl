.. _label-liftedops:

Lifted operators
================

Parsl allows some operators (``[]`` and ``.``) to be used on an AppFuture in
a way that makes sense with those operators on the eventually returned
result.

Lifted [] operator
------------------

When an app returns a complex structure such as a ``dict`` or a ``list``,
it is sometimes useful to pass an element of that structure to a subsequent
task, without waiting for that subsequent task to complete.

To help with this, Parsl allows the ``[]`` operator to be used on an
`AppFuture`. This operator will return another `AppFuture` that will
complete after the initial future, with the result of ``[]`` on the value
of the initial future.

The end result is that this assertion will hold:

.. code-block:: python

    fut = my_app()
    assert fut['x'].result() == fut.result()[x]

but more concurrency will be available, as execution of the main workflow
code will not stop to wait for ``result()`` to complete on the initial
future.

`AppFuture` does not implement other methods commonly associated with
dicts and lists, such as ``len``, because those methods should return a
specific type of result immediately, and that is not possible when the
results are not available until the future.

If a key does not exist in the returned result, then the exception will
appear in the Future returned by ``[]``, rather than at the point that
the ``[]`` operator is applied. This is because the valid values that can
be used are not known until the underlying result is available.

Lifted . operator
-----------------

The ``.`` operator works similarly to ``[]`` described above:

.. code-block:: python

    fut = my_app
    assert fut.x.result() == fut.result().x

Attributes beginning with ``_`` are not lifted as this usually indicates an
attribute that is used for internal purposes, and to try to avoid mixing
protocols (such as iteration in for loops) defined on AppFutures vs protocols
defined on the underlying result object.
