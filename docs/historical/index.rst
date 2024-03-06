Historical: Changelog
=====================


.. note::
   After Parsl 1.2.0, releases moved to a lighter weight automated model.
   This manual changelog is no longer updated and is now marked as
   historical.

   Change information is delivered as commit messages on individual pull
   requests, which can be see using any relevant git history browser -
   for example, on the web at https://github.com/Parsl/parsl/commits/master/
   or on the commandline using using git log.


Historical: Libsubmit Changelog
===============================

.. note::
   As of Parsl 0.7.0 the libsubmit repository has been merged into Parsl
   and nothing more will appear in this changelog.


Historical: Swift vs Parsl
--------------------------

.. note::
   This section describes comparisons between Parsl and an earlier workflow
   system, Swift, as part of a justification for the early prototyping stages
   of Parsl development. It is no longer relevant for modern Parsl users, but
   remains of historical interest.

The following text is not well structured, and is mostly a brain dump that needs to be organized.
Moving from Swift to an established language (python) came with its own tradeoffs. We get the backing
of a rich and very well known language to handle the language aspects as well as the libraries.
However, we lose the parallel evaluation of every statement in a script. The thesis is that what we
lose is minimal and will not affect 95% of our workflows. This is not yet substantiated.

Please note that there are two Swift languages: `Swift/K <http://swift-lang.org/main/>`_
and `Swift/T <http://swift-lang.org/Swift-T/index.php>`_ . These have diverged in syntax and behavior.
Swift/K is designed for grids and clusters runs the java based
`Karajan <https://wiki.cogkit.org/wiki/Karajan>`_ (hence, /K) execution framework.
Swift/T is a completely new implementation of Swift/K for high-performance computing. Swift/T uses
Turbine(hence, /T) and and
`ADLB <http://www.mcs.anl.gov/project/adlb-asynchronous-dynamic-load-balancer>`_ runtime libraries for
highly scalable dataflow processing over MPI,
without single-node bottlenecks.

Historical: Performance and Scalability
=======================================

.. note::
   This scalability review summarises results in a paper, Parsl: Pervasive
   Parallel Programming in Python, which was published in 2019. The results
   have not been updated since then. For that reason, this section is marked
   as historical.

Parsl is designed to scale from small to large systems .

