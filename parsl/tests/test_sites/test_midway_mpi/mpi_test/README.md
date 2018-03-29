Running the MPI Test
====================

This simple MPI test is designed to give you some very basic information:
    1. What are the ranks running on ?
    2. Args passed by a launch mechanism printed as argv[1], and argv[2]


Compile
=======

1. Load the appropriate MPI modules for your system
2. Compile the code with make:

    make clean; make

Running the app
===============

Make sure the right MPI modules is loaded. Run the app as a simple executable:

    ./mpi_hello

Or launch it with N ranks with an mpi launcher like mpirun:

    mpirun -n 8 mpi_hello