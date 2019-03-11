module load mvapich2

cd mpi_test
make clean ; make
cd ..
ln -s mpi_test/mpi_hello .
