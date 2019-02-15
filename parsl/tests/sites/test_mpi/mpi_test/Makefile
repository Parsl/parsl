CC=mpicc

CFLAGS = -Wall -g -lm -fopenmp -DDEBUG

SRC = $(wildcard *.c)

DEPS = $(wildcard *.h)

OBJS = $(SRC:.c=.o)

EXEC=mpi_hello

%.o: %.c $(DEPS)
	$(CC) -c -o $@ $< $(CFLAGS)

$(EXEC) : $(OBJS)
	$(CC) -o $@ $^ $(CFLAGS)

clean:
	rm -f $(OBJS) $(EXEC) *~ *out *err
