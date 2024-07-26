#include <poll.h>
#include <stdlib.h>

// prim_pollhelper_allocate_memory: Int -> PrimIO AnyPtr

void *pollhelper_allocate_memory(int n) {
    return calloc(sizeof(struct pollfd), n);
}
