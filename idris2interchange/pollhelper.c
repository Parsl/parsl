#include <poll.h>
#include <stdlib.h>

// prim_pollhelper_allocate_memory: Int -> PrimIO AnyPtr

void *pollhelper_allocate_memory(int n) {
    return calloc(sizeof(struct pollfd), n);
}

// prim_pollhelper_set_entry : AnyPtr -> Int -> Int -> PrimIO ()

void pollhelper_set_entry(struct pollfd *buf, int pos, int fd) {
    buf[pos].fd = fd;
    buf[pos].events = POLLIN;
    // right now I never use POLLOUT in the rust impl I think, and generally
    // don't try to deal with being unable to write / what to do in that
    // situation. It's probably worth thinking about in the general
    // queueing behaviour situation, though.
}

