#include <zmq.h>
#include <assert.h>

// TODO: do we even need these glue functions? If they're fairly trivial,
// then perhaps FFI is enough to call direct to the ZMQ calls?
void *glue_zmq_ctx_new() {
    void *ctx = zmq_ctx_new();
    assert(ctx != NULL);
    return ctx;
}

void *glue_zmq_socket(void* ctx, int type) {
    void *sock = zmq_socket(ctx, type);
    assert(sock != NULL);
    return sock;
}

void glue_zmq_connect(void* sock, char *dest) {
    zmq_connect(sock, dest);
}
