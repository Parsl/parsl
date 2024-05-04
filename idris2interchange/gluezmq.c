#include <zmq.h>
#include <assert.h>
#include <stdlib.h>
#include <string.h>

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
    int r = zmq_connect(sock, dest);
    assert(r == 0);
}

void *glue_zmq_recv_msg_alloc(void *sock) {
    zmq_msg_t *msg = malloc(sizeof(zmq_msg_t));
    zmq_msg_init(msg);
    int e = zmq_msg_recv(msg, sock, 0);
    if(e == -1) {
        char *err_m = strerror(errno);
        printf("zmq_msg_recv failed: errno = %d, %s\n", errno, err_m);
    }
    assert(e != -1);
    return msg;
    // caller is responsible for ownership of msg memory block
}

int glue_zmq_msg_size(void *msg) {
    return zmq_msg_size(msg);
}
