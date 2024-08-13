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
    printf("created socket, pointer is %p\n", sock);
    return sock;
}

void glue_zmq_connect(void* sock, char *dest) {
    int r = zmq_connect(sock, dest);
    assert(r == 0);
}

void *glue_zmq_recv_msg_alloc(void *sock) {
    zmq_msg_t *msg = malloc(sizeof(zmq_msg_t));
    zmq_msg_init(msg);
    int e = zmq_msg_recv(msg, sock, ZMQ_DONTWAIT);
    if(e == -1) {
        if (errno == EAGAIN) {
            printf("EAGAIN from zmq_msg_recv\n");
            return NULL;
        } else {
            char *err_m = strerror(errno);
            printf("zmq_msg_recv failed: errno = %d, %s\n", errno, err_m);
        }
    }
    assert(e != -1);
    return msg;
    // caller is responsible for ownership of msg memory block
}

int glue_zmq_msg_size(void *msg) {
    return zmq_msg_size(msg);
}

void* glue_zmq_msg_data(void *msg) {
    return zmq_msg_data(msg);
}

int glue_zmq_get_socket_fd(void *sock) {
    int fd, r;
    size_t optionlen;
    printf("in glue_zmq_get_socket_fd: pre getsockopt\n");
    printf("sock is %p\n", sock);
    printf("&fd is %p\n", &fd);
    printf("&optionlen is %p\n", &optionlen);
    optionlen = sizeof(fd);
    r = zmq_getsockopt(sock, ZMQ_FD, &fd, &optionlen);
    printf("in glue_zmq_get_socket_fd: post getsockopt\n");
    // TODO: do something with unused r?
    assert (optionlen == sizeof(int));
    return fd;
}


void glue_zmq_alloc_send_bytes(void *sock, void *bytes, int len) {
    int n;
    n = zmq_send(sock, bytes, len, 0);
    assert(n == len);
    // TODO: could be -1 on failure and we should do *something* with that
}
