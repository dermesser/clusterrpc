#include <czmq.h>

typedef struct {
    // IN
    uint8_t* input;
    size_t input_len;

    // OUT
    _Bool ok;
    // Freed by server.
    char* error_string;
    // Freed by server.
    uint8_t* response;
    size_t response_len;
} crpc_context;

typedef void (crpc_handler_fn)(crpc_context*);
typedef crpc_handler_fn* (crpc_dispatch_fn)(const char* service, const char* method);

typedef struct {
    zsock_t* front_router;
    zsock_t* back_router;

    // Set by client.
    crpc_dispatch_fn* dispatch;

    // TODO: Worker queue. For now, handle in main thread.
} crpc_server;

static const char *backend_router_address = "inproc://clusterrpc.backend_router";

void crpc_start_server(const char* address, crpc_dispatch_fn* dispatch);
