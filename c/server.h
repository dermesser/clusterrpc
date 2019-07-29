#include <czmq.h>

typedef struct {
    // IN
    uint8_t* input;
    size_t input_len;

    // OUT
    _Bool ok;
    // Freed by server. Must be dynamically allocated.
    char* error_string;
    // Freed by server. Must be dynamically allocated.
    uint8_t* response;
    size_t response_len;
} crpc_context;

typedef void(crpc_handler_fn)(crpc_context*);
typedef crpc_handler_fn*(crpc_dispatch_fn)(const char* service,
                                           const char* method);

struct crpc_server;
typedef struct crpc_server crpc_server;

void crpc_start_server(const char* address, crpc_dispatch_fn* dispatch);
