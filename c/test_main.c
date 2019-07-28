#include "server.h"

#include <stdio.h>
#include <memory.h>

static void generic_handler(crpc_context* ctx) {
    char* input = malloc(ctx->input_len+1);
    strncpy(input, ctx->input, ctx->input_len);
    fprintf(stderr, "received request: %s\n", input);
    ctx->ok = true;
    ctx->response = input;
    ctx->response_len = ctx->input_len+1;
    return;
}

static crpc_handler_fn* dispatch(const char* service, const char* method) {
    fprintf(stderr, "received request for %s.%s\n", service, method);

    return generic_handler;
}

int main(void) {
    crpc_start_server("tcp://127.0.0.1:9500", dispatch);

    return 0;
}
