#include "server.h"

#include <stdio.h>
#include <memory.h>

static void generic_handler(crpc_context* ctx) {
    char* input = malloc(ctx->input_len+1);
    strncpy(input, (char*)ctx->input, ctx->input_len);
    ctx->ok = true;
    ctx->response = (uint8_t*)input;
    ctx->response_len = ctx->input_len+1;
    return;
}

static crpc_handler_fn* dispatch(const char* service, const char* method) {
    return generic_handler;
}

int main(void) {
    crpc_start_server("tcp://127.0.0.1:9500", dispatch);

    return 0;
}
