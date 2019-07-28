#include <stdio.h>
#include <stdint.h>

#include "proto/rpc.pb-c.h"

#include "server.h"

// send_response sends a response.
//
// It frees only response_data.
static void send_response(char* error_message, Proto__RPCResponse__Status status, zframe_t* client_id,
        zframe_t* request_id, zframe_t* zero_frame, uint8_t* response_data, size_t response_data_len, zsock_t* front_router) {
    Proto__RPCResponse response;
    proto__rpcresponse__init(&response);
    response.error_message = error_message;
    response.response_status = status;
    response.response_data.data = response_data;
    response.response_data.len = response_data_len;
    zmsg_t* response_msg = zmsg_new();
    size_t response_len = proto__rpcresponse__get_packed_size(&response);
    uint8_t* response_serialized = malloc(response_len);
    proto__rpcresponse__pack(&response, response_serialized);

    zmsg_append(response_msg, &client_id);
    zmsg_append(response_msg, &request_id);
    zmsg_append(response_msg, &zero_frame);
    zmsg_addmem(response_msg, response_serialized, response_len);
    zmsg_send(&response_msg, front_router);
    free(response_serialized);
    free(response_data);
}

struct _crpc_worker_info {
    zsock_t* worker_req;
    crpc_dispatch_fn* dispatch;
};

const char* ready_string = "__ready__";

static void* _crpc_server_thread(void* crpc_worker_info) {
    struct _crpc_worker_info* info = crpc_worker_info;

    zmsg_t* ready_msg = zmsg_new();
    zmsg_addstr(ready_msg, "BOGUS_CLIENT_ID");
    zmsg_addstr(ready_msg, "REQUEST_ID");
    zmsg_addstr(ready_msg, "");
    zmsg_addstr(ready_msg, ready_string);
    zmsg_send(&ready_msg, info->worker_req);

    zmsg_t* request_msg;
    while (request_msg = zmsg_recv(info->worker_req)) {
        assert(zmsg_size(request_msg) == 4);

    }
}

static void* _crpc_server_main(void* server_v) {
    crpc_server* server = server_v;
    while (true) {
        // TODO: Make asynchronous.
        zmsg_t* message = zmsg_recv(server->front_router);
        if (zmsg_size(message) != 4) {
            fprintf(stderr, "Bad message size! Expected 4 frames, got %ld\n", zmsg_size(message));
            continue;
        }

        zframe_t* client_id = zmsg_pop(message);
        zframe_t* request_id = zmsg_pop(message);
        zframe_t* zero = zmsg_pop(message);
        zframe_t* data = zmsg_pop(message);
        Proto__RPCRequest* request = proto__rpcrequest__unpack(NULL, zframe_size(data), zframe_data(data));

        crpc_handler_fn* handler = server->dispatch(request->srvc, request->procedure);
        if (!handler) {
            send_response("no handler could be found", PROTO__RPCRESPONSE__STATUS__STATUS_NOT_FOUND, client_id,
                    request_id, zero, NULL, 0, server->front_router);
            continue;
        }

        crpc_context context;
        context.input = request->data.data;
        context.input_len = request->data.len;

        (*handler)(&context);

        if (!context.ok) {
            send_response(context.error_string, PROTO__RPCRESPONSE__STATUS__STATUS_NOT_OK, client_id,
                    request_id, zero, NULL, 0, server->front_router);
            if (context.response) free(context.response);
            continue;
        }
        send_response("", PROTO__RPCRESPONSE__STATUS__STATUS_OK, client_id,
                request_id, zero, context.response, context.response_len, server->front_router);
        fprintf(stderr, "sent response to %s.%s request\n", request->srvc, request->procedure);
        proto__rpcrequest__free_unpacked(request, NULL);
        zmsg_destroy(&message);
    }
    return NULL;
}

void crpc_start_server(const char* address, crpc_dispatch_fn* dispatch) {
    crpc_server* server = malloc(sizeof(crpc_server));
    if (!server) return;

    server->front_router = zsock_new_router(address);
    server->back_router = zsock_new_router(backend_router_address);
    server->dispatch = dispatch;

    _crpc_server_main(server);
}
