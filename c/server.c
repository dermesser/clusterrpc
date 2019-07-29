#include <stdio.h>
#include <stdint.h>
#include <pthread.h>

#include "rpc.pb-c.h"

#include "server.h"

typedef struct {
    zsock_t* worker_req;
    const char* identity;
    crpc_dispatch_fn* dispatch;
    pthread_t thread;
} crpc_worker;

#define number_of_workers 4

struct crpc_server {
    zsock_t* front_router;
    zsock_t* back_router;

    // Set by client.
    crpc_dispatch_fn* dispatch;

    crpc_worker* workers[number_of_workers];
    int next_worker;
};

// send_response sends a response.
//
// It frees response_data and all zframe_t* arguments.
static void send_response(char* error_message, Proto__RPCResponse__Status status, zframe_t* client_id,
        zframe_t* request_id, zframe_t* zero_frame, char* rpc_id, uint8_t* response_data, size_t response_data_len, zsock_t* front_router) {
    Proto__RPCResponse response;
    proto__rpcresponse__init(&response);
    response.error_message = error_message;
    response.response_status = status;
    response.rpc_id = rpc_id;
    response.response_data.data = response_data;
    response.response_data.len = response_data_len;
    response.has_response_data = true;
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

const char* ready_string = "__ready__";

static void* _crpc_server_thread(void* crpc_worker_info) {
    crpc_worker* info = crpc_worker_info;

    zmsg_t* ready_msg = zmsg_new();
    zmsg_addstr(ready_msg, "BOGUS_CLIENT_ID");
    zmsg_addstr(ready_msg, "REQUEST_ID");
    zmsg_addstr(ready_msg, "");
    zmsg_addstr(ready_msg, ready_string);
    zmsg_send(&ready_msg, info->worker_req);

    zmsg_t* request_msg;
    while ((request_msg = zmsg_recv(info->worker_req))) {
        if (zmsg_size(request_msg) != 4) {
            fprintf(stderr, "Bad message size! Expected 4 frames, got %ld\n", zmsg_size(request_msg));
            continue;
        }

        zframe_t* client_id = zmsg_pop(request_msg);
        zframe_t* request_id = zmsg_pop(request_msg);
        zframe_t* zero = zmsg_pop(request_msg);
        zframe_t* data = zmsg_pop(request_msg);
        Proto__RPCRequest* request = proto__rpcrequest__unpack(NULL, zframe_size(data), zframe_data(data));

        crpc_handler_fn* handler = info->dispatch(request->srvc, request->procedure);
        if (!handler) {
            send_response("no handler could be found", PROTO__RPCRESPONSE__STATUS__STATUS_NOT_FOUND, client_id,
                    request_id, zero, request->rpc_id, NULL, 0, info->worker_req);
            continue;
        }

        crpc_context context;
        context.input = request->data.data;
        context.input_len = request->data.len;

        (*handler)(&context);

        if (!context.ok) {
            send_response(context.error_string, PROTO__RPCRESPONSE__STATUS__STATUS_NOT_OK, client_id,
                    request_id, zero, request->rpc_id, NULL, 0, info->worker_req);
            if (context.response) free(context.response);
            continue;
        }
        send_response("", PROTO__RPCRESPONSE__STATUS__STATUS_OK, client_id,
                request_id, zero, request->rpc_id, context.response, context.response_len, info->worker_req);
        proto__rpcrequest__free_unpacked(request, NULL);
        zmsg_destroy(&request_msg);
    }
    return NULL;
}

static void* _crpc_server_main(void* server_v) {
    crpc_server* server = server_v;
    zpoller_t* poll = zpoller_new(server->front_router, server->back_router, NULL);

    while (true) {
        zsock_t* ready = zpoller_wait(poll, -1);
        if (ready == server->front_router) {
            zmsg_t* msg;
            while ((msg = zmsg_recv_nowait(ready))) {
                zmsg_t* be_msg = zmsg_new();
                const char* id = server->workers[(server->next_worker+1)%number_of_workers]->identity;
                zmsg_addstr(be_msg, id);
                zmsg_addstr(be_msg, "");
                zmsg_add(be_msg, zmsg_pop(msg));
                zmsg_add(be_msg, zmsg_pop(msg));
                zmsg_add(be_msg, zmsg_pop(msg));
                zmsg_add(be_msg, zmsg_pop(msg));
                zmsg_send(&be_msg, server->back_router);
            }
        } else if (ready == server->back_router) {
            zmsg_t* msg;
            while ((msg = zmsg_recv_nowait(ready))) {
                zframe_t* worker_id = zmsg_pop(msg);
                zframe_t* zero = zmsg_pop(msg);
                zmsg_send(&msg, server->front_router);
            }
        } else {
            break;
        }
    }

    zpoller_destroy(&poll);
    return NULL;
}

void crpc_start_server(const char* address, crpc_dispatch_fn* dispatch) {
    crpc_server* server = malloc(sizeof(crpc_server));
    if (!server) return;

    char* backend_router_address = malloc(128);
    snprintf(backend_router_address, 128, "inproc://backend.router.%d", getpid());
    server->next_worker = 0;
    server->front_router = zsock_new_router(address);
    server->back_router = zsock_new_router(backend_router_address);
    server->dispatch = dispatch;

    zsock_set_router_mandatory(server->front_router, 1);
    zsock_set_router_mandatory(server->back_router, 1);

    // 128 workers for now.
    for (int i = 0; i < number_of_workers; i++) {
        char* identity = malloc(5);
        snprintf(identity, 5, "%04d", i);
        fprintf(stderr, "started worker %s\n", identity);
        zsock_t* worker = zsock_new(ZMQ_REQ);
        zsock_set_identity(worker, identity);
        zsock_connect(worker, backend_router_address);

        crpc_worker* rpc_worker = malloc(sizeof(crpc_worker));
        rpc_worker->identity = identity;
        rpc_worker->worker_req = worker;
        rpc_worker->dispatch = dispatch;
        assert(0 == pthread_create(&rpc_worker->thread, NULL, _crpc_server_thread, rpc_worker));
        server->workers[i] = rpc_worker;
    }

    _crpc_server_main(server);
}
