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

#define number_of_workers 12
#define request_queue_length 512

struct crpc_server {
    zsock_t* front_router;
    zsock_t* back_router;

    // Set by client.
    crpc_dispatch_fn* dispatch;

    crpc_worker* workers[number_of_workers];

    int free_workers_q[number_of_workers];
    size_t free_workers_h, free_workers_t;
    size_t free_workers_len;
    zmsg_t* queued_messages[request_queue_length];
    size_t queued_messages_h, queued_messages_t;
    size_t queued_messages_len;
};

// Enqueue an int to tail of an int queue.
bool enqueue_int(int element, int* base, size_t cap, size_t* len, size_t* tail) {
    if (*len >= cap) return false;
    if (*tail == cap)
        *tail = 0;

    base[*tail] = element;
    *tail += 1;
    *len += 1;
    return true;
}
int dequeue_int(int* base, size_t cap, size_t* len, size_t* head) {
    if (*len == 0) return -1;
    if (*head == cap-1) {
        *head = 0;
        *len -= 1;
        return base[cap-1];
    }
    *head += 1;
    *len -= 1;
    return base[*head-1];
}

bool enqueue_zmsg_t(zmsg_t* element, zmsg_t** base, size_t cap, size_t* len, size_t* tail) {
    if (*len >= cap) return false;
    if (*tail == cap)
        *tail = 0;

    base[*tail] = element;
    *tail += 1;
    *len += 1;
    return true;
}
zmsg_t* dequeue_zmsg_t(zmsg_t** base, size_t cap, size_t* len, size_t* head) {
    if (*len == 0) return NULL;
    if (*head == cap-1) {
        *head = 0;
        *len -= 1;
        return base[cap-1];
    }
    *head += 1;
    *len -= 1;
    return base[*head-1];
}


// send_response sends a response.
//
// It frees all zframe_t* arguments, but nothing else.
static void send_response(char* error_message, Proto__RPCResponse__Status
        status, zframe_t* client_id, zframe_t* request_id, zframe_t*
        zero_frame, char* rpc_id, uint8_t* response_data, size_t
        response_data_len, zsock_t* front_router) {
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
        free(context.response);
        zmsg_destroy(&request_msg);
        zframe_destroy(&data);
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
                const char* id = NULL;
                if (server->free_workers_len > 0) {
                    int worker_ix = dequeue_int(server->free_workers_q,
                            number_of_workers, &server->free_workers_len,
                            &server->free_workers_h);
                    assert(worker_ix >= 0);
                    id = server->workers[worker_ix]->identity;
                } else if (server->queued_messages_len < request_queue_length) {
                    enqueue_zmsg_t(msg, server->queued_messages,
                            request_queue_length, &server->queued_messages_len,
                            &server->queued_messages_t);
                    continue;
                }
                zmsg_t* be_msg = zmsg_new();
                zmsg_addstr(be_msg, id);
                zmsg_addstr(be_msg, "");
                zmsg_add(be_msg, zmsg_pop(msg));
                zmsg_add(be_msg, zmsg_pop(msg));
                zmsg_add(be_msg, zmsg_pop(msg));
                zmsg_add(be_msg, zmsg_pop(msg));
                zmsg_send(&be_msg, server->back_router);
                zmsg_destroy(&msg);
            }
        } else if (ready == server->back_router) {
            zmsg_t* msg;
            while ((msg = zmsg_recv_nowait(ready))) {
                zframe_t* worker_id = zmsg_pop(msg);
                size_t worker_id_int = 0;
                for (size_t i = 0; i < number_of_workers; i++) {
                    if (zframe_streq(worker_id, server->workers[i]->identity)) {
                        worker_id_int = i;
                        break;
                    }
                }
                zframe_t* zero = zmsg_pop(msg);
                zmsg_send(&msg, server->front_router);

                if (server->queued_messages_len > 0) {
                    zmsg_t* qmsg = dequeue_zmsg_t(server->queued_messages,
                            request_queue_length, &server->queued_messages_len,
                            &server->queued_messages_h);
                    if (qmsg && zmsg_is(qmsg)) {
                        zmsg_t* be_msg = zmsg_new();
                        zmsg_add(be_msg, worker_id);
                        zmsg_add(be_msg, zero);
                        zmsg_add(be_msg, zmsg_pop(qmsg));
                        zmsg_add(be_msg, zmsg_pop(qmsg));
                        zmsg_add(be_msg, zmsg_pop(qmsg));
                        zmsg_add(be_msg, zmsg_pop(qmsg));
                        zmsg_send(&be_msg, server->back_router);
                        zmsg_destroy(&qmsg);
                        break;
                    }
                }
                zframe_destroy(&worker_id);
                zframe_destroy(&zero);
                enqueue_int(worker_id_int, server->free_workers_q,
                        number_of_workers, &server->free_workers_len,
                        &server->free_workers_t);
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
    memset(server, 0, sizeof(crpc_server));

    char* backend_router_address = malloc(128);
    snprintf(backend_router_address, 128, "inproc://backend.router.%d", getpid());
    server->front_router = zsock_new_router(address);
    server->back_router = zsock_new_router(backend_router_address);
    server->dispatch = dispatch;

    zsock_set_router_mandatory(server->front_router, 1);
    zsock_set_router_mandatory(server->back_router, 1);

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
        //enqueue_int(i, server->free_workers_q, number_of_workers, &server->free_workers_len, &server->free_workers_t);
    }

    _crpc_server_main(server);
}
