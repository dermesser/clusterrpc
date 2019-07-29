#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <sys/time.h>
#include <unistd.h>

#include "rpc.pb-c.h"

#include "server.h"

typedef struct {
    zsock_t* worker_req;
    const char* identity;
    crpc_dispatch_fn* dispatch;
    pthread_t thread;
} crpc_worker;

#define number_of_workers 4
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
bool enqueue_int(int element, int* base, size_t cap, size_t* len,
                 size_t* tail) {
    if (*len >= cap) return false;
    if (*tail == cap) *tail = 0;

    base[*tail] = element;
    *tail += 1;
    *len += 1;
    return true;
}
int dequeue_int(int* base, size_t cap, size_t* len, size_t* head) {
    if (*len == 0) return -1;
    if (*head == cap - 1) {
        *head = 0;
        *len -= 1;
        return base[cap - 1];
    }
    *head += 1;
    *len -= 1;
    return base[*head - 1];
}

bool enqueue_zmsg_t(zmsg_t* element, zmsg_t** base, size_t cap, size_t* len,
                    size_t* tail) {
    if (*len >= cap) return false;
    if (*tail == cap) *tail = 0;

    base[*tail] = element;
    *tail += 1;
    *len += 1;
    return true;
}
zmsg_t* dequeue_zmsg_t(zmsg_t** base, size_t cap, size_t* len, size_t* head) {
    if (*len == 0) return NULL;
    if (*head == cap - 1) {
        *head = 0;
        *len -= 1;
        return base[cap - 1];
    }
    *head += 1;
    *len -= 1;
    return base[*head - 1];
}

typedef struct {
    zframe_t *client_id, *request_id, *zero, *data;
    Proto__RPCRequest* request;
} crpc_request;

// Initialize a crpc_request from a request message. Takes ownership of
// request_msg.
static bool crpc_initialize_request(zmsg_t* request_msg, crpc_request* dest) {
    if (zmsg_size(request_msg) != 4) {
        fprintf(stderr, "Bad message size! Expected 4 frames, got %ld\n",
                zmsg_size(request_msg));
        return false;
    }

    dest->client_id = zmsg_pop(request_msg);
    dest->request_id = zmsg_pop(request_msg);
    dest->zero = zmsg_pop(request_msg);
    dest->data = zmsg_pop(request_msg);
    zmsg_destroy(&request_msg);
    dest->request = proto__rpcrequest__unpack(NULL, zframe_size(dest->data),
                                              zframe_data(dest->data));
    return true;
}

// Frees fields of request (but not request itself).
static void crpc_free_request(crpc_request* request) {
    if (request->request)
        proto__rpcrequest__free_unpacked(request->request, NULL);
    if (request->client_id && zframe_is(request->client_id))
        zframe_destroy(&request->client_id);
    if (request->request_id && zframe_is(request->request_id))
        zframe_destroy(&request->request_id);
    if (request->zero && zframe_is(request->zero))
        zframe_destroy(&request->zero);
    if (request->data && zframe_is(request->data))
        zframe_destroy(&request->data);
}

typedef struct {
    Proto__TraceInfo trace;
} crpc_trace_context;

static bool crpc_initialize_trace(crpc_request* request,
                                  crpc_trace_context* trace_context) {
    if (!request->request->want_trace) return false;

    proto__trace_info__init(&trace_context->trace);
    static char* machine_name = NULL;
    if (!machine_name) {
        const size_t hostname_len = 128;
        machine_name = malloc(hostname_len);
        gethostname(machine_name, hostname_len);
    }
    trace_context->trace.machine_name = machine_name;
    size_t endpoint_len = 2 + strlen(request->request->procedure) +
                          strlen(request->request->srvc);
    trace_context->trace.endpoint_name = calloc(endpoint_len, 1);
    strcpy(trace_context->trace.endpoint_name, request->request->srvc);
    strcpy(trace_context->trace.endpoint_name + strlen(request->request->srvc),
           ".");
    strcpy(
        trace_context->trace.endpoint_name + 1 + strlen(request->request->srvc),
        request->request->procedure);
    trace_context->trace.endpoint_name[endpoint_len - 1] = 0;

    struct timeval t;
    gettimeofday(&t, NULL);
    trace_context->trace.received_time = 1000000 * t.tv_sec + t.tv_usec;
    return true;
}

static bool crpc_attach_trace(Proto__RPCResponse* response,
                              crpc_trace_context* trace_context) {
    if (!trace_context->trace.received_time) return false;
    struct timeval t;
    gettimeofday(&t, NULL);
    trace_context->trace.replied_time = 1000000 * t.tv_sec + t.tv_usec;
    response->traceinfo = &trace_context->trace;
    return true;
}

static void crpc_free_trace(crpc_trace_context* trace_context) {
    free(trace_context->trace.endpoint_name);
}

// send_response sends a response.
//
// It frees all zframe_t* arguments and trace_context, but nothing else.
static void send_response(crpc_request* request,
                          crpc_trace_context* trace_context,
                          char* error_message,
                          Proto__RPCResponse__Status status,
                          uint8_t* response_data, size_t response_data_len,
                          zsock_t* front_router) {
    Proto__RPCResponse response;
    proto__rpcresponse__init(&response);
    response.error_message = error_message;
    response.response_status = status;
    response.rpc_id = request->request->rpc_id;
    response.response_data.data = response_data;
    response.response_data.len = response_data_len;
    response.has_response_data = true;

    bool traced = crpc_attach_trace(&response, trace_context);
#define small_response_len 128

    uint8_t response_serialized_small[small_response_len];
    size_t response_len = proto__rpcresponse__get_packed_size(&response);
    uint8_t* response_serialized = response_serialized_small;
    if (response_len > small_response_len)
        response_serialized = malloc(response_len);
    proto__rpcresponse__pack(&response, response_serialized);

    zmsg_t* response_msg = zmsg_new();
    zmsg_append(response_msg, &request->client_id);
    zmsg_append(response_msg, &request->request_id);
    zmsg_append(response_msg, &request->zero);
    zmsg_addmem(response_msg, response_serialized, response_len);
    zmsg_send(&response_msg, front_router);
    if (response_serialized != response_serialized_small)
        free(response_serialized);
    if (traced) crpc_free_trace(trace_context);
}

const char* ready_string = "__ready__";

static void* crpc_server_thread(void* crpc_worker_info) {
    crpc_worker* info = crpc_worker_info;

    zmsg_t* ready_msg = zmsg_new();
    zmsg_addstr(ready_msg, "BOGUS_CLIENT_ID");
    zmsg_addstr(ready_msg, "REQUEST_ID");
    zmsg_addstr(ready_msg, "");
    zmsg_addstr(ready_msg, ready_string);
    zmsg_send(&ready_msg, info->worker_req);

    zmsg_t* request_msg;
    while ((request_msg = zmsg_recv(info->worker_req))) {
        crpc_request request;
        if (!crpc_initialize_request(request_msg, &request)) {
            crpc_free_request(&request);
            continue;
        }
        crpc_trace_context trace_context;
        memset(&trace_context, 0, sizeof(crpc_trace_context));
        crpc_initialize_trace(&request, &trace_context);

        crpc_handler_fn* handler =
            info->dispatch(request.request->srvc, request.request->procedure);
        if (!handler) {
            send_response(&request, &trace_context, "no handler could be found",
                          PROTO__RPCRESPONSE__STATUS__STATUS_NOT_FOUND, NULL, 0,
                          info->worker_req);
            continue;
        }

        crpc_context context;
        context.input = request.request->data.data;
        context.input_len = request.request->data.len;

        (*handler)(&context);

        if (!context.ok) {
            send_response(&request, &trace_context, context.error_string,
                          PROTO__RPCRESPONSE__STATUS__STATUS_NOT_OK, NULL, 0,
                          info->worker_req);
            if (context.response) free(context.response);
            continue;
        }
        send_response(&request, &trace_context, "",
                      PROTO__RPCRESPONSE__STATUS__STATUS_OK, context.response,
                      context.response_len, info->worker_req);
        free(context.response);
        crpc_free_request(&request);
    }
    return NULL;
}

static void* crpc_server_main(void* server_v) {
    crpc_server* server = server_v;
    zpoller_t* poll =
        zpoller_new(server->front_router, server->back_router, NULL);

    while (true) {
        zsock_t* ready = zpoller_wait(poll, -1);
        if (ready == server->front_router) {
            zmsg_t* msg;
            while ((msg = zmsg_recv_nowait(ready))) {
                const char* id = NULL;
                if (server->free_workers_len > 0) {
                    int worker_ix = dequeue_int(
                        server->free_workers_q, number_of_workers,
                        &server->free_workers_len, &server->free_workers_h);
                    assert(worker_ix >= 0);
                    id = server->workers[worker_ix]->identity;
                } else if (server->queued_messages_len < request_queue_length) {
                    enqueue_zmsg_t(msg, server->queued_messages,
                                   request_queue_length,
                                   &server->queued_messages_len,
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
                zframe_t* zero = zmsg_pop(msg);
                size_t worker_id_int = 0;
                for (size_t i = 0; i < number_of_workers; i++) {
                    if (zframe_streq(worker_id, server->workers[i]->identity)) {
                        worker_id_int = i;
                        break;
                    }
                }
                zmsg_send(&msg, server->front_router);

                if (server->queued_messages_len > 0) {
                    zmsg_t* qmsg = dequeue_zmsg_t(server->queued_messages,
                                                  request_queue_length,
                                                  &server->queued_messages_len,
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
    snprintf(backend_router_address, 128, "inproc://backend.router.%d",
             getpid());
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
        assert(0 == pthread_create(&rpc_worker->thread, NULL,
                                   crpc_server_thread, rpc_worker));
        server->workers[i] = rpc_worker;
    }

    crpc_server_main(server);
}
