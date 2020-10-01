/* Generated by the protocol buffer compiler.  DO NOT EDIT! */
/* Generated from: rpc.proto */

#ifndef PROTOBUF_C_rpc_2eproto__INCLUDED
#define PROTOBUF_C_rpc_2eproto__INCLUDED

#include <protobuf-c/protobuf-c.h>

PROTOBUF_C__BEGIN_DECLS

#if PROTOBUF_C_VERSION_NUMBER < 1000000
# error This file was generated by a newer version of protoc-c which is incompatible with your libprotobuf-c headers. Please update your headers.
#elif 1003002 < PROTOBUF_C_MIN_COMPILER_VERSION
# error This file was generated by an older version of protoc-c which is incompatible with your libprotobuf-c headers. Please regenerate this file with a newer version of protoc-c.
#endif


typedef struct _Proto__TraceInfo Proto__TraceInfo;
typedef struct _Proto__RPCRequest Proto__RPCRequest;
typedef struct _Proto__RPCResponse Proto__RPCResponse;


/* --- enums --- */

typedef enum _Proto__RPCResponse__Status {
  /*
   * Default value
   */
  PROTO__RPCRESPONSE__STATUS__STATUS_UNKNOWN = 0,
  /*
   * a.k.a. 200
   */
  PROTO__RPCRESPONSE__STATUS__STATUS_OK = 1,
  /*
   * service/endpoint not found (a.k.a. 404)
   */
  PROTO__RPCRESPONSE__STATUS__STATUS_NOT_FOUND = 2,
  /*
   * The handler returned an error; see the error_message for a description (500). response_data may have content
   */
  PROTO__RPCRESPONSE__STATUS__STATUS_NOT_OK = 4,
  /*
   * An error happened in the clusterrpc implementation (500)
   */
  PROTO__RPCRESPONSE__STATUS__STATUS_SERVER_ERROR = 5,
  /*
   * The requested timeout has been expired
   */
  PROTO__RPCRESPONSE__STATUS__STATUS_TIMEOUT = 6,
  /*
   * The server is overloaded (503)
   */
  PROTO__RPCRESPONSE__STATUS__STATUS_OVERLOADED_RETRY = 7,
  /*
   * We couldn't even send the request (PB serialization error, ...)
   */
  PROTO__RPCRESPONSE__STATUS__STATUS_CLIENT_REQUEST_ERROR = 9,
  /*
   * We couldn't send the request because of network/socket issues.
   */
  PROTO__RPCRESPONSE__STATUS__STATUS_CLIENT_NETWORK_ERROR = 10,
  /*
   * Client function called in a wrong way (e.g. different lengt of raddrs
   * and rports slices to NewClientRR())
   */
  PROTO__RPCRESPONSE__STATUS__STATUS_CLIENT_CALLED_WRONG = 11,
  /*
   * Timeout somewhere in the call stack
   */
  PROTO__RPCRESPONSE__STATUS__STATUS_MISSED_DEADLINE = 12,
  /*
   * Loadshedding mode, not accepting requests right now
   */
  PROTO__RPCRESPONSE__STATUS__STATUS_LOADSHED = 13,
  /*
   * Health check failed
   */
  PROTO__RPCRESPONSE__STATUS__STATUS_UNHEALTHY = 14
    PROTOBUF_C__FORCE_ENUM_TO_BE_INT_SIZE(PROTO__RPCRESPONSE__STATUS)
} Proto__RPCResponse__Status;

/* --- messages --- */

struct  _Proto__TraceInfo
{
  ProtobufCMessage base;
  int64_t received_time;
  int64_t replied_time;
  char *machine_name;
  char *endpoint_name;
  char *error_message;
  char *redirect;
  size_t n_child_calls;
  Proto__TraceInfo **child_calls;
};
#define PROTO__TRACE_INFO__INIT \
 { PROTOBUF_C_MESSAGE_INIT (&proto__trace_info__descriptor) \
    , 0, 0, NULL, NULL, NULL, NULL, 0,NULL }


struct  _Proto__RPCRequest
{
  ProtobufCMessage base;
  /*
   * A unique-ish ID for this RPC
   */
  char *rpc_id;
  char *srvc;
  char *procedure;
  ProtobufCBinaryData data;
  /*
   * UNIX µs timestamp after which we don't want to have an answer anymore
   */
  protobuf_c_boolean has_deadline;
  int64_t deadline;
  /*
   * (i.e. the server doesn't need to bother sending one)
   */
  char *caller_id;
  protobuf_c_boolean has_want_trace;
  protobuf_c_boolean want_trace;
};
#define PROTO__RPCREQUEST__INIT \
 { PROTOBUF_C_MESSAGE_INIT (&proto__rpcrequest__descriptor) \
    , NULL, NULL, NULL, {0,NULL}, 0, 0, NULL, 0, 0 }


struct  _Proto__RPCResponse
{
  ProtobufCMessage base;
  char *rpc_id;
  protobuf_c_boolean has_response_data;
  ProtobufCBinaryData response_data;
  Proto__RPCResponse__Status response_status;
  char *error_message;
  Proto__TraceInfo *traceinfo;
};
#define PROTO__RPCRESPONSE__INIT \
 { PROTOBUF_C_MESSAGE_INIT (&proto__rpcresponse__descriptor) \
    , NULL, 0, {0,NULL}, PROTO__RPCRESPONSE__STATUS__STATUS_UNKNOWN, NULL, NULL }


/* Proto__TraceInfo methods */
void   proto__trace_info__init
                     (Proto__TraceInfo         *message);
size_t proto__trace_info__get_packed_size
                     (const Proto__TraceInfo   *message);
size_t proto__trace_info__pack
                     (const Proto__TraceInfo   *message,
                      uint8_t             *out);
size_t proto__trace_info__pack_to_buffer
                     (const Proto__TraceInfo   *message,
                      ProtobufCBuffer     *buffer);
Proto__TraceInfo *
       proto__trace_info__unpack
                     (ProtobufCAllocator  *allocator,
                      size_t               len,
                      const uint8_t       *data);
void   proto__trace_info__free_unpacked
                     (Proto__TraceInfo *message,
                      ProtobufCAllocator *allocator);
/* Proto__RPCRequest methods */
void   proto__rpcrequest__init
                     (Proto__RPCRequest         *message);
size_t proto__rpcrequest__get_packed_size
                     (const Proto__RPCRequest   *message);
size_t proto__rpcrequest__pack
                     (const Proto__RPCRequest   *message,
                      uint8_t             *out);
size_t proto__rpcrequest__pack_to_buffer
                     (const Proto__RPCRequest   *message,
                      ProtobufCBuffer     *buffer);
Proto__RPCRequest *
       proto__rpcrequest__unpack
                     (ProtobufCAllocator  *allocator,
                      size_t               len,
                      const uint8_t       *data);
void   proto__rpcrequest__free_unpacked
                     (Proto__RPCRequest *message,
                      ProtobufCAllocator *allocator);
/* Proto__RPCResponse methods */
void   proto__rpcresponse__init
                     (Proto__RPCResponse         *message);
size_t proto__rpcresponse__get_packed_size
                     (const Proto__RPCResponse   *message);
size_t proto__rpcresponse__pack
                     (const Proto__RPCResponse   *message,
                      uint8_t             *out);
size_t proto__rpcresponse__pack_to_buffer
                     (const Proto__RPCResponse   *message,
                      ProtobufCBuffer     *buffer);
Proto__RPCResponse *
       proto__rpcresponse__unpack
                     (ProtobufCAllocator  *allocator,
                      size_t               len,
                      const uint8_t       *data);
void   proto__rpcresponse__free_unpacked
                     (Proto__RPCResponse *message,
                      ProtobufCAllocator *allocator);
/* --- per-message closures --- */

typedef void (*Proto__TraceInfo_Closure)
                 (const Proto__TraceInfo *message,
                  void *closure_data);
typedef void (*Proto__RPCRequest_Closure)
                 (const Proto__RPCRequest *message,
                  void *closure_data);
typedef void (*Proto__RPCResponse_Closure)
                 (const Proto__RPCResponse *message,
                  void *closure_data);

/* --- services --- */


/* --- descriptors --- */

extern const ProtobufCMessageDescriptor proto__trace_info__descriptor;
extern const ProtobufCMessageDescriptor proto__rpcrequest__descriptor;
extern const ProtobufCMessageDescriptor proto__rpcresponse__descriptor;
extern const ProtobufCEnumDescriptor    proto__rpcresponse__status__descriptor;

PROTOBUF_C__END_DECLS


#endif  /* PROTOBUF_C_rpc_2eproto__INCLUDED */