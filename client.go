package clusterrpc

import (
	"clusterrpc/proto"
	"errors"
	"io"
	"log"
	"net"
	"os"
	"time"

	pb "code.google.com/p/goprotobuf/proto"
)

type Client struct {
	channel         *net.TCPConn
	sequence_number uint64
	logger          *log.Logger
	timeout         time.Duration
	// Used for default calls
	default_service, default_endpoint string
}

/*
Create a new client that connects to the clusterrpc server at raddr:rport.
The client_name is used for logging purposes.
*/
func NewClient(client_name, raddr string, rport int) (cl *Client, e error) {
	addr := net.TCPAddr{}
	addr.IP = net.ParseIP(raddr)
	addr.Port = rport

	conn, err := net.DialTCP("tcp", nil, &addr)

	if err != nil {
		return nil, err
	}

	cl = new(Client)
	cl.channel = conn
	cl.sequence_number = 0
	cl.logger = log.New(os.Stderr, "clusterrpc.Client "+client_name+": ", log.Lmicroseconds)

	return
}

/*
Change the writer to which the client logs operations.
*/
func (cl *Client) SetLoggingOutput(w io.Writer) {
	cl.logger = log.New(w, cl.logger.Prefix(), cl.logger.Flags())
}

/*
Set the logger of the client to a custom one.
*/
func (cl *Client) SetLogger(l *log.Logger) {
	cl.logger = l
}

/*
Sets the default service and endpoint; those are used for calls to RequestDefault()
*/
func (cl *Client) SetDefault(service, endpoint string) {
	cl.default_service = service
	cl.default_endpoint = endpoint
}

/*
Disable the client. Following calls will result in nil dereference.
TODO: Maybe do not do the above stated.
*/
func (cl *Client) Close() {
	cl.logger.Println("Closing client channel")
	cl.channel.Close()
	cl.channel = nil
}

/*
Call a remote procedure service.endpoint with data as input.

Returns either a byte array and nil as error or a status string as the error string.
Returned strings are

        "STATUS_OK" // Not returned in reality
        "STATUS_NOT_FOUND" // Invalid service or endpoint
        "STATUS_NOT_OK" // Handler returned an error.
        "STATUS_SERVER_ERROR" // The clusterrpc server experienced an internal server error
*/
func (cl *Client) Request(data []byte, service, endpoint string) ([]byte, error) {
	rqproto := proto.RPCRequest{}

	rqproto.SequenceNumber = pb.Uint64(cl.sequence_number)
	cl.sequence_number++

	rqproto.Srvc = pb.String(service)
	rqproto.Procedure = pb.String(endpoint)
	rqproto.Data = pb.String(string(data))

	rq_serialized, pberr := protoToLengthPrefixed(&rqproto)

	if pberr != nil {
		return nil, pberr
	}

	if cl.timeout > 0 {
		cl.channel.SetDeadline(time.Now().Add(cl.timeout))
	}
	n, werr := cl.channel.Write(rq_serialized)

	if werr != nil {
		cl.logger.Println(werr.Error())
		return nil, werr
	} else if n < len(rq_serialized) {
		cl.logger.Println(service+"."+endpoint, "Sent less bytes than needed")
		cl.channel.Close()
		return nil, errors.New("Couldn't send complete messsage")
	}

	if cl.timeout > 0 {
		cl.channel.SetDeadline(time.Now().Add(cl.timeout))
	}
	respprotobytes, rerr := readSizePrefixedMessage(cl.channel)

	if rerr != nil {
		cl.logger.Println(service+"."+endpoint, "Couldn't read message")
		return nil, rerr
	}

	respproto := proto.RPCResponse{}

	err := pb.Unmarshal(respprotobytes, &respproto)

	if err != nil {
		cl.logger.Println(err.Error())
		return nil, err
	}

	if respproto.GetResponseStatus() != proto.RPCResponse_STATUS_OK {
		err = RequestError{status: respproto.GetResponseStatus(), message: respproto.GetErrorMessage()}
		return nil, err
	}

	return []byte(respproto.GetResponseData()), nil
}

func (cl *Client) RequestDefault(data []byte) (response []byte, e error) {
	return cl.Request(data, cl.default_service, cl.default_endpoint)
}
