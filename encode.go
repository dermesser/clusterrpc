package clusterrpc

import (
	"bytes"
	proto "clusterrpc/proto"
	"errors"
	"io"

	pb "code.google.com/p/goprotobuf/proto"
)

/*
Returns a protobuf struct with the pointers initialized (pointing to allocated
memory)
*/
func NewRequest() proto.RPCRequest {
	return proto.RPCRequest{SequenceNumber: new(uint64), Srvc: new(string), Procedure: new(string), Data: new(string)}

}

func NewResponse() proto.RPCResponse {
	return proto.RPCResponse{SequenceNumber: new(uint64), ResponseData: new(string), ResponseStatus: new(proto.RPCResponse_Status)}
}

// Converts a number to a little-endian byte array
func lengthToBytes(l uint64) [8]byte {
	var sizebuf [8]byte

	var i int = 7
	for ; i >= 0; i-- {
		// The commented implementation is 5 times slower but equivalent
		/*
			var divisor uint64 = 1 << (uint(i) * 8)
			sizebuf[i] = uint8(l / divisor)
			l = l - ((l / divisor) * divisor)
		*/
		var shift uint = uint(i * 8)
		sizebuf[i] = uint8((l & (255 << shift)) >> shift)
	}

	return sizebuf
}

// Gets the value from an encoded size number (lengthToBytes())
func sizebufToLength(b [8]byte) uint64 {
	var size uint64 = 0

	var i int = 7

	for ; i >= 0; i-- {
		var multiplier uint64 = 1 << (8 * uint(i))
		size += multiplier * uint64(b[i])

		// Bitshift is not faster
		/*
			var shift uint = 8 * uint(i)
			size |= uint64(b[i]) << shift
		*/
	}
	return size
}

func RequestToBytes(r proto.RPCRequest) []byte {
	serialized, err := pb.Marshal(&r)

	if err != nil {
		return nil
	}

	return BytesToLengthPrefixed(serialized)
}

func ResponseToBytes(r proto.RPCResponse) []byte {

	serialized, err := pb.Marshal(&r)

	if err != nil {
		return nil
	}

	return BytesToLengthPrefixed(serialized)
}

func BytesToLengthPrefixed(b []byte) []byte {
	buf := new(bytes.Buffer)

	var sizebuf [8]byte = lengthToBytes(uint64(len(b)))
	buf.Write(sizebuf[:])
	buf.Write(b)

	return buf.Bytes()
}

// Reads the length of the message from a Reader.
// If you use a net.Conn, you may use the error as net.Error
// and look if a timeout has occurred etc.
func ReadSizePrefixedMessage(r io.Reader) ([]byte, error) {
	var sizebuf [8]byte

	_, err := r.Read(sizebuf[:])

	if err != nil {
		return nil, err
	}

	length := sizebufToLength(sizebuf)

	result := make([]byte, length)

	n, err := r.Read(result)

	if err != nil {
		return nil, err
	} else if uint64(n) < length {
		return result, errors.New("Could not read promised length")
	}

	return result, nil
}
