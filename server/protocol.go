package server

import "log"

// Support types for dealing with ZeroMQ multi-frame messages.
// This is supposed to put an end to endless inconsistencies and bugs when dealing with the framing of
// channel messages in the backend.

type clientMessage struct {
	requestId []byte
	clientId  []byte
	payload   []byte
}

func newClientMessage(requestId []byte, clientId []byte, payload []byte) clientMessage {
	return clientMessage{requestId: requestId, clientId: clientId, payload: payload}
}

func parseClientMessage(msg [][]byte) clientMessage {
	if len(msg) != 4 {
		log.Panic("clientMessage message has != 4 frames!", len(msg))
	}

	return clientMessage{requestId: msg[0], clientId: msg[1], payload: msg[3]}
}

func (msg clientMessage) serializeClientMessage() [][]byte {
	frames := make([][]byte, 4)
	frames[0] = msg.requestId
	frames[1] = msg.clientId
	frames[2] = []byte{}
	frames[3] = msg.payload
	return frames
}

type backendMessage struct {
	workerId []byte
	message  clientMessage
}

func newBackendMessage(workerId []byte, msg clientMessage) backendMessage {
	return backendMessage{workerId: workerId, message: msg}
}

func parseBackendMessage(msg [][]byte) backendMessage {
	if len(msg) != 6 {
		log.Panic("backendMessage has != 6 frames!", len(msg))
	}

	message := backendMessage{workerId: msg[0]}
	message.message = parseClientMessage(msg[2:])
	return message
}

func (msg backendMessage) serializeBackendMessage() [][]byte {
	frames := make([][]byte, 2, 6)
	clientFrames := msg.message.serializeClientMessage()

	frames[0] = msg.workerId
	frames[1] = []byte{}

	return append(frames, clientFrames...)
}
