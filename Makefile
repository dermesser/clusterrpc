PREFIX=github.com/dermesser/

install: protos deps
	go install ${PREFIX}clusterrpc/proto \
	    ${PREFIX}clusterrpc \
	    ${PREFIX}clusterrpc/server \
	    ${PREFIX}clusterrpc/client \
	    ${PREFIX}clusterrpc/crpc-keygen \
	    ${PREFIX}clusterrpc/securitymanager \
	    ${PREFIX}clusterrpc/log

build: protos
	go build ${PREFIX}clusterrpc/proto \
	    ${PREFIX}clusterrpc \
	    ${PREFIX}clusterrpc/server \
	    ${PREFIX}clusterrpc/client \
	    ${PREFIX}clusterrpc/crpc-keygen \
	    ${PREFIX}clusterrpc/securitymanager \
	    ${PREFIX}clusterrpc/log

deps:
	go get github.com/gogo/protobuf/proto \
	    github.com/pebbe/zmq4

test:
	go test

bench:
	go test -bench .

protos:
	protoc --gogofast_out=. proto/rpc.proto

