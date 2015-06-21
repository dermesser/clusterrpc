install: protos
	go install clusterrpc/proto clusterrpc clusterrpc/server clusterrpc/client clusterrpc/crpc-keygen clusterrpc/securitymanager

build: protos
	go build clusterrpc/proto clusterrpc clusterrpc/server clusterrpc/client clusterrpc/crpc-keygen clusterrpc/securitymanager

test:
	go test

bench:
	go test -bench .

protos:
	protoc --gogofast_out=. proto/rpc.proto

