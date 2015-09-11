install: protos
	go install clusterrpc/proto clusterrpc clusterrpc/server clusterrpc/client clusterrpc/crpc-keygen clusterrpc/securitymanager clusterrpc/log

build: protos
	go build clusterrpc/proto clusterrpc clusterrpc/server clusterrpc/client clusterrpc/crpc-keygen clusterrpc/securitymanager clusterrpc/log

test:
	go test

bench:
	go test -bench .

protos:
	protoc --gogofast_out=. proto/rpc.proto

