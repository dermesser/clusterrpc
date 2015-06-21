install: protos
	go install clusterrpc/proto clusterrpc clusterrpc/server clusterrpc/client clusterrpc/crpc-keygen

build: protos
	go build clusterrpc/proto clusterrpc clusterrpc/server clusterrpc/client clusterrpc/crpc-keygen

test:
	go test

bench:
	go test -bench .

protos:
	protoc --gogofast_out=. proto/rpc.proto

