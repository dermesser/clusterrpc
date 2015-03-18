
build: protos
	go build clusterrpc/proto clusterrpc clusterrpc/server clusterrpc/client

install: protos
	go install clusterrpc/proto clusterrpc clusterrpc/server clusterrpc/client

test:
	go test

bench:
	go test -bench .

protos:
	protoc --go_out=. proto/rpc.proto

