
build: protos
	go build

install: protos
	go install clusterrpc clusterrpc/server clusterrpc/client clusterrpc/proto

test:
	go test

bench:
	go test -bench .

protos:
	protoc --go_out=. proto/rpc.proto

