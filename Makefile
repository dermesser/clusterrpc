
build: protos
	go build

test:
	go test

bench:
	go test -bench .

protos:
	protoc --go_out=. proto/rpc.proto

