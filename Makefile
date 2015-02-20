
build: protos
	go build

install: protos
	go install

test:
	go test

bench:
	go test -bench .

protos:
	protoc --go_out=. proto/rpc.proto

