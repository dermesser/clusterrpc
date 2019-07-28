
LIBS=-lpthread -lprotobuf-c -lczmq
DEPS=server.h
MAIN_FILES=server.o rpc.pb-c.o

all: protos test_main.c $(MAIN_FILES)
	gcc -o test_main -g -Wall $(LIBS) $(MAIN_FILES) test_main.c

%.o: %.c $(DEPS)
	gcc -g -c -o $@ $< $(CFLAGS)

protos: ../proto/rpc.proto
	cd ../proto && protoc --c_out=../c/ rpc.proto
	gcc -c *.pb-c.c

clean:
	rm -f *.o *.pb-c.* test_main
