
CFLAGS=-g -Wall
LIBS=-lpthread -lprotobuf-c -lzmq -lczmq
DEPS=server.h
MAIN_FILES=server.o rpc.pb-c.o

all: protos test_main.c $(MAIN_FILES)
	gcc $(LIBS) -o test_main $(CFLAGS) $(MAIN_FILES) test_main.c

%.o: %.c $(DEPS)
	gcc -c $(CFLAGS) -o $@ $<

protos: ../proto/rpc.proto
	cd ../proto && protoc --c_out=../c/ rpc.proto
	gcc -c *.pb-c.c

clean:
	rm -f *.o *.pb-c.* test_main
