LDFLAGS = -L/usr/local/lib `pkg-config --libs protobuf grpc++`\
           -Wl,--no-as-needed -lgrpc++_reflection -Wl,--as-needed\
           -ldl

CXX = g++
CPPFLAGS += `pkg-config --cflags protobuf grpc`
CXXFLAGS += -std=c++11 -fpermissive -g

GRPC_CPP_PLUGIN = grpc_cpp_plugin
GRPC_CPP_PLUGIN_PATH ?= `which $(GRPC_CPP_PLUGIN)`

all: app1malloc dir heap_test

app1malloc: dsm.pb.o dsm.grpc.pb.o mutex.pb.o mutex.grpc.pb.o app1malloc.o
	$(CXX) $^ $(LDFLAGS) -o $@
	
dir: dsm.pb.o dsm.grpc.pb.o dir.o
	$(CXX) $^ $(LDFLAGS) -o $@
	
heap_test: dsm.pb.o dsm.grpc.pb.o heap_test.o
	$(CXX) $^ $(LDFLAGS) -o $@

dsm.grpc.pb.cc: dsm.proto
	protoc --grpc_out=. --plugin=protoc-gen-grpc=$(GRPC_CPP_PLUGIN_PATH) $<

dsm.pb.cc: dsm.proto
	protoc --cpp_out=. $<

mutex.grpc.pb.cc: mutex.proto
	protoc --grpc_out=. --plugin=protoc-gen-grpc=$(GRPC_CPP_PLUGIN_PATH) $<

mutex.pb.cc: mutex.proto
	protoc --cpp_out=. $<

clean:
	rm -f *.o *.pb.cc *.pb.h p3 dir
