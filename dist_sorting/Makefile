LDFLAGS = -L/usr/local/lib `pkg-config --libs protobuf grpc++`\
           -Wl,--no-as-needed -lgrpc++_reflection -Wl,--as-needed\
           -ldl

CXX = g++
CPPFLAGS += `pkg-config --cflags protobuf grpc`
CXXFLAGS += -std=c++11 -fpermissive -g

GRPC_CPP_PLUGIN = grpc_cpp_plugin
GRPC_CPP_PLUGIN_PATH ?= `which $(GRPC_CPP_PLUGIN)`

all: app1 dir

app1: dsm.pb.o dsm.grpc.pb.o mutex.pb.o mutex.grpc.pb.o app1.o
	$(CXX) $^ $(LDFLAGS) -o $@

dsm.grpc.pb.cc: dsm.proto
	protoc --grpc_out=. --plugin=protoc-gen-grpc=$(GRPC_CPP_PLUGIN_PATH) $<

dsm.pb.cc: dsm.proto
	protoc --cpp_out=. $<

mutex.grpc.pb.cc: mutex.proto
	protoc --grpc_out=. --plugin=protoc-gen-grpc=$(GRPC_CPP_PLUGIN_PATH) $<

mutex.pb.cc: mutex.proto
	protoc --cpp_out=. $<

dir: dsm.pb.o dsm.grpc.pb.o dir.o
	$(CXX) $^ $(LDFLAGS) -o $@

clean:
	rm -f *.o *.pb.cc *.pb.h
