LDFLAGS = -L/usr/local/lib `pkg-config --libs protobuf grpc++`\
           -Wl,--no-as-needed -lgrpc++_reflection -Wl,--as-needed\
           -ldl

CXX = g++
CPPFLAGS += `pkg-config --cflags protobuf grpc`
CXXFLAGS += -std=c++11 -fpermissive -g

GRPC_CPP_PLUGIN = grpc_cpp_plugin
GRPC_CPP_PLUGIN_PATH ?= `which $(GRPC_CPP_PLUGIN)`

all: dir kmeans

dir: dsm.pb.o dsm.grpc.pb.o mutex.pb.o mutex.grpc.pb.o dir.o
	$(CXX) $^ $(LDFLAGS) -o $@
	
kmeans: dsm.pb.o dsm.grpc.pb.o mutex.pb.o mutex.grpc.pb.o kmeans.o
	$(CXX) $^ $(LDFLAGS) -o $@

%.grpc.pb.cc: %.proto
	protoc --grpc_out=. --plugin=protoc-gen-grpc=$(GRPC_CPP_PLUGIN_PATH) $<

%.pb.cc: %.proto
	protoc --cpp_out=. $<

clean:
	rm -f *.o *.pb.cc *.pb.h dir wc kmeans
