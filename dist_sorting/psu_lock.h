#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <errno.h>
#include <netdb.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#include <string>
#include <vector>
#include <fstream>
#include <iostream>
#include <map>
#include <bits/stdc++.h>

#include <grpcpp/grpcpp.h>
#include "mutex.grpc.pb.h"

using namespace std;

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;
using grpc::Channel;
using grpc::ClientContext;

using mutex::mutex_service;
using mutex::mutex_request;
using mutex::mutex_reply;
using mutex::mutex_deferred_reply;

#define MAX_LOCKS 10

bool initialized = false;

string mutex_port_no = "4999";

map<int, string> mutex_id_to_ip;
map<string, int> mutex_ip_to_id;
vector<string> other_addresses;

// my id. assigned from the list of nodes
unsigned int self_id = -1;
string self_ip;


void mutex_check_host_name(int hostname) { //This function returns host name for local computer
   if (hostname == -1) {
      perror("gethostname");
      exit(1);
   }
}
void mutex_check_host_entry(struct hostent * hostentry) { //find host info from host name
   if (hostentry == NULL){
      perror("gethostbyname");
      exit(1);
   }
}
char * mutex_get_ip() {
   char host[256];
   char *IP;
   struct hostent *host_entry;
   int hostname;
   hostname = gethostname(host, sizeof(host)); //find the host name
   host_entry = gethostbyname(host); //find host information
   mutex_check_host_entry(host_entry);
   IP = inet_ntoa(*((struct in_addr*) host_entry->h_addr_list[0])); //Convert into IP string
   return IP;
}
void mutex_get_file_contents(string filename, vector<string> &vector_of_strings) {
	ifstream in(filename.c_str());
	if(!in)
    {
        cout << "Cannot open the File : " << filename << endl;
        exit(1);
    }
	
	string str;
	while ( getline(in, str) ) {
		if (str.size() > 0) {
			vector_of_strings.push_back(str);
		}
	}
	
	in.close();
}
void mutex_initialize_ip_addresses(string filename) {
	char * ip = mutex_get_ip();
	string ip_string = string(ip);
	vector<string> vec;
	mutex_get_file_contents(filename, vec);
	for (int i = 0; i < vec.size(); i++) {
		mutex_id_to_ip[i] = vec[i];
		mutex_ip_to_id[vec[i]] = i;
		if (ip_string == vec[i]) {
			self_id = i;
			self_ip = vec[i];
		} else {
			other_addresses.push_back(vec[i]);
		}
	}
}


pthread_mutex_t sequence_no_mutex;
unsigned int my_sequence_no;
unsigned int highest_sequence_no_seen;

bool requesting_cr[MAX_LOCKS];
vector<int> deferred_replies[MAX_LOCKS];
int my_requesting_sequence_numbers[MAX_LOCKS];
vector<int> waiting_on[MAX_LOCKS];


void init_globals(){
	for (int i = 0; i < MAX_LOCKS; i++) {
		requesting_cr[i] = false;
	}
	my_sequence_no = 0;
	highest_sequence_no_seen = 0;
	for (int i = 0; i < MAX_LOCKS; i++) {
		my_requesting_sequence_numbers[i] = -1;
	}
}


class MutexServiceImplementation final : public mutex_service::Service {
    Status send_mutex_request(
        ServerContext* context, 
        const mutex_request* request, 
        mutex_reply* reply
    ) override {
		// get the message contents
        int sequence_no_from_requester = request->sequence_no();
		int requested_lockno = request->lockno();
		int requester_id = request->id();
		
		// assert that these values are not -1
		
		pthread_mutex_lock(&sequence_no_mutex);
		if (sequence_no_from_requester > highest_sequence_no_seen) {
			highest_sequence_no_seen = sequence_no_from_requester;
		}
		pthread_mutex_unlock(&sequence_no_mutex);
		
		int my_requesting_sequence_no = my_requesting_sequence_numbers[requested_lockno];
		int requesting_this_cr = requesting_cr[requested_lockno];
		bool need_to_defer = false;
		if (requesting_this_cr && (sequence_no_from_requester > my_requesting_sequence_no)) {
			need_to_defer = true;
		}
		if (requesting_this_cr && (sequence_no_from_requester == my_requesting_sequence_no) && (self_id < requester_id)) {
			need_to_defer = true;
		}
		
		int ack = 1;
		if (need_to_defer) {
			deferred_replies[requested_lockno].push_back(requester_id);
			ack = -1;
		}
		
		//cout << "Arguments that I received: " << sequence_no_from_requester << " " << requested_lockno << " " << requester_id << endl;
		
		reply->set_acknowledgement(ack);
        return Status::OK;
    } 
	
	
	Status send_deferred_reply (
        ServerContext* context, 
        const mutex_deferred_reply* request, 
        mutex_reply* reply
    ) override { 
		// get message contents
		int released_by = request->sender();
		int lockno = request->lockno();
		
		//cout << "Arguments received by deferred reply: released_by: " << released_by << ", lock_number: " << lockno << endl;
		
		// assert that these are not -1
		
		int position = -1;
		for (int i = 0; i < waiting_on[lockno].size(); i++) {
			if ( waiting_on[lockno][i] == released_by ) {
				position = i;
				break;
			}
		}
		waiting_on[lockno].erase(waiting_on[lockno].begin()+position);
		
		reply->set_acknowledgement(1);
		return Status::OK;
		
	}
};


void * run_server(void * args) {
    string address = string(self_ip) + ":" + mutex_port_no;
    MutexServiceImplementation service;
    ServerBuilder builder;
    builder.AddListeningPort(address, grpc::InsecureServerCredentials());
    builder.RegisterService(&service);
    std::unique_ptr<Server> server(builder.BuildAndStart());
    std::cout << "Server listening on port: " << address << std::endl;
    server->Wait();
}


class mutex_interface {
    public:
        mutex_interface(std::shared_ptr<Channel>channel) : stub_(mutex_service::NewStub(channel)) {}

	int send_deferred_reply (int lockno) {
		mutex_deferred_reply def_reply;
		def_reply.set_sender(self_id);
		def_reply.set_lockno(lockno);
		
		mutex_reply reply;
		
		ClientContext context;
		
		Status status = stub_->send_deferred_reply(&context, def_reply, &reply);
		
		if (status.ok()) {
			return 0;
		} else {
			return -1;
		}
	}

	int send_mutex_request (int lockno, int sequence_no) {
		mutex_request req;
		req.set_lockno(lockno);
		req.set_sequence_no(sequence_no);
		req.set_id(self_id);
		
		mutex_reply reply;
		
		ClientContext context;
		
		Status status = stub_->send_mutex_request(&context, req, &reply);
		
		if(status.ok()){
			if (reply.acknowledgement()!=-1)
				return 0;
			else
				return -1;
        } else {
            std::cout << status.error_code() << ": " << status.error_message() << std::endl;
            return -2;
        }
		
	}

    private:
        std::unique_ptr<mutex_service::Stub> stub_;
};


void enter_cr(int lockno) {
	requesting_cr[lockno] = true;
	
	pthread_mutex_lock(&sequence_no_mutex);
	my_sequence_no = highest_sequence_no_seen + 1;
	pthread_mutex_unlock(&sequence_no_mutex);
	
	my_requesting_sequence_numbers[lockno] = my_sequence_no;
	
	for(int i = 0; i < other_addresses.size(); i++) {
		string address = other_addresses[i] + ":" + mutex_port_no;
		int id = mutex_ip_to_id[other_addresses[i]];
		
		mutex_interface client(
			grpc::CreateChannel(
				address, 
				grpc::InsecureChannelCredentials()
			)
		);
		
		int response = -2;
		response = client.send_mutex_request(lockno, my_sequence_no);
		
		if (response == -1) {
			waiting_on[lockno].push_back(id);
		}
	}
	
	// TODO: some other mechanism??
	while (waiting_on[lockno].size() > 0) {
		;
	}
	
}

void send_deferred_replies(int lockno) {
	for (int i = 0; i < deferred_replies[lockno].size(); i++) {
		int id = deferred_replies[lockno][i];
		string address = mutex_id_to_ip[id] + ":" + mutex_port_no;
		
		mutex_interface client(
			grpc::CreateChannel(
				address, 
				grpc::InsecureChannelCredentials()
			)
		);
		
		client.send_deferred_reply(lockno);
	}
	
	while ( deferred_replies[lockno].size() > 0 ) {
		deferred_replies[lockno].pop_back();
	}
}

void leave_cr(int lockno) {
	requesting_cr[lockno] = false;
	send_deferred_replies(lockno);
}


void psu_init_lock(unsigned int lockno) {
	if (!initialized) {
		initialized = true;
		init_globals();
		
		mutex_initialize_ip_addresses("nodes.txt");
		cout << self_id << endl;
		cout << self_ip << endl;
		
		pthread_t server_thread;
		pthread_create(&server_thread, NULL, run_server, NULL);
	}
}


void psu_mutex_lock(unsigned int lockno) {
	enter_cr(lockno);
}

void psu_mutex_unlock(unsigned int lockno) {
	leave_cr(lockno);
}