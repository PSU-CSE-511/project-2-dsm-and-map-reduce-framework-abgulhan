#include <sys/mman.h>
#include <errno.h>
#include <signal.h>
#include <ucontext.h>
#include <stdint.h>
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
#include <assert.h>

#include <string>
#include <vector>
#include <fstream>
#include <iostream>
#include <map>
#include <bits/stdc++.h>
#include <cmath>

#include <grpcpp/grpcpp.h>
#include "dsm.grpc.pb.h"

using namespace std;

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;
using grpc::Channel;
using grpc::ClientContext;

using dsm::dsm_service;
using dsm::dsm_request;
using dsm::dsm_reply;

#define handle_error(msg) \
    do { perror(msg); exit(EXIT_FAILURE); } while (0)

#define MAX_PAGES 	100
#define MAX_NODES	10
	
#define INVALID		0
#define READ_ONLY	1
#define READ_WRITE	2
#define DIRECTORY	3

#define READ_REQ	0
#define WRITE_REQ	1
#define ACK			2
#define INVALIDATE	3
#define REGISTER	4
#define UNREGISTER	5
#define HEAP_REG	6

#define NO_ACK		0
#define ACK_NO_DATA	1
#define ACK_W_DATA	2
#define ACK_W_PERM	3

string port_no = "4002";

bool is_initialized = false;

int num_pages = 0;
int num_nodes = 0;
int my_id = -1;
int directory_id = -1;
int state_of_pages[MAX_PAGES];
char null_data[4096] = {0}; //Find a better way to allow null string field in grpc calls
string null_str = std::string();


map<uint64_t, int> vaddr_to_entry;
map<int, uint64_t> entry_to_vaddr;

map<int, string> id_to_ip;
map<string, int> ip_to_id;

map<uint64_t, string> vaddr_to_name;//used locally
map<string, uint64_t> name_to_vaddr;

map<uint64_t, string> entry_to_name;//used by directory 
map<string, uint64_t> name_to_entry;

typedef struct directory_entry {
	bool present[MAX_NODES];
	int state;
	pthread_mutex_t lock;
	int locked_for;
	int intended_state;
	bool is_registered;
	char data[4096];
	string name;
	// TODO
} directory_entry_t;

directory_entry_t directory[MAX_PAGES];

pthread_mutex_t num_pages_lock;

void check_host_name(int hostname) { //This function returns host name for local computer
   if (hostname == -1) {
      perror("gethostname");
      exit(1);
   }
}
void check_host_entry(struct hostent * hostentry) { //find host info from host name
   if (hostentry == NULL){
      perror("gethostbyname");
      exit(1);
   }
}
char * get_ip() {
   char host[256];
   char *IP;
   struct hostent *host_entry;
   int hostname;
   hostname = gethostname(host, sizeof(host)); //find the host name
   host_entry = gethostbyname(host); //find host information
   check_host_entry(host_entry);
   IP = inet_ntoa(*((struct in_addr*) host_entry->h_addr_list[0])); //Convert into IP string
   return IP;
}
void get_file_contents(string filename, vector<string> &vector_of_strings) {
	ifstream in(filename.c_str());
	if(!in)
    {
        //cout << "Cannot open the File : " << filename << endl;
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
void initialize_ip_addresses(string filename) {
	char * ip = get_ip();
	string ip_string = string(ip);
	vector<string> vec;
	get_file_contents(filename, vec);
	for (int i = 0; i < vec.size(); i++) {
		if (vec[i] == "") {
			break;
		}
		num_nodes = i+1;
		id_to_ip[i] = vec[i];
		ip_to_id[vec[i]] = i;
		if (ip_string == vec[i]) {
			my_id = i;
		}
	}
	directory_id = num_nodes - 1;
}

class dsm_services_interface {
	public:
        dsm_services_interface(std::shared_ptr<Channel>channel) : stub_(dsm_service::NewStub(channel)) {}
	
	dsm_reply send_dsm_request(int type, uint64_t vaddr, int node_id, char data[4096] = {0}, string name = "") {
		dsm_request request __attribute__ ((aligned (4096)));
		request.set_type(type);
		request.set_vaddr(vaddr);
		request.set_id(node_id);
		request.set_data(data, 4096);
		request.set_name(name);
		
		dsm_reply reply __attribute__ ((aligned (4096)));
		
		ClientContext context __attribute__ ((aligned (4096)));
		
		//cout << "sending request: type=" << type << ", vaddr=" << vaddr << ", node_id=" << node_id << endl;
		//cout << "Address of context: " << &context << endl;
		//cout << "Address of request: " << &request << endl;
		//cout << "Address of reply:	 " << &reply << endl;
		
		//cout << "Address of stub: " << &stub_ << endl;
		
		Status status = stub_->send_dsm_request(&context, request, &reply);
		
		//cout << "sent request: type=" << type << ", vaddr=" << vaddr << ", node_id=" << node_id << endl;
		
		if (status.ok()) {
			return reply;
		} else {
			handle_error("Did not receive reply! Exiting...\n");
		}
	}
	
	private:
        std::unique_ptr<dsm_service::Stub> stub_;
};


dsm_services_interface* dir_interface;

class DsmServiceImplementaion final : public dsm_service::Service {
	Status send_dsm_request(
        ServerContext* context, 
        const dsm_request* request, 
        dsm_reply* reply
    ) override {
		int request_type = request->type();
		uint64_t vaddr = request->vaddr();
		int requester_id = request->id();
		char received_data[4096];// = request->data();
		memcpy(received_data, &request->data().front(), 4096);
		string name = request->name();
		
		if (directory_id == my_id || true){
			string type_str;
			switch(request_type){
				case 0: type_str = "READ_REQ"; break;
				case 1: type_str = "WRITE_REQ"; break;
				case 2: type_str = "ACK"; break;
				case 3: type_str = "INVALIDATE"; break;
				case 4: type_str = "REGISTER"; break;
				case 5: type_str = "UNREGISTER"; break;
				case 6: type_str = "HEAP_REG"; break;
				default: type_str = to_string(request_type);
			}
			//cout << "Received request from " << requester_id << " with type " << type_str << " for address: " ;
			//printf("0x%lx\n",(long) vaddr);
			
		}
		
		if (directory_id != my_id) {
			// I am a holder of data. these requests are from directory
			if (request_type == READ_REQ) {
				// I need to shift to READ_ONLY after sending my data
				// TODO: data needs to be larger!
				if (!vaddr) //if received request is a heap then convert name into vaddr
					vaddr = name_to_vaddr[name];
				
				//int * ptr = (int *) vaddr;
				char data[4096] = {0};
				memcpy(data, (void *) vaddr, 4096);//int data = *ptr;
				reply->set_ack(ACK_W_DATA);
				reply->set_data(data, 4096);
				
				mprotect((void*) vaddr, 4096, PROT_READ);
				state_of_pages[ vaddr_to_entry[vaddr] ] = READ_ONLY;
				int * ptr = (int *) vaddr;
				//printf("I change to read only here! for addr: 0x%lx, sent-data is: %d\n",(long) vaddr, *ptr);
			} else if  (request_type == INVALIDATE) {
				// I am in READ_ONLY state, need to invalidate my copy and send ack
				if (state_of_pages[ vaddr_to_entry[vaddr] ] != READ_ONLY) {
					//cout << "Not in readonly!\n" << endl;
					//cout << "Actual state: " << state_of_pages[ vaddr_to_entry[vaddr] ] << endl;
				}
				if (!vaddr)
					vaddr = name_to_vaddr[name];
					
				char data[4096] = {0};
				memcpy(data, (void *) vaddr, 4096);//int data = *ptr;
				reply->set_ack(ACK_W_DATA);
				reply->set_data(data, 4096);
				
				mprotect((void*) vaddr, 4096, PROT_NONE);
				state_of_pages[ vaddr_to_entry[vaddr] ] = INVALID;
				//printf("I invalidated my copy here! for addr: 0x%lx\n",(long) vaddr);
				
				//reply->set_ack(ACK_NO_DATA);
			}
		}
		// end actions as holder
		else {
			// actions as directory
			if (request_type == REGISTER) { // requester want to register this vaddr
				if (vaddr_to_entry.count(vaddr)) { // this is already registered
					int index = vaddr_to_entry[vaddr];
					assert(directory[index].is_registered);
					reply->set_ack(ACK_W_PERM);
					reply->set_permission(INVALID);
				} else { // this is not registered
					// first, allocate an entry for this
					pthread_mutex_lock(&num_pages_lock);
					vaddr_to_entry[vaddr] = num_pages;
					num_pages++;
					int index = vaddr_to_entry[vaddr];
					pthread_mutex_lock(&directory[index].lock);
					pthread_mutex_unlock(&num_pages_lock);
					
					assert(!directory[index].is_registered);
					
					// now, we have allocated entry, and locked it atomically
					// now, grant rd/wrt access
					directory[index].locked_for = requester_id;
					directory[index].is_registered = true;
					directory[index].intended_state = READ_WRITE;
					reply->set_ack(ACK_W_PERM);
					reply->set_permission(READ_WRITE);
				}
			} else if (request_type == HEAP_REG) { // requester wants to register a heap segment. 
				//string name = request->name(); 
				//check if name exists
				//cout << "checking if " << name << " exists in directory" << endl;
				if (name_to_entry.count(name)){//already registered
					int index = name_to_entry[name];
					assert(directory[index].is_registered);
					reply->set_ack(ACK_W_PERM);
					reply->set_permission(INVALID);	
				} else { // this is not registered
					// first, allocate an entry for this
					pthread_mutex_lock(&num_pages_lock);
					name_to_entry[name] = num_pages;
					num_pages++;
					int index = name_to_entry[name];
					pthread_mutex_lock(&directory[index].lock);
					pthread_mutex_unlock(&num_pages_lock);
					
					assert(!directory[index].is_registered);
					
					// now, we have allocated entry, and locked it atomically
					// now, grant rd/wrt access
					directory[index].locked_for = requester_id;
					directory[index].is_registered = true;
					directory[index].intended_state = READ_WRITE;
					reply->set_ack(ACK_W_PERM);
					reply->set_permission(READ_WRITE);
					
				}	
			} else if (request_type == ACK) {				
				// I am directory, this is an ACK. Need to update presence bits and unlock
				int index = -1;
				if (vaddr) {
					index = vaddr_to_entry[vaddr];
				} else {	 
					index = name_to_entry[name];
				}
				if ( directory[index].locked_for == requester_id ) {
					if (directory[index].intended_state == READ_WRITE) {
						for (int i = 0; i < MAX_NODES; i++) directory[index].present[i] = false;
					}
					directory[index].present[requester_id] = 1;
					directory[index].state = directory[index].intended_state;
					directory[index].intended_state = -1;
					pthread_mutex_unlock(&directory[index].lock);
					//cout << "vaddr: "; 
					//printf("0x%lx\n",(long) vaddr);
					//cout << " now held by: " << requester_id << endl;
					//cout << "presence bits: ";
					for (int i = 0; i < 6; i++) {
						//cout << directory[index].present[i] << " ";
					}
					//cout << endl;
					//cout << "unlocked successfully for node " << requester_id << endl;
				}
			} else if (request_type == READ_REQ) {
				// assuming this is not present at the requester
				bool is_heap = false;
				int index = -1;
				if (vaddr) {
					index = vaddr_to_entry[vaddr];
				} else {	 
					index = name_to_entry[name];
					is_heap = true;
				}
				assert ( !directory[index].present[requester_id] );
				
				// lock entry for requester
				pthread_mutex_lock(&directory[index].lock);
				directory[index].locked_for = requester_id;
				directory[index].intended_state = READ_ONLY;
				
				// find the holder of data
				int holder_id = -1;
				for (int i = 0; i < MAX_NODES; i++) {
					if (directory[index].present[i]) {
						holder_id = i;
						break;
					}
				}
				assert (holder_id != -1);
				
				//cout << "Holder is: Node " << holder_id << endl;
				
				if (holder_id != directory_id) {
					string holder_address = id_to_ip[holder_id] + ":" + port_no;
					//cout << "Holder's address is: " << holder_address << endl;
					dsm_services_interface client(
						grpc::CreateChannel(
							holder_address, 
							grpc::InsecureChannelCredentials()
						)
					);
					
					dsm_reply reply_from_holder;
					if (is_heap){
						//char data[4096] = {0};
						reply_from_holder = client.send_dsm_request(READ_REQ, 0, directory_id, null_data, name);
					}
					else	
						reply_from_holder = client.send_dsm_request(READ_REQ, vaddr, directory_id, null_data, null_str);
					
					if (reply_from_holder.ack() == ACK_W_DATA) {
						reply->set_ack(ACK_W_DATA);
						reply->set_data(&reply_from_holder.data().front(), 4096);
						int *ptr = (int *)&reply_from_holder.data().front();
						//cout << "got int data from holder: " << *ptr << " at addr: " << vaddr << endl;
						//cout << "now sending this over." << endl;
					}
				} else {
					directory[index].present[directory_id] = 0;
					reply->set_ack(ACK_W_DATA);
					reply->set_data(directory[index].data, 4096);
				}
				
			} else if (request_type == WRITE_REQ) {
				// this request comes only when all holders are in read_only
				//cout << "write request called" << endl;
				bool is_heap = false;
				int index = -1;
				if (vaddr) {
					index = vaddr_to_entry[vaddr];
				} else {	 
					index = name_to_entry[name];
					is_heap = true;
				}
				//assert (directory[index].state == READ_ONLY);
				
				pthread_mutex_lock(&directory[index].lock);
				directory[index].locked_for = requester_id;
				directory[index].intended_state = READ_WRITE;
				
				if (directory[index].state == DIRECTORY) {
					//printf("reached for vaddr: 0x%lx\n", vaddr);
					directory[index].present[directory_id] = 0;
					reply->set_ack(ACK_W_DATA);
					reply->set_data(directory[index].data, 4096);
				} else {
					dsm_reply reply_from_holder;
					bool reply_found = false;
					for (int i = 0; i < num_nodes; i++) {
						if ( directory[index].present[i] && i != requester_id ) {
							string holder_address = id_to_ip[i] + ":" + port_no;
							dsm_services_interface client(
								grpc::CreateChannel(
									holder_address, 
									grpc::InsecureChannelCredentials()
								)
							);
							if (is_heap) {
								//char data[4096] = {0};
								reply_from_holder = client.send_dsm_request(INVALIDATE, 0, directory_id, null_data, name);	
							}
							else{
								reply_from_holder = client.send_dsm_request(INVALIDATE, vaddr, directory_id, null_data, null_str);
							}
							reply_found = true;
							assert ( reply_from_holder.ack() == ACK_W_DATA );
							directory[index].present[i] = false;
						}
					}
					if (reply_found) {
						reply->set_ack(ACK_W_DATA);
						reply->set_data(&reply_from_holder.data().front(), 4096);
					} else {
						reply->set_ack(ACK_NO_DATA);
					}
					
				}
				
			} else if (request_type == UNREGISTER) {
				int index = -1;
				if (vaddr) {
					index = vaddr_to_entry[vaddr];
				} else {	 
					index = name_to_entry[name];
				}
				pthread_mutex_lock(&directory[index].lock);
				memcpy(directory[index].data, received_data, 4096);//directory[index].data = received_data;
				directory[index].present[requester_id] = 0;
				bool only_at_directory = true;
				for (int i = 0; i < directory_id; i++) {
					if (directory[index].present[i]) {
						only_at_directory = false;
					}
				}
				if (only_at_directory) {
					directory[index].state = DIRECTORY;
					directory[index].present[directory_id] = 1;
				}
				//cout << "Set data to: " << directory[index].data << endl;
				pthread_mutex_unlock(&directory[index].lock);
			}
		}
		// end actions as directory
		
		return Status::OK;
		
	}
};


void * start_server(void * args) {
	string address = string( id_to_ip[my_id] ) + ":" + port_no;
	DsmServiceImplementaion service;
	ServerBuilder builder;
	builder.AddListeningPort(address, grpc::InsecureServerCredentials());
	builder.RegisterService(&service);
	std::unique_ptr<Server> server(builder.BuildAndStart());
	std::cout << "Non-directory Server listening on port: " << address << std::endl;
	server->Wait();
}

void handler(int sig, siginfo_t *si, void *contextptr) {
	//printf("Got SIGSEGV at address: 0x%lx\n",(long) si->si_addr);
    
	// TODO
	uint64_t vaddr = si->si_addr;
	ucontext_t *const ctx = (ucontext_t *const)contextptr; //used to detect what operation caused sigsegv
	for (uint64_t i=0; i<4096; i++){
		if (vaddr_to_entry.count(vaddr-i)){
			vaddr -= i;
			//cout << "page table access offset: " << i << endl;
			//printf("page table start address: 0x%lx\n",(long) si->si_addr);
			//printf("converted vaddr: 0x%lx\n", vaddr);
			break;
		}
	}
	if ( !vaddr_to_entry.count(vaddr) ) {
		cout << "internal vaddr: " << vaddr << endl;
		handle_error("Segmentation fault at internal address.");
	}
	mprotect((void*) vaddr, 4096, PROT_READ | PROT_WRITE);
	
	string dir_addr = id_to_ip[directory_id] + ":" + port_no;
	dsm_services_interface client(
		grpc::CreateChannel(
			dir_addr, 
			grpc::InsecureChannelCredentials()
		)
	);
	
    //printf("Implements the handler only\n");
	
	int index = vaddr_to_entry[vaddr];
	int state = state_of_pages[index];
	
	string name;
	bool heap = false;
	if (vaddr_to_name.count(vaddr)){
		heap = true;
		name = vaddr_to_name[vaddr];
	}
	//cout << "Here" << endl;
	
	if (state == INVALID) {
		//First get read access then get write access if necessary
		
		/*read access*/
		//cout << "reading when invalid " << endl;
		//dsm_reply reply_from_dir = dir_interface->send_dsm_request(READ_REQ, vaddr, my_id);
		dsm_reply reply_from_dir;
		if (heap){
			//char data[4096] = {0};
			reply_from_dir = client.send_dsm_request(READ_REQ, 0, my_id, null_data, name);
		}
		else {
			//printf("Trying this: send READ_REQ for vaddr: 0x%lx\n",(long) vaddr);
			reply_from_dir = client.send_dsm_request(READ_REQ, vaddr, my_id, null_data, null_str);
		}
		
		if (reply_from_dir.ack() == ACK_W_DATA) {
			//cout << "got reply for read!" << endl;
			mprotect((void*) vaddr, 4096, PROT_READ|PROT_WRITE);
			//char * ptr = si->si_addr;
			memcpy((void*) vaddr, &reply_from_dir.data().front(), 4096);//*ptr = reply_from_dir.data();
			mprotect((void*) vaddr, 4096, PROT_READ);
			state_of_pages[index] = READ_ONLY;
			//dir_interface->send_dsm_request(ACK, vaddr, my_id);
			if (heap){
				//char data[4096] = {0};
				client.send_dsm_request(ACK, 0, my_id, null_data, name);
			}
			else
				client.send_dsm_request(ACK, vaddr, my_id, null_data, null_str);
			
			int * ptr = (int *) vaddr;
			//cout << "Got read-only permission on " << vaddr << " where int data is: " << *ptr << endl;
			
		} else {
			handle_error("did not get read-only permission! stuck at invalid!");
		}
		
		
		if (ctx->uc_mcontext.gregs[REG_ERR] & 2){ /*write access*/
			//cout << "Also writing when invalid" << endl;
			if (heap){
				//char data[4096] = {0};
				reply_from_dir = client.send_dsm_request(WRITE_REQ, 0, my_id, null_data, name);
			}
			else
				reply_from_dir = client.send_dsm_request(WRITE_REQ, vaddr, my_id, null_data, null_str);
			if (reply_from_dir.ack() == ACK_W_DATA) {
				//cout << "got reply for write!" << endl;
				mprotect((void*) vaddr, 4096, PROT_READ|PROT_WRITE);
				//memcpy((void*) vaddr, &reply_from_dir.data().front(), 4096);//*ptr = reply_from_dir.data();
				state_of_pages[index] = READ_WRITE;
				memcpy((void*) vaddr, &reply_from_dir.data().front(), 4096);
				if (heap){
					//char data[4096] = {0};
					client.send_dsm_request(ACK, 0, my_id, null_data, name);
				}
				else
					client.send_dsm_request(ACK, vaddr, my_id, null_data, null_str);
				int * ptr = (int *) vaddr;
				//cout << "Got read-write permission on " << vaddr << " where int data is: " << *ptr << endl;
			} else if (reply_from_dir.ack() == ACK_NO_DATA) {
				//cout << "got reply for write!" << endl;
				mprotect((void*) vaddr, 4096, PROT_READ|PROT_WRITE);
				state_of_pages[index] = READ_WRITE;
				if (heap){
					client.send_dsm_request(ACK, 0, my_id, null_data, name);
				} else
					client.send_dsm_request(ACK, vaddr, my_id, null_data, null_str);
				int * ptr = (int *) vaddr;
				//cout << "Got read-write permission on " << vaddr << " where int data is: " << *ptr << endl;
			} else {
				handle_error("did not get read/write permission! stuck at invalid!");
			}	
		}
	} else if (state == READ_ONLY) {
		//cout << "state is read only" << endl;
		dsm_reply reply_from_dir;
		if (heap) {
			//char data[4096] = {0};
			reply_from_dir = client.send_dsm_request(WRITE_REQ, 0, my_id, null_data, name);
		}
		else {
			//reply_from_dir = dir_interface->send_dsm_request(WRITE_REQ, vaddr, my_id);
			reply_from_dir = client.send_dsm_request(WRITE_REQ, vaddr, my_id, null_data, null_str);
		}
		if (reply_from_dir.ack() == ACK_W_DATA) {
			//cout << "got reply for write!" << endl;
			mprotect((void*) vaddr, 4096, PROT_READ|PROT_WRITE);
			state_of_pages[index] = READ_WRITE;
			memcpy((void*) vaddr, &reply_from_dir.data().front(), 4096);
			//dir_interface->send_dsm_request(ACK, vaddr, my_id);
			if (heap){
				//char data[4096] = {0};
				client.send_dsm_request(ACK, 0, my_id, null_data, name);
			}
			else
				client.send_dsm_request(ACK, vaddr, my_id, null_data, null_str);
			int * ptr = (int *) vaddr;
			//cout << "Got read-write permission on " << vaddr << " where int data is: " << *ptr << endl;
		} else if (reply_from_dir.ack() == ACK_NO_DATA) {
			mprotect((void*) vaddr, 4096, PROT_READ|PROT_WRITE);
			state_of_pages[index] = READ_WRITE;
			if (heap){
				client.send_dsm_request(ACK, 0, my_id, null_data, name);
			}
			else
				client.send_dsm_request(ACK, vaddr, my_id, null_data, null_str);
			int * ptr = (int *) vaddr;
			//cout << "Got read-write permission on " << vaddr << " where int data is: " << *ptr << endl;
		} else {
			handle_error("did not get read-write permission! stuck at invalid!");
		}
	}
	//cout << "Exiting handler..." << endl;
}

void cleanup() {
	//cout << "exiting..." << endl;
	for (int i = 0; i < num_pages; i++) {
		if (state_of_pages[i] == READ_WRITE || state_of_pages[i] == READ_ONLY) {
			string address = string(id_to_ip[directory_id]) + ":" + port_no;
			dsm_services_interface client(
				grpc::CreateChannel(
					address, 
					grpc::InsecureChannelCredentials()
				)
			);
			if (vaddr_to_entry.count(entry_to_vaddr[i])) {
				uint64_t vaddr = entry_to_vaddr[i];
				char data[4096];//int data = *((int *)vaddr);
				memcpy(data, (void *)vaddr, 4096);
				//cout << "Sending data: " << data << endl;
				client.send_dsm_request(UNREGISTER, vaddr, my_id, data, null_str);
			} else {
				uint64_t vaddr = entry_to_vaddr[i];
				char data[4096];//int data = *((int *)vaddr);
				memcpy(data, (void *)vaddr, 4096);
				string name = entry_to_name[i];
				//cout << "Sending data: " << data << endl;
				client.send_dsm_request(UNREGISTER, 0, my_id, data, name);
			}
		}
	}
}

void initialize_globals(string filename, bool is_directory = false) {
		
	atexit(cleanup);
		
	initialize_ip_addresses(filename);
	
	string dir_addr = id_to_ip[directory_id] + ":" + port_no;
	dsm_services_interface client(
		grpc::CreateChannel(
			dir_addr, 
			grpc::InsecureChannelCredentials()
		)
	);
	dir_interface = &client;
	
	struct sigaction sa;
    sa.sa_flags = SA_SIGINFO;
    sigemptyset(&sa.sa_mask);
    sa.sa_sigaction = handler;
    if (sigaction(SIGSEGV, &sa, NULL) == -1)
        handle_error("sigaction");
	
	if (is_directory && directory_id != my_id) {
		//cout << "This is not directory node!" << endl;
		handle_error("Exiting on non-directory node.");
	}
	if (!is_directory && directory_id == my_id) {
		//cout << "This is directory node, run dir here!" << endl;
		handle_error("Exiting on directory node.");
	}
	if (directory_id == my_id) {
		// init directory structures
		for (int i = 0; i < MAX_PAGES; i++) {
			for (int j = 0; j < MAX_NODES; j++) {
				directory[i].present[j] = 0;
			}
			directory[i].state = -1;
			directory[i].locked_for = -1;
			directory[i].intended_state = -1;
			directory[i].is_registered = false;
			directory[i].name = "";
		}
		string address = string( id_to_ip[my_id] ) + ":" + port_no;
		DsmServiceImplementaion service;
		ServerBuilder builder;
		builder.AddListeningPort(address, grpc::InsecureServerCredentials());
		builder.RegisterService(&service);
		std::unique_ptr<Server> server(builder.BuildAndStart());
		//std::cout << "Directory Server listening on port: " << address << std::endl;
		server->Wait();
	} else {
		// init local structures
		for (int i = 0; i < MAX_PAGES; i++) {
			state_of_pages[i] = -1;
		}
		pthread_t thread_id;
		pthread_create(&thread_id, NULL, start_server, NULL);
	}
}

/// assuming that psu_ds_size is multiple of 4096
void psu_dsm_register_datasegment(void* psu_ds_start, size_t psu_ds_size) {
	if (!is_initialized) {
		initialize_globals("nodes.txt");
		is_initialized = true;
	}
	
	for (int i = 0; i < ( ceil((float)psu_ds_size/(float)4096) ) ; i++) {
		uint64_t vaddr = (uint64_t)psu_ds_start + i * 4096;
		if ( !vaddr_to_entry.count(vaddr) ) {
			pthread_mutex_lock(&num_pages_lock);
			vaddr_to_entry[vaddr] = num_pages;
			entry_to_vaddr[num_pages] = vaddr;
			num_pages++;
			pthread_mutex_unlock(&num_pages_lock);
			
			string address = string(id_to_ip[directory_id]) + ":" + port_no;
			dsm_services_interface client(
				grpc::CreateChannel(
					address, 
					grpc::InsecureChannelCredentials()
				)
			);
			
			dsm_reply reply = client.send_dsm_request(REGISTER, vaddr, my_id, null_data, null_str);
			if (reply.ack() != ACK_W_PERM) {
				handle_error("Some error occurred at registering.\n");
			}
			if (reply.permission() == READ_WRITE) {
				//cout << "registered! granted RDWT access!" << endl;
				// TODO: update self state to READ_WRITE, use mprotect()
				int index = vaddr_to_entry[vaddr];
				state_of_pages[index] = READ_WRITE;
				mprotect((void *)vaddr, 4096, PROT_READ | PROT_WRITE);
				client.send_dsm_request(ACK, vaddr, my_id, null_data, null_str);
			}
			if (reply.permission() == INVALID) {
				// TODO: update self state to INVALID, use mprotect()
				int index = vaddr_to_entry[vaddr];
				state_of_pages[index] = INVALID;
				mprotect((void *)vaddr, 4096, PROT_NONE);
				//cout << "registered! but not granted access!" << endl;
			}
		}
	}
		
	// for testing only
	/*
	cout << "Num pages: " << num_pages << endl;
	cout << "My id: " << my_id << endl;
	cout << "Directory id: " << directory_id << endl;
	for (int i = 0; i < MAX_PAGES; i++) {
		cout << state_of_pages[i] << " ";
	}
	cout << endl;
	*/
}

void * psu_dsm_malloc(char* name, size_t size) { //should return value be of type psu_dsm_ptr_t ?
	if (!is_initialized) {
		initialize_globals("nodes.txt");
		is_initialized = true;
	}
	
	
	string str_name = string(name);
	void * heap = NULL;
	int units = 0;
	
	units = ceil( (float)size/(float)4096 );
	heap = mmap(0, 4096 * units, PROT_NONE, MAP_ANON|MAP_PRIVATE, 0, 0);//malloc(4096); //TODO: if size > 4096
	if (heap == (void *)-1) {
		handle_error("mmap to allocate memory");
	}
	
	for (int i = 0; i < units ; i++) {
		string address = string(id_to_ip[directory_id]) + ":" + port_no;
		//send name to DSM to check if this heap location has been allocated before
		dsm_services_interface client(
			grpc::CreateChannel(
				address, 
				grpc::InsecureChannelCredentials()
			)
		);
		
		if (i>0){ //assign a unique name to each page
			str_name = string(name) + "(" + string(to_string(i)) + ")"; //TODO: Find a better way to do this
		}
		
	
		uint64_t vaddr = (uint64_t)heap + (i*4096);
		//cout << "address of heap page: " << vaddr << endl;
		//printf("address of heap page: 0x%lx\n",(long) heap);
		if ( !vaddr_to_entry.count(vaddr) ) {
			pthread_mutex_lock(&num_pages_lock);
			vaddr_to_entry[vaddr] = num_pages;
			entry_to_vaddr[num_pages] = vaddr;
			num_pages++;
			pthread_mutex_unlock(&num_pages_lock);
					 
			name_to_vaddr[str_name] = vaddr;
			vaddr_to_name[vaddr] = str_name;
		}
		else{
			munmap((void *)vaddr, 4096);//free(heap);
			handle_error("Register heap failed!");
		}
		//cout <<"sending gprc... " << endl;
		//char data[4096]={0}; //TODO: find a better way to send empty data parameter
		dsm_reply reply = client.send_dsm_request(HEAP_REG, 0, my_id, null_data, str_name); 
		
		if (reply.permission() == READ_WRITE){//first time registering heap with this name	
			//cout << "Got R/W permission" << endl;
			int index = vaddr_to_entry[vaddr];
			state_of_pages[index] = READ_WRITE;
			mprotect((void *)vaddr, 4096, PROT_READ | PROT_WRITE);
			client.send_dsm_request(ACK, 0, my_id, null_data, str_name); //TODO: send non int data. Need to modify RPC
		}
		
		else if (reply.permission() == INVALID){//heap with this name has been registered before
			//cout << "Got invalid permission" << endl;
			int index = vaddr_to_entry[vaddr];
			state_of_pages[index] = INVALID;
			mprotect((void *)vaddr, 4096, PROT_NONE);
		}

		else{
			handle_error("did not get valid reply for heap!");
		}
	}
		
	// for testing only
	/*
	cout << "Num pages: " << num_pages << endl;
	cout << "My id: " << my_id << endl;
	cout << "Directory id: " << directory_id << endl;
	for (int i = 0; i < MAX_PAGES; i++) {
		cout << state_of_pages[i] << " ";
	}
	cout << endl;
	*/
	return heap;
}
