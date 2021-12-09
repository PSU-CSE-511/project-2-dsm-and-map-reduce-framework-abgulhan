#include <stdlib.h>
#include <stdio.h>
#include <string>
#include <vector>
#include <fstream>
#include <iostream>
#include <map>
#include <bits/stdc++.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <time.h>
#include <pthread.h>
#include <float.h>

#include "psu_dsm_system.h"
#include "psu_lock.h"

#define MAX_THREADS 10
#define SIZE_DB 410

#define NODES_FILE "nodes.txt"

#define MAPPER_ID 0

using namespace std;

struct coord{
	double x;
	double y;
};
typedef struct coord coord;

struct keyvalue{
	coord value;
	int key;
};
typedef struct keyvalue keyvalue;

int a __attribute__ ((aligned (4096)));
int b __attribute__ ((aligned (4096)));
int c __attribute__ ((aligned (4096)));
int start_map[MAX_THREADS] __attribute__ ((aligned (4096)));
int start_reduce[MAX_THREADS] __attribute__ ((aligned (4096)));
void * (*map_function_ptr) (void *) __attribute__ ((aligned (4096)));
void * (*reduce_function_ptr) (void *) __attribute__ ((aligned (4096)));
keyvalue input_data[SIZE_DB] __attribute__ ((aligned (4096)));;
keyvalue mapped_data[SIZE_DB] __attribute__ ((aligned (4096)));;
keyvalue reduced_data[SIZE_DB] __attribute__ ((aligned (4096)));;
int barrier_count __attribute__ ((aligned (4096)));
int upthreads_count __attribute__ ((aligned (4096)));
keyvalue * input_data_start __attribute__ ((aligned (4096)));
keyvalue * output_data_start __attribute__ ((aligned (4096)));
int num_data_points __attribute__ ((aligned (4096)));
int num_clusters __attribute__ ((aligned (4096)));
int d __attribute__ ((aligned (4096)));

string input_filename;
int id;

void init_globals_mr() {
	for (int i = 0; i < MAX_THREADS; i++) {
		start_map[i] = start_reduce[i] = 0;
	}
	num_data_points = 0;
}

int worker_node_id = -1;
int map_reduce_initialize_machine_id(string filename) {
	char * ip = mutex_get_ip();
	string ip_string = string(ip);
	vector<string> vec;
	mutex_get_file_contents(filename, vec);
	for (int i = 0; i < vec.size(); i++) {
		if (ip_string == vec[i]) {
			worker_node_id = i;
			return i;
		}
	}
}
int get_master_node_id(string filename){
	ifstream myFile;
	string line;
	int lines;

	myFile.open(filename);

	for(lines = 0; getline(myFile,line); lines++){
		if (line==""){
			lines--;
		}
	}
	cout << lines << endl;
	cout << lines-2 << endl;
	return lines-2;
}
//int num_threads = 0;

void psu_mr_setup() {
	init_globals_mr();
	//num_threads = nthreads;
}


void psu_mr_map(void* (*map_fp)(void*), void *inpdata, void *opdata) {
	// store map_fp
	map_function_ptr = map_fp;
	// store other values
	input_data_start = (keyvalue *) inpdata;
	output_data_start = (keyvalue *) opdata;
	
	barrier_count = 0;
	
	for (int i = 0; i < num_clusters; i++) {
		start_map[i] = 1;
	}
	
	while (barrier_count != num_clusters) ;
	
}

void * map_kmeans(void * ptr) {
	
	cout << "I have: num_data_points: " << num_data_points << ", and num_clusters: " << num_clusters << endl;
	
	int machine_id = id;//map_reduce_initialize_machine_id(NODES_FILE);
	int start_keyvalue_idx = num_clusters + machine_id * (num_data_points/num_clusters);
	int end_keyvalue_idx = num_clusters + (machine_id + 1) * (num_data_points/num_clusters);
	printf("I am mapping from %d to %d\n", start_keyvalue_idx, end_keyvalue_idx);
	
	for (int i = start_keyvalue_idx; i < end_keyvalue_idx; i++) {
		double distance = DBL_MAX;;
		double x1 = input_data_start[i].value.x;
		double y1 = input_data_start[i].value.y;
		int assigned_centroid = -1;
		for (int j = 0; j < num_clusters; j++) {
			double x2 = input_data_start[j].value.x;
			double y2 = input_data_start[j].value.y;
			double d = (x1-x2) * (x1-x2) + (y1-y2) * (y1-y2);
			if (d < distance) {
				assigned_centroid = j;
				distance = d;
			}
		}
		output_data_start[i].value.x = x1;
		output_data_start[i].value.y = y1;
		output_data_start[i].key = assigned_centroid;
	}
	
}


void psu_mr_reduce(void* (*reduce_fp)(void*), void *ipdata, void *opdata) {
	// store reduce_fp
	reduce_function_ptr = reduce_fp;
	// store other values
	input_data_start = (keyvalue *)ipdata;
	output_data_start = (keyvalue *)opdata;
	
	barrier_count = 0;
	
	for (int i = 0; i < num_clusters; i++) {
		start_reduce[i] = 1;
	}
	
	while (barrier_count != num_clusters) ;
	
}

void * reduce_kmeans (void * ptr) {
	
	int machine_id = worker_node_id;
	cout << "I am reducing for " << machine_id << "-th cluster" << endl;
	
	double x_total = 0.0;
	double y_total = 0.0;
	int num_points_in_cluster = 0;
	for (int i = 0; i < num_data_points; i++) {
		if (machine_id == input_data_start[i+num_clusters].key) {
			num_points_in_cluster++;
			x_total += input_data_start[i+num_clusters].value.x;
			y_total += input_data_start[i+num_clusters].value.y;
		}
	} 
	
	output_data_start[machine_id].value.x = x_total/num_points_in_cluster;
	output_data_start[machine_id].value.y = y_total/num_points_in_cluster;
	
}

void setup_input_data() {
	fstream file;
	string token;
	file.open(input_filename);
	
	file >> num_data_points;
	file >> num_clusters;
	
	for (int i = 0; i < num_data_points; i++) {
		file >> input_data[i+num_clusters].value.x;
		file >> input_data[i+num_clusters].value.y;
	}
	
	for (int i = 0; i < num_clusters; i++) {
		file >> input_data[i].value.x;
		file >> input_data[i].value.y;
	}
	
}

void mr_master() {
	printf("This is MR master node, upgraded!\n");
	//printf("Enter any number to start after starting other nodes\n");
	
	//int x;
	//scanf("%d", &x);
	psu_mr_setup();
	setup_input_data();
	b = 1;
	
	int tmp=1;
	while (upthreads_count != num_clusters) {
		if (tmp){
			printf("Waiting for other nodes...\n");
			tmp=0;
		}
	}
	
	printf("all other threads are up now!\n");
	
	
	
	setup_input_data();
	
	printf("Done setup!\n");
	/*
	for (int i = 0; i < num_clusters+num_data_points; i++) {
		cout << input_data[i].value.x << " " << input_data[i].value.y << ", assigned to: " << input_data[i].key << endl;
	}
	printf("\n");
	*/
	
	if (id < num_clusters) {
		cout << "not enough worker threads!" << endl;
		return;
	}
	
	psu_mr_map(map_kmeans, input_data, mapped_data);
	printf("Done mapping!\n");
	
	/*
	for (int i = 0; i < num_clusters+num_data_points; i++) {
		cout << mapped_data[i].value.x << " " << mapped_data[i].value.y << ", assigned to: " << mapped_data[i].key << endl;
	}
	printf("\n");
	*/
	
	psu_mr_reduce(reduce_kmeans, mapped_data, reduced_data);
	
	printf("Done reducing! Final results:\n");
	
	for (int i = 0; i < num_clusters; i++) {
		//cout << reduced_data[i].value.x << " " << reduced_data[i].value.y << endl;
		printf("%.2f %.2f\n", round(reduced_data[i].value.x*100)/100,round(reduced_data[i].value.y*100)/100);
	}
	printf("\n");
}


void mr_worker() {
	printf("This is NOT MR master node\n");
		
	while(b != 1) {
		;
	}
	
	psu_mutex_lock(0);
	upthreads_count++;
	psu_mutex_unlock(0);
	
	map_reduce_initialize_machine_id(NODES_FILE);
	printf("========My id is %d\n", worker_node_id);
	
	printf("Waiting for map signal...\n");
	while (start_map[worker_node_id] != 1) {
		;
	}
	printf("========Received signal from master to map!\n");
	
	map_function_ptr(NULL);
	//toy_wc_mapper(NULL);
	
	psu_mutex_lock(0);
	barrier_count++;
	psu_mutex_unlock(0);
	
	printf("Done mapping. Waiting to reduce now...\n");
	while(start_reduce[worker_node_id] != 1) {
		;
	}
	
	printf("========Received signal from master to reduce!\n");
	
	reduce_function_ptr(NULL);
	//toy_wc_reducer(NULL);
	
	psu_mutex_lock(0);
	barrier_count++;
	psu_mutex_unlock(0);
	
	printf("Done reducing!\n");
}


int main(int argc, char* argv[])
{
	if (argc > 1) {
		input_filename = string(argv[1]);
	} else {
		input_filename = string("input1.txt");
	}
	
	psu_init_lock(0);
	psu_init_lock(1);
	psu_dsm_register_datasegment(&a, 4096*22);
	id = map_reduce_initialize_machine_id(NODES_FILE);
	int master_id = get_master_node_id(NODES_FILE); //Master id is the second last line in the node file
	
	if (id == master_id) {
		mr_master();
	} else {
		mr_worker();
	}
	
	return 0;
}