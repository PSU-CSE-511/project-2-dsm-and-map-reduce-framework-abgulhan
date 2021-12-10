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

#include "psu_dsm_system.h"
#include "psu_lock.h"

#define MAX_THREADS 10
#define SIZE_DB 200

#define NODES_FILE "nodes.txt"

#define NUM_THREADS 2

using namespace std;

struct keyvalue{
	char word[16];
	int value;
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
int d __attribute__ ((aligned (4096)));

string word_count_filename;

void init_globals_mr() {
	for (int i = 0; i < MAX_THREADS; i++) {
		start_map[i] = start_reduce[i] = 0;
	}
	for (int i = 0; i < SIZE_DB; i++) {
		input_data[i].value = 0;
		mapped_data[i].value = 0;
		reduced_data[i].value = 0;
	}
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

int num_threads = 0;

void psu_mr_setup(unsigned int nthreads) {
	init_globals_mr();
	// initialize input_data from file TODO
	
	/*
	This is for toy example
	char words[2][16] = {"hello", "happy"};
	for(int i = 0; i < SIZE_DB; ++i)
	{
		int temp = rand()%NUM_THREADS;
		strcpy(input_data[i].word, words[temp]);
		input_data[i].value = 0;
	}
	*/
	
	num_threads = nthreads;
}


void psu_mr_map(void* (*map_fp)(void*), void *inpdata, void *opdata) {
	// store map_fp
	map_function_ptr = map_fp;
	// store other values
	input_data_start = (keyvalue *) inpdata;
	output_data_start = (keyvalue *) opdata;
	
	barrier_count = 0;
	
	for (int i = 0; i < NUM_THREADS; i++) {
		start_map[i] = 1;
	}
	
	while (barrier_count != NUM_THREADS) ;
	
}

void * toy_wc_mapper(void * ptr) {
	int machine_id = map_reduce_initialize_machine_id(NODES_FILE);
	int start_keyvalue_idx = machine_id * (SIZE_DB/NUM_THREADS);
	int end_keyvalue_idx = (machine_id + 1) * (SIZE_DB/NUM_THREADS);
	printf("I am mapping from %d to %d\n", start_keyvalue_idx, end_keyvalue_idx);
	for (int i = start_keyvalue_idx; i < end_keyvalue_idx; i++) {
		if (i == SIZE_DB) break;
		strcpy(output_data_start[i].word, input_data_start[i].word);
		if (strlen(output_data_start[i].word) > 0)
			output_data_start[i].value = 1;
	}
}


void psu_mr_reduce(void* (*reduce_fp)(void*), void *ipdata, void *opdata) {
	// store reduce_fp
	reduce_function_ptr = reduce_fp;
	// store other values
	input_data_start = (keyvalue *)ipdata;
	output_data_start = (keyvalue *)opdata;
	
	barrier_count = 0;
	
	for (int i = 0; i < NUM_THREADS; i++) {
		start_reduce[i] = 1;
	}
	
	while (barrier_count != NUM_THREADS) ;
	
	printf("Results of reducing:\n", NUM_THREADS);
	for (int i = 0; i < NUM_THREADS; i++) {
		printf("%s\n", reduced_data[i].word);
		printf("%d\n", reduced_data[i].value);
	}
	
}

void * better_wc_reducer (void * ptr) {
	map<string, int> map;
	int machine_id = map_reduce_initialize_machine_id(NODES_FILE);
	char start_lowercase_letter = 'a' + machine_id * (26/NUM_THREADS);
	char end_lowercase_letter = 'a' + (machine_id+1) * (26/NUM_THREADS);
	if (end_lowercase_letter > 'z') {
		end_lowercase_letter = 'z';
	}
	char start_uppercase_letter = start_lowercase_letter - ( 'a' - 'A' );
	char end_uppercase_letter = end_lowercase_letter - 'a' + 'A';
	
	for (int i = 0; i < SIZE_DB; i++) {
		char ch = input_data_start[i].word[0];
		if ( (ch >= start_lowercase_letter && ch <= end_lowercase_letter) || (ch >= start_uppercase_letter && ch <= end_uppercase_letter) ) {
			string key = string(input_data_start[i].word);
			map[key]++;
		}
	}
	
	int output_index_start = machine_id * (SIZE_DB/NUM_THREADS);
	int output_index_end = (1 + machine_id) * (SIZE_DB/NUM_THREADS);
	if (output_index_end > SIZE_DB) {
		output_index_end = SIZE_DB;
	}
	
	int i = 0;
	for (const auto &pair : map) {
		//cout << pair.first << " " << pair.second << endl;
		int write_index = output_index_start + i;
		if (write_index >= output_index_end) {
			write_index = output_index_end - 1;
		}
		i++;
		strcpy(output_data_start[write_index].word, pair.first.c_str());
		output_data_start[write_index].value = pair.second;
	}
	
}

void * toy_wc_reducer(void * ptr) {
	char words[2][16] = {"hello", "happy"};
	int machine_id = map_reduce_initialize_machine_id(NODES_FILE);
	int count = 0;
	char key[16];
	strcpy(key, words[machine_id]);
	printf("The key I am working with: %s\n", key);
	
	for (int i = 0; i < SIZE_DB; i++) {
		if (!strcmp(key, input_data_start[i].word)) {
			count++;
		}
	}
	strcpy(output_data_start[machine_id].word, key);
	output_data_start[machine_id].value = count;
}


void setup_input_data() {
	fstream file;
	string token;
	file.open(word_count_filename);
	int i = 0;
	while (file >> token) {
		strcpy(input_data[i++].word, token.c_str());
		i = i % SIZE_DB;
	}
}


void mr_master() {
	printf("This is MR master node\n");
	printf("Enter any number to start after starting other nodes\n");
	
	int x;
	scanf("%d", &x);
	b = 1;
	
	while (upthreads_count != NUM_THREADS) {
		;
	}
	
	printf("all other threads are up now!\n");
	
	psu_mr_setup(NUM_THREADS);
	setup_input_data();
	
	printf("Done setup! Set up data:\n");
	
	for (int i = 0; i < SIZE_DB; i++) {
		printf("%s %d ", input_data[i].word, input_data[i].value);
	}
	printf("\n");
	
	psu_mr_map(toy_wc_mapper, input_data, mapped_data);
	printf("Done mapping! After mapping:\n");
	
	for (int i = 0; i < SIZE_DB; i++) {
		printf("%s %d ", mapped_data[i].word, mapped_data[i].value);
	}
	printf("\n");
	
	psu_mr_reduce(better_wc_reducer, mapped_data, reduced_data);
	
	printf("Done reducing! Final results:\n");
	
	for (int i = 0; i < SIZE_DB; i++) {
		if (reduced_data[i].value == 0) {
			continue;
		}
		printf("%s %d\n", reduced_data[i].word, reduced_data[i].value);
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
		word_count_filename = string(argv[1]);
	} else {
		word_count_filename = string("input1");
	}
	
	psu_init_lock(0);
	psu_init_lock(1);
	psu_dsm_register_datasegment(&a, 4096*14);
	int id = map_reduce_initialize_machine_id(NODES_FILE);
	
	if (id == NUM_THREADS) {
		mr_master();
	} else {
		mr_worker();
	}
	
	return 0;
}
