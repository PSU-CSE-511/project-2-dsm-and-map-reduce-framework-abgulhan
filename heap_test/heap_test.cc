#include <stdlib.h>
#include <stdio.h>
#include <string>
#include <vector>
#include <fstream>
#include <iostream>
#include <map>
#include "psu_dsm_system.h"
/*
type malloc to allocate a new variable in heap
type set to change value of an allocated variable
type print to print all variables
*/
template<typename K, typename V>
void print_map(std::map<K, V> const &m) {
    for (auto it = m.cbegin(); it != m.cend(); ++it) {
        cout << "{" << (*it).first << ": " << (*it).second << "}\n";
    }
}

int main(int argc, char* argv[]) {

	string x;
	string name;
	int size;
	string value;
	map<string, char*> name_to_var;
	
	cout << ">>> ";
	while(cin >> x) {
		if (x == "0") {
			cout << "exiting..." << endl;
			break;
		} else if (x == "malloc") {
			cout << "enter name: ";
			cin >> name;
			if (name == "")
				cout << "invalid name" << endl;
			else{
				cout << "enter size: ";
				cin >> size;
				if (size == 0){
					cout << "invalid size" << endl;
				}
				else{
					cout << "registering heap..." << endl;
					name_to_var[name] = psu_dsm_malloc(name.c_str(), size);
				}
			}
		} else if (x == "set") {
			cout << "enter variable name: ";
			cin >> name;
			if (name != "") {
				cout << "enter new value: ";
				cin >> value;
				strcpy(name_to_var[name], value.c_str());//*name_to_var[name] = size;
				cout << "new value of " << name << " = " << name_to_var[name] << endl;
			} else {
				cout << "invalid name" << endl;
			}
		}
		else if (x == "print") {
			print_map(name_to_var);
		}
		else if (x == "var") {
			cout << "enter name: ";
			cin >> value;
			cout << "enter index: ";
			cin >> size;
			cout << *(name_to_var[value]+size) << endl;
		}
		cout << ">>> ";
	}
	return 0;
}
