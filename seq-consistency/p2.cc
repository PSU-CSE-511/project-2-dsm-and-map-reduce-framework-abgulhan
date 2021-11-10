//Program 2 of the sequential consistency check

#include <stdlib.h>
#include <stdio.h>
#include "psu_dsm_system.h"

int a __attribute__ ((aligned (4096)));
int b __attribute__ ((aligned (4096)));

int main(int argc, char* argv[])
{

	psu_dsm_register_datasegment(&a, 4096*2);
	while (a == 0);
	b = 1;

	return 0;
}