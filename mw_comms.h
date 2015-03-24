#ifndef _MW_COMMS_H_
#define _MW_COMMS_H_

#include <mpi.h>

// Probe status
#define PROBE_SUCCESS    0x00
#define PROBE_TIMEOUT    0x01

// Tags
#define TAG_COMPUTE 0
#define TAG_KILL    1
#define TAG_ARB     2
#define TAG_RESULT  3

int TO_Probe(int ms_timeout, int source, int tag, MPI_Comm comm, MPI_Status *status);
void send_arb_all(int n_proc, int rank);

#endif
