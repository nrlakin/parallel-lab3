#ifndef _MW_COMMS_H_
#define _MW_COMMS_H_

#include <mpi.h>

#define PROBE_SUCCESS    0x00
#define PROBE_TIMEOUT    0x01

int TO_Probe(int ms_timeout, int source, int tag, MPI_Comm comm, MPI_Status *status);

#endif
