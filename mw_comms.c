
#include <stdlib.h>
#include <stdio.h>
#include <mpi.h>
#include "mw_comms.h"

#define TAG_ARB     2

int TO_Probe(int ms_timeout, int source, int tag, MPI_Comm comm, MPI_Status *status) {
  int flag = 0;
  double current, start = MPI_Wtime()*1000;
  while (!flag) {
    current = MPI_Wtime() * 1000;
    if((current - start) > ms_timeout) {
      return PROBE_TIMEOUT;
    }
    MPI_Iprobe(source, tag, comm, &flag, status);
  }
  return PROBE_SUCCESS;
}

void send_arb_all(int n_proc, int rank) {
  int dummy, i;
  for (i=1; i<n_proc && i != rank; i++) {
    MPI_Send(&dummy, 0, MPI_INT, i, TAG_ARB, MPI_COMM_WORLD);
  }
}
