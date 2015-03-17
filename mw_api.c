/*******************************************************************************
*
* File: mw_api.c
* Description: Implementation for master-worker API.
*
*******************************************************************************/
#include <mpi.h>
#include <stdlib.h>
#include <stdio.h>
#include "mw_api.h"

#define WORKER_QUEUE_LENGTH 1001
#define MASTER_QUEUE_LENGTH 10001
//#define JOBS_PER_PACKET 5

#define TAG_COMPUTE 0
#define TAG_KILL    1

/*** Status Codes ***/
#define IDLE      0x00
#define BUSY      0x01

/*** Local Prototypes ***/
mw_work_t **get_next_job(mw_work_t **current_job, int count);
void KillWorkers(int n_proc);
void SendWork(int dest, mw_work_t **first_job, int n_jobs, struct mw_api_spec *f);
void SendResults(int dest, mw_result_t **first_result, int n_results, struct mw_api_spec *f);

// Helper function to pull next 'count' jobs off queue without running past end
// of buffer.
mw_work_t **get_next_job(mw_work_t **current_job, int count) {
  int i;
  for(i=0; i<count; i++) {
    if(*current_job==NULL)break;
    current_job++;
  }
  return current_job;
}

// Sends 'terminate' command to all worker threads.
void KillWorkers(int n_proc) {
  int i, dummy;
  MPI_Status status;
  for(i=1; i<n_proc; i++) {
    MPI_Send(&dummy, 0, MPI_INT, i, TAG_KILL, MPI_COMM_WORLD);
  }
}

// Send n_jobs to worker of rank 'dest'.
void SendWork(int dest, mw_work_t **first_job, int n_jobs, struct mw_api_spec *f) {
  unsigned char *send_buffer;
  int length;
  if (f->serialize_work(first_job, n_jobs, &send_buffer, &length) == 0) {
    fprintf(stderr, "Error serializing work on master process.\n");
  }
  MPI_Send(send_buffer, length, MPI_UNSIGNED_CHAR, dest, TAG_COMPUTE, MPI_COMM_WORLD);
  free(send_buffer);
}

// Send n_results to process of rank 'dest'.
void SendResults(int dest, mw_result_t **first_result, int n_results, struct mw_api_spec *f) {
  unsigned char *send_buffer;
  int length;
  if (f->serialize_results(first_result, n_results, &send_buffer, &length) == 0) {
    fprintf(stderr, "Error serializing results.\n");
  }
  MPI_Send(send_buffer, length, MPI_UNSIGNED_CHAR, 0, TAG_COMPUTE, MPI_COMM_WORLD);
  free(send_buffer);
}

void MW_Run(int argc, char **argv, struct mw_api_spec *f) {
  int rank, n_proc, i, length;
  unsigned char *receive_buffer;
  MPI_Status status;

  // Get environment variables.
  MPI_Comm_size (MPI_COMM_WORLD, &n_proc);
  MPI_Comm_rank (MPI_COMM_WORLD, &rank);

  if (n_proc < 2) {
    fprintf(stderr, "Runtime Error: Master-Worker requires at least 2 processes.\n");
    return;
  }

  // Master program.
  if (rank == 0) {
    int source;
    double start, end;
    // start timer
    start = MPI_Wtime();

    // Init work queues
    mw_work_t **work_queue = f->create(argc, argv);
    mw_work_t **next_job = work_queue;
    mw_result_t *result_queue[MASTER_QUEUE_LENGTH];
    mw_result_t **next_result = result_queue;
    result_queue[0] = NULL;

    // Worker status array.
    unsigned char worker_status[n_proc-1];
    // Initialize by giving tasks to all workers.
    for (i=1; i<n_proc; i++) {
      SendWork(i, next_job, f->jobs_per_packet, f);
      next_job = get_next_job(next_job, f->jobs_per_packet);
      worker_status[i-1] = BUSY;
    }

    // Loop; give new jobs to workers as they return answers.
    while(1) {
      MPI_Probe(MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
      MPI_Get_count(&status, MPI_UNSIGNED_CHAR, &length);
      source = status.MPI_SOURCE;
      if (NULL == (receive_buffer = (unsigned char*) malloc(sizeof(unsigned char) * length))) {
        fprintf(stderr, "malloc failed on process %d...\n", rank);
        return;
      };
      MPI_Recv(receive_buffer, length, MPI_UNSIGNED_CHAR, source, MPI_ANY_TAG, MPI_COMM_WORLD,
            &status);
      if (f->deserialize_results(result_queue, receive_buffer, length) == 0) {
        fprintf(stderr, "Error deserializing results on process %d\n", rank);
        return;
      }
      free(receive_buffer);
      worker_status[source-1] = IDLE;
      if (*next_job == NULL) {
        for (i=0; i<n_proc-1; i++) {
          if (worker_status[i]==BUSY) break;
        }
        if (i == (n_proc-1)) break;
      } else {
        SendWork(source, next_job, f->jobs_per_packet, f);
        next_job = get_next_job(next_job, f->jobs_per_packet);
        worker_status[source-1] = BUSY;
      }
    }
    // Done; terminate worker threads and calculate result.
    KillWorkers(n_proc);
    printf("Calculating result.\n");
    if (f->result(result_queue) == 0) {
      fprintf(stderr, "Error in user-defined result calculation.\n");
      return;
    }
    // end timer
    end = MPI_Wtime();
    printf("%f seconds elapsed.\n", end-start);

    // Clean up master data structures.
    if (f->cleanup(work_queue, result_queue)) {
      fprintf(stderr, "Successfully cleaned up memory.\n");
    } else {
      fprintf(stderr, "Error in user-defined clean function.\n");
      return;
    }
  } else {
    // Worker program.
    mw_work_t *work_queue[WORKER_QUEUE_LENGTH];
    mw_work_t **next_job = work_queue;
    mw_result_t *result_queue[WORKER_QUEUE_LENGTH];
    mw_result_t **next_result = result_queue;
    work_queue[0] = NULL;
    result_queue[0] = NULL;
    int count = 0;

    while(1) {
      // Wait for job.
      MPI_Probe(0, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
      MPI_Get_count(&status, MPI_UNSIGNED_CHAR, &length);
      if (status.MPI_TAG == TAG_KILL) break;
      if (NULL == (receive_buffer = (unsigned char*) malloc(sizeof(unsigned char) * length))) {
        fprintf(stderr, "malloc failed on process %d...", rank);
        return;
      };
      MPI_Recv(receive_buffer, length, MPI_UNSIGNED_CHAR, 0, TAG_COMPUTE, MPI_COMM_WORLD,
          &status);
      // Deserialize new jobs.
      if (f->deserialize_work(work_queue, receive_buffer, length) == 0) {
        fprintf(stderr, "Error deserializing work on process %d.\n", rank);
        return;
      }
      free(receive_buffer);
      // Process new jobs.
      while(*next_job != NULL) {
        *next_result++ = f->compute(*next_job++);
        count++;
      }
      *next_result=NULL;              // terminate new result queue.
      next_job = work_queue;
      while(*next_job != NULL) {      // free work structures on finishing calculation.
        free(*next_job++);
      }
      // Send results to master.
      SendResults(0, result_queue, count, f);
      next_result = result_queue;
      while(*next_result != NULL) {
        free(*next_result++);
      }
      // Reset pointers
      next_result = result_queue;
      next_job = work_queue;
      *next_job = NULL;
      *next_result = NULL;
      count = 0;
    }
    // free any dynamically allocated worker structures here.
  }
}
