/*******************************************************************************
*
* File: mw_api.c
* Description: Implementation for master-worker API.
*
*******************************************************************************/
#include <mpi.h>
#include <stdlib.h>
#include <stdio.h>
#include <time.h>
#include "mw_api.h"
#include "uthash.h"
#include "mw_queue.h"
#include "mw_comms.h"

/*** Controllable Timeout Variables ***/
#define WORKER_TIMEOUT  1000  // timeout for workers waiting to hear from a master
#define ARB_TIMEOUT     8000  // timeout for workers while waiting for arbitration to resolve
#define WAIT_TIMEOUT    15000 // timeout for workers waiting for new master to initialize
#define MASTER_TIMEOUT  5000  // timeout for master waiting to hear from morkers

/*** Controllable Probability for Success ***/
#define SUCCESS_PROB  0.99995

/*** Result Queue Max Size ***/
#define WORKER_QUEUE_LENGTH 1001
#define MASTER_QUEUE_LENGTH 10001

/*** Status Codes ***/
#define WORKER      0x00
#define ARB_SENT    0x01

struct process_status_t {
  int master;
  int timeout;
  int status;
} proc_status;

/*** Local Prototypes ***/
mw_work_t **get_next_job(mw_work_t **current_job, int count);
void KillWorkers(int n_proc, int rank);
void SendWork(int dest, mw_work_t *job, struct mw_api_spec *f);
//void SendResults(int dest, mw_result_t **first_result, int n_results, struct mw_api_spec *f);
void SendResults(int dest, mw_result_t *result, struct mw_api_spec *f);
int F_Send(void *buf, int count, MPI_Datatype datatype, int dest, int tag, MPI_Comm comm);
unsigned char random_fail(int tag);

// returns whether queue is empty
unsigned char queueEmpty(job_queue_t *queue) {
  return (queue->count == 0);
}

// Hash Table Implementation used: uthash
// Declare and initializea struct for the hash table
struct my_struct {
  unsigned long id;                    /* key */
  int count;
  UT_hash_handle hh;         /* makes this structure hashable */
};
struct my_struct *ids = NULL;

// Helper Functions for adding, finding, and emptying the hash table
void add_id(unsigned long input_id, int count) {
  struct my_struct *s;
  HASH_FIND(hh, ids, &input_id, sizeof(unsigned long), s);  /* id already in the hash? */
  if (s==NULL) {
    s = (struct my_struct*)malloc(sizeof(struct my_struct));
    s->id = input_id;
    HASH_ADD(hh, ids, id, sizeof(unsigned long), s);  /* id: count of key field */
  }
  s->count = count;
}
struct my_struct *find_id(unsigned long input_id) {
  struct my_struct *s;
  HASH_FIND(hh, ids, &input_id, sizeof(unsigned long), s);  /* s: output pointer */
  return s;
}
void delete_all() {
  struct my_struct *current_id, *tmp;
  HASH_ITER(hh, ids, current_id, tmp) {
    HASH_DEL(ids,current_id);  /* delete it (ids advances to next) */
    free(current_id);            /* free it */
  }
}

// Non-Blocking Send Function that uses random_fail to create failure cases
int F_Send(void *buf, int count, MPI_Datatype datatype, int dest, int tag, MPI_Comm comm) {
  MPI_Request request;
   if (random_fail(tag)) {
    printf("processor died\n");
    MPI_Finalize();
    exit (0);
    return 0;
   } else {
    return MPI_Isend (buf, count, datatype, dest, tag, comm, &request);
   }
}

// RNG function to determine whether failure happens
// has exceptions for: second master (based on given only one master failure assumption)
// and the arbitration process
unsigned char random_fail(int tag) {
  double dice_roll;
  int rank;
  MPI_Comm_rank (MPI_COMM_WORLD, &rank);
  if (rank != 0) {
    if (tag == TAG_COMPUTE) return 0;    // second master
    if (tag == TAG_KILL) return 0;
    if (tag == TAG_ARB) return 0;
  }
  dice_roll = (double)rand() / RAND_MAX;
  return (dice_roll > SUCCESS_PROB);
}

// Function move jobs from one work queue to another and send to the specified destination
// Dequeues a node from the pending work queue, sends it, and enqueues to the workers queue
// has an exception if no work is found in the pending queue
void assign_job(int dest, job_queue_t *pendingQPtr, job_queue_t *workerQPtr, struct mw_api_spec *f) {
  job_data_t *node;
  node = dequeue(pendingQPtr);
  if (node == NULL) return;
  SendWork(dest, node->work_ptr, f);
  enqueue(workerQPtr, node);
}

// Function for one worker to take over another worker's queue (includes the send)
int steal_jobs(int src_proc, int dest_proc, job_queue_t *srcQ, job_queue_t **destQptr, struct mw_api_spec *f) {
  job_queue_t *srcPtr = srcQ;
  job_queue_t *destPtr = *destQptr;
  if (!queueEmpty(destPtr)) {
    printf("Warning: busy worker %d is stealing work...\n", dest_proc);
  }
  if (queueEmpty(srcPtr)) {
    printf("Proc %d is stealing from proc %d, but src q is empty\n", dest_proc, src_proc);
  }
  // link queues
  *destQptr = srcQ;
  SendWork(dest_proc, srcPtr->first->work_ptr, f);
  printf("Assigned %d's queue to %d.\n", src_proc, dest_proc);
  return 0;
}

// Initialize ffunction for mallocing and creating the Job Queue
void InitializeJobQueue(job_queue_t * queuePtr, mw_work_t **work_queue) {
  mw_work_t **next_work = work_queue;
  job_data_t *job_ptr;
  unsigned long job_id = 0;
  while (*next_work != NULL) {
    if (NULL == (job_ptr = (job_data_t*) malloc(sizeof(job_data_t)))) {
      fprintf(stderr, "malloc failed initializing job queue...\n");
      return;
    };
    job_ptr->work_ptr = *next_work++;
    job_ptr->result_ptr = NULL;
    job_ptr->job_id = job_id++;
    job_ptr->next_job = NULL;
    enqueue(queuePtr, job_ptr);
  }
}

// Garbage collection for Job Queue Structure
void ClearJobQueue(job_data_t * job_queue) {
  job_data_t *next_job, *job_ptr = job_queue;
  while(job_ptr != NULL) {
    next_job = job_ptr->next_job;
    free(job_ptr);
    job_ptr = next_job;
  }
}

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
void KillWorkers(int n_proc, int rank) {
  int i, dummy;
  MPI_Status status;
  for(i=1; i<n_proc; i++) {
    if (i == rank) continue;
    MPI_Send(&dummy, 0, MPI_INT, i, TAG_KILL, MPI_COMM_WORLD);
  }
}

// Write job to file with the given file stream
// Writes each job to file in the following order: job_id, length of bytestream, serialized job
void WriteJob(FILE * jf_stream, job_data_t *job_ptr, struct mw_api_spec *f) {
  unsigned char * byte_stream;
  int length;
  if (jf_stream == NULL) {
    fprintf(stderr, "Error writing to job file.\n");
    exit(1);
  }
  // printf("Serializing job_id %ld\n", job_ptr->job_id);
  fwrite(&(job_ptr->job_id), 1, sizeof(unsigned long), jf_stream);
  // uses special serialize_work2 (userdefined serialize one work struct function)
  if (f->serialize_work(job_ptr->work_ptr, &byte_stream, &length) == 0) {
    fprintf(stdout, "Error serializing work on master process.\n");
  }
  // printf("serialized job\n");
  fwrite(&length, 1, sizeof(int), jf_stream);
  fwrite(byte_stream, 1, length, jf_stream);
  free(byte_stream);
  return;
}

// Read one job from file with the the given filestream
// mallocs space for: the job structure as well as the work
// Reads and deserialiezs the work associated with the job and attaches it to the job structure
// sets result_ptr and next_job to null
long int ReadJob(FILE * jf_stream, long int offset, job_data_t **new_job, struct mw_api_spec *f) {
  long int bytes_read = 0;
  unsigned long temp_job_id;
  int work_size;
  unsigned char *byte_stream;
  fseek(jf_stream, offset, SEEK_SET);
  bytes_read += fread(&temp_job_id, 1, sizeof(unsigned long), jf_stream);
  if (bytes_read == 0)return 0;
  // printf("mallocing job node.\n");
  if (NULL == (*new_job = (job_data_t*) malloc(sizeof(job_data_t)))) {
    fprintf(stderr, "malloc failed initializing job queue...\n");
    return 0;
  };
  (*new_job)->job_id = temp_job_id;
  bytes_read += fread(&work_size, 1, sizeof(int), jf_stream);
  // printf("reading file, mallocing byte stream\n");
  // printf("reading %d bytes.\n", work_size);
  if (NULL == (byte_stream = (unsigned char*) malloc(sizeof(unsigned char)*work_size))) {
    fprintf(stderr, "malloc failed allocating byte stream...\n");
    return 0;
  };
  bytes_read += fread(byte_stream, 1, sizeof(unsigned char) * work_size,jf_stream);
  f->deserialize_work(&((*new_job)->work_ptr), byte_stream, work_size);
  free(byte_stream);
  (*new_job)->result_ptr = NULL;
  (*new_job)->next_job = NULL;
  // printf("bytes read: %ld\n", bytes_read);
  return bytes_read;
}

// Write result to file with the given file stream
// Writes each result to file in the following order: job_id, length of bytestream, serialized result
void WriteResult(FILE * jf_stream, job_data_t *job_ptr, unsigned char *byte_stream, int length) {
  if (jf_stream == NULL) {
    fprintf(stderr, "Error writing to job file.\n");
    exit(1);
  }
  int test;
  // printf("%lu\n", job_ptr->job_id);
  fwrite(&(job_ptr->job_id), 1, sizeof(unsigned long), jf_stream);
  // printf("%d\n", length);
  fwrite(&length, 1, sizeof(int), jf_stream);
  // printf("%s\n", byte_stream);
  fwrite(byte_stream, 1, length, jf_stream);
  return;
}

// Read result from file with the given file stream
// Reads each result to file. Mallocs space for new job struct and result stream attaching it to result_ptr
// sets work_ptr and next_job to null
long int ReadResult(FILE * jf_stream, long int offset, job_data_t **new_job, struct mw_api_spec *f) {
  long int bytes_read = 0;
  unsigned long temp_job_id;
  int result_size;
  unsigned char *byte_stream;
  fseek(jf_stream, offset, SEEK_SET);
  bytes_read += fread(&temp_job_id, 1, sizeof(unsigned long), jf_stream);
  if (bytes_read == 0) return 0;
  // printf("mallocing job node.\n");
  if (NULL == (*new_job = (job_data_t*) malloc(sizeof(job_data_t)))) {
    fprintf(stderr, "malloc failed initializing job queue...\n");
    return 0;
  };
  (*new_job)->job_id = temp_job_id;
  bytes_read += fread(&result_size, 1, sizeof(int), jf_stream);
  // printf("reading file, mallocing byte stream\n");
  // printf("reading %d bytes.\n", result_size);
  if (NULL == (byte_stream = (unsigned char*) malloc(sizeof(unsigned char)*result_size))) {
    fprintf(stderr, "malloc failed allocating byte stream...\n");
    return 0;
  };
  bytes_read += fread(byte_stream, 1, sizeof(unsigned char) * result_size, jf_stream);
  f->deserialize_result(&((*new_job)->result_ptr), byte_stream, result_size);
  (*new_job)->work_ptr = NULL;
  (*new_job)->next_job = NULL;
  free(byte_stream);
  // printf("bytes read: %ld\n", bytes_read);
  return bytes_read;
}

// Function which opens results.txt and pool.txt and rebuilts pending and result queue
// uses uthash hash table to perform diff between completed and work pool to create pending
int RebuildQueues(job_queue_t *pendingPtr, job_queue_t *resultPtr, struct mw_api_spec *f) {
  long int bytes_read, offset = 0;
  FILE * jf_stream;
  // printf("rebuilding done queue...\n");
  jf_stream = fopen("results.txt", "rb");
  // printf("opened file.\n");
  job_data_t *new_job;
  do {
    bytes_read = ReadResult(jf_stream, offset, &new_job, f);
    if (bytes_read != 0) {
      // printf("adding to queue.\n");
      enqueue(resultPtr, new_job);
      // printf("adding to hash %lu\n", new_job->job_id);
      add_id(new_job->job_id, 1);
    }
    offset += bytes_read;
  } while(bytes_read != 0);
  fclose(jf_stream);

  // printf("rebuilding pending queue...\n");
  jf_stream = fopen("pool.txt", "rb");
  // printf("opened file.\n");
  offset = 0;
  bytes_read = 0;
  do {
    bytes_read = ReadJob(jf_stream, offset, &new_job, f);
    if (bytes_read != 0) {
      // printf("checking hash %lu\n", new_job->job_id);
      if (find_id(new_job->job_id) == NULL) {
        // printf("Not Found in Hash\n");
        enqueue(pendingPtr, new_job);
      } else {
        free(new_job);
      }
    }
    offset += bytes_read;
  } while(bytes_read != 0);
  fclose(jf_stream);
  delete_all();
  return 1;
}

// Iterates through initial job_queue and writes it to disk (pool.txt)
void WriteAllJobs(job_data_t *first_job, struct mw_api_spec *f) {
  FILE * job_record;
  job_data_t *temp = first_job;
  job_record = fopen("pool.txt", "wb");
  while (temp != NULL) {
    WriteJob(job_record, temp, f);
    temp = temp->next_job;
  }
  fclose(job_record);
  return;
}

// Send n_jobs to worker of rank 'dest'.
void SendWork(int dest, mw_work_t *job, struct mw_api_spec *f) {
  unsigned char *send_buffer;
  int length;
  //printf("in SendWork\n");
  //  if (f->serialize_work(first_job, n_jobs, &send_buffer, &length) == 0) {
  if (f->serialize_work(job, &send_buffer, &length) == 0) {
    fprintf(stderr, "Error serializing work on master process.\n");
  }
  // printf("done serializing %d\n", dest);
  F_Send(send_buffer, length, MPI_UNSIGNED_CHAR, dest, TAG_COMPUTE, MPI_COMM_WORLD);
  // printf("done with send %d\n", dest);
  free(send_buffer);
  // printf("done with free %d\n", dest);
}

// Send n_results to process of rank 'dest'.
void SendResults(int dest, mw_result_t *result, struct mw_api_spec *f) {
  unsigned char *send_buffer;
  int length;
  if (f->serialize_result(result, &send_buffer, &length) == 0) {
    fprintf(stderr, "Error serializing results.\n");
  }
  // MPI_Send(send_buffer, length, MPI_UNSIGNED_CHAR, proc_status.master, TAG_RESULT, MPI_COMM_WORLD);
  F_Send(send_buffer, length, MPI_UNSIGNED_CHAR, proc_status.master, TAG_RESULT, MPI_COMM_WORLD);
  free(send_buffer);
}

// Master state function
// Contains code to initialize, recover, and operate a master processor
void InitMaster(int n_proc, int rank, struct mw_api_spec *f, int argc, char **argv) {
  int i, length, timeout_flag, source;
  unsigned char *receive_buffer;
  MPI_Status status;

  // Initialize Queues
  job_queue_t PendingQueue, DoneQueue;
  job_queue_t WorkerQueues[n_proc-1];
  job_queue_t *WorkerQPtrs[n_proc-1];
  job_data_t *temp_job;
  mw_result_t **result_queue;
  init_queue(&PendingQueue);
  init_queue(&DoneQueue);
  for (i=0; i< (n_proc-1); i++) init_queue(&WorkerQueues[i]);
  mw_work_t **work_queue = NULL;

  // double start, end;
  // start timer
  // start = MPI_Wtime();

  // Initialize Work / Results Queues
  // rank = 0 corresponds to first master operation - creates work pool in this case
  // otherwise rebuilds the queues with the helper function RebuildQueues
  FILE * result_jf_stream;
  if (rank == 0) {
    printf("Initializing Work, Rank 0\n");
    work_queue = f->create(argc, argv);
    InitializeJobQueue(&PendingQueue, work_queue);
    WriteAllJobs(PendingQueue.first, f);
    // initialize a new result file if rank is 0
    result_jf_stream = fopen("results.txt", "wb");
    fclose(result_jf_stream);
  } else {
    printf("Rebuilding Work, Rank %d\n", rank);
    RebuildQueues(&PendingQueue, &DoneQueue, f);
  }

  // Initialize by giving tasks to all workers.
  for (i=1; i<n_proc; i++) {
    WorkerQPtrs[i-1] = &WorkerQueues[i-1];    //match ptrs
    if (i == rank) continue;                  // don't assign work to yourself
    assign_job(i, &PendingQueue, WorkerQPtrs[i-1], f);
  }

  // Loop; give new jobs to workers as they return answers.
  while(1) {
    timeout_flag = TO_Probe(MASTER_TIMEOUT, MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
    // check for worker no-response
    if (timeout_flag==PROBE_TIMEOUT) break;
    MPI_Get_count(&status, MPI_UNSIGNED_CHAR, &length);
    source = status.MPI_SOURCE;
    if (NULL == (receive_buffer = (unsigned char*) malloc(sizeof(unsigned char) * length))) {
      fprintf(stderr, "malloc failed on process %d...\n", rank);
      return;
    };
    MPI_Recv(receive_buffer, length, MPI_UNSIGNED_CHAR, source, MPI_ANY_TAG, MPI_COMM_WORLD,
          &status);
    if (status.MPI_TAG == TAG_RESULT) {
      // printf("Got result from process %d of length %d\n", source, length);
      // printf("Source %d, Current master %d\n", source, rank);

      // this if-else statement handles the case where a worker has stolen work from a dead worker
      if (!queueEmpty(WorkerQPtrs[source-1])) {
        temp_job = dequeue(WorkerQPtrs[source-1]);
        // write results as they come
        result_jf_stream = fopen("results.txt", "ab");
        WriteResult(result_jf_stream, temp_job, receive_buffer, length);
        fclose(result_jf_stream);

        if (temp_job != NULL) {
          if (f->deserialize_result(&(temp_job->result_ptr), receive_buffer, length) == 0) {
            fprintf(stderr, "Error deserializing results on process %d\n", rank);
            return;
          }
        }
      } else {
        temp_job = NULL;
      }

      free(receive_buffer);
      // Move job to DoneQueue.
      enqueue(&DoneQueue, temp_job);

      // after results have been processed - assign a new job to the returning worker
      // if pending queue is empty -  stealing pending jobs
      // else - give a job off of pending queue
      if (queueEmpty(&PendingQueue)) {
        // printf("No more pending work\n");
        for (i=0; i<n_proc-1; i++) {
          if (!queueEmpty(&WorkerQueues[i])) break;
        }
        if (i == (n_proc-1)) break;
        else {
          // printf("source: %d, current master rank %d \n", rank, source);
          steal_jobs(i+1, source, &WorkerQueues[i], &WorkerQPtrs[source-1], f);
        }
      } else {
        assign_job(source, &PendingQueue, WorkerQPtrs[source-1], f);
      }
    }
  }
  // Done; terminate worker threads and calculate result.
  KillWorkers(n_proc, rank);
  // Hacky: Generate temp result queue for now.
  printf("completed jobs: %d\n", DoneQueue.count);
  if (NULL == (result_queue = (mw_result_t**) malloc(sizeof(mw_result_t*) * (DoneQueue.count+1)))) {
    fprintf(stderr, "malloc failed on process %d...", rank);
    return;
  };
  job_data_t * temp = DoneQueue.first;
  mw_result_t ** resPtr = result_queue;
  while (temp != NULL) {
    *resPtr = temp->result_ptr;
    resPtr++;
    temp = temp->next_job;
  }
  *resPtr = NULL; //terminate
  // </hack>

  // calculate results
  printf("Calculating result.\n");
  if (f->result(result_queue) == 0) {
    fprintf(stderr, "Error in user-defined result calculation.\n");
    return;
  }

  // end timer
  // end = MPI_Wtime();
  // printf("%f seconds elapsed.\n", end-start);

  // Clean up master data structures.
  if (work_queue != NULL) {
    if (f->cleanup(work_queue, result_queue)) {
      fprintf(stderr, "Successfully cleaned up memory.\n");
    } else {
      fprintf(stderr, "Error in user-defined clean function.\n");
      return;
    }
  }
}

void MW_Run(int argc, char **argv, struct mw_api_spec *f) {
  int rank, n_proc, i, length, timeout_flag, source;
  unsigned char *receive_buffer;
  MPI_Status status;

  // Get environment variables.
  MPI_Comm_size (MPI_COMM_WORLD, &n_proc);
  MPI_Comm_rank (MPI_COMM_WORLD, &rank);

  srand(rank*time(NULL));
  if (n_proc < 2) {
    fprintf(stderr, "Runtime Error: Master-Worker requires at least 2 processes.\n");
    return;
  }

  if (rank == 0) {
    // Master program.
    InitMaster(n_proc, rank, f, argc, argv);
  } else {
    // Worker program.
    mw_work_t *work_queue[WORKER_QUEUE_LENGTH];
    mw_work_t **next_job = work_queue;
    mw_result_t *result_queue[WORKER_QUEUE_LENGTH];
    mw_result_t **next_result = result_queue;
    work_queue[0] = NULL;
    result_queue[0] = NULL;
    int count = 0;
    int lowest_rank = rank;
    // set default worker timeout, master, and status
    proc_status.timeout = WORKER_TIMEOUT;
    proc_status.master = 0;
    proc_status.status = WORKER;
    while(1) {
      // Wait for job - probe for master timeout
      timeout_flag = TO_Probe(proc_status.timeout, MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
      // State machine handling different worker states
      if (timeout_flag == PROBE_TIMEOUT) {
        // timeout has been detected
        if (proc_status.status == ARB_SENT) {
          // arbitration packet has already been sent
          // lowest rank takes over mater operation (and breaks when master is done)
          // otherwise wait for new master to give work
          if (rank == lowest_rank) {
            printf("Process %d is becoming master\n", rank);
            InitMaster(n_proc, rank, f, argc, argv);
            // printf("breaking out of master\n");
            break;
          } else {
            printf("Process %d is waiting for master\n", rank);
            proc_status.timeout = WAIT_TIMEOUT;
          }
        }
        // send your arbitration status to all other workers, set state, and set timeout to arbitration
        send_arb_all(n_proc, rank);
        proc_status.status = ARB_SENT;
        proc_status.timeout = ARB_TIMEOUT;
        //printf("Timeout. Process %d thinks %d is lowest\n", rank, lowest_rank);
        receive_buffer = NULL;
      } else {
        // timeout has not been detected
        MPI_Get_count(&status, MPI_UNSIGNED_CHAR, &length);
        source = status.MPI_SOURCE;
        if (status.MPI_TAG == TAG_KILL) break;    // handle kill tag
        if (status.MPI_TAG == TAG_ARB) {
          // if arbitration packet is received, send arbitration packet out and set status/timeout to arbitration
          if(proc_status.status == WORKER) {
            send_arb_all(n_proc, rank);
            //printf("Process %d sent arb packets.\n", rank);
          }
          proc_status.status = ARB_SENT;
          proc_status.timeout = ARB_TIMEOUT;
          // reset lowest_rank tracker
          if(source < lowest_rank)lowest_rank = source;
          //printf("Process %d thinks %d is lowest\n", rank, lowest_rank);
        }
        if (status.MPI_TAG == TAG_COMPUTE) {
          // new message has been received from master, reset status/master/timeout and continue as worker
          // printf("Process %d is falling back to worker\n", rank);
          proc_status.status = WORKER;
          proc_status.master = source;
          proc_status.timeout = WORKER_TIMEOUT;
          // printf("Got compute tag %d.\n", rank);
        }
        if (NULL == (receive_buffer = (unsigned char*) malloc(sizeof(unsigned char) * length))) {
          fprintf(stderr, "malloc failed on process %d...\n", rank);
          return;
        };
        MPI_Recv(receive_buffer, length, MPI_UNSIGNED_CHAR, source, status.MPI_TAG, MPI_COMM_WORLD,
          &status);
      }
      // still a worker - proceed with worker action
      if (proc_status.status == WORKER) {
        // Deserialize new jobs.
        if (f->deserialize_work(work_queue, receive_buffer, length) == 0) {
          fprintf(stderr, "Error deserializing work on process %d.\n", rank);
          return;
        }

        // Process new jobs.
        while(*next_job != NULL) {
          *next_result++ = f->compute(*next_job++);
          //count++;
        }
        *next_result=NULL;              // terminate new result queue.
        next_job = work_queue;
        while(*next_job != NULL) {      // free work structures on finishing calculation.
          free(*next_job++);
        }
        // Send results to master.
        SendResults(proc_status.master, *result_queue, f);
        next_result = result_queue;
        while(*next_result != NULL) {
          free(*next_result++);
        }
      }
      // Reset pointers
      if (receive_buffer) free(receive_buffer);
      next_result = result_queue;
      next_job = work_queue;
      *next_job = NULL;
      *next_result = NULL;
      count = 0;
    }
    printf("Worker %d signing off.\n", rank);
    // free any dynamically allocated worker structures here.
  }
}
