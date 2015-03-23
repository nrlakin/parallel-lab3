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

#define WORKER_QUEUE_LENGTH 1001
#define MASTER_QUEUE_LENGTH 10001
//#define JOBS_PER_PACKET 5

#define TAG_COMPUTE 0
#define TAG_KILL    1

/*** Worker Status Codes ***/
#define IDLE      0x00
#define BUSY      0x01

/*** Local Prototypes ***/
mw_work_t **get_next_job(mw_work_t **current_job, int count);
void KillWorkers(int n_proc);
//void SendWork(int dest, mw_work_t **first_job, int n_jobs, struct mw_api_spec *f);
void SendWork(int dest, mw_work_t *job, struct mw_api_spec *f);
void SendResults(int dest, mw_result_t **first_result, int n_results, struct mw_api_spec *f);
int F_Send(void *buf, int count, MPI_Datatype datatype, int dest, int tag, MPI_Comm comm);
unsigned char random_fail(void);

unsigned char queueEmpty(job_queue_t *queue) {
  return (queue->count == 0);
}

// BEGIN Hash Table Stuff
struct my_struct {
  int id;                    /* key */
  int count;
  UT_hash_handle hh;         /* makes this structure hashable */
};

struct my_struct *ids = NULL;

void add_id(int input_id, int count) {
  struct my_struct *s;

  HASH_FIND_INT(ids, &input_id, s);  /* id already in the hash? */
  if (s==NULL) {
    s = (struct my_struct*)malloc(sizeof(struct my_struct));
    s->id = input_id;
    HASH_ADD_INT( ids, id, s );  /* id: count of key field */
  }
  strcpy(s->count, count);
}

struct my_struct *find_id(int input_id) {
  struct my_struct *s;

  HASH_FIND_INT( ids, &input_id, s );  /* s: output pointer */
  return s;
}

void delete_id(struct my_struct *id) {
  HASH_DEL( ids, id);  /* id: pointer to delete */
  free(id);
}

void delete_all() {
  struct my_struct *current_id, *tmp;

  HASH_ITER(hh, ids, current_id, tmp) {
    HASH_DEL(ids,current_id);  /* delete it (ids advances to next) */
    free(current_id);            /* free it */
  }
}
// END Hash Table Stuff

int F_Send(void *buf, int count, MPI_Datatype datatype, int dest, int tag, MPI_Comm comm) {
   if (random_fail()) {      MPI_Finalize();
    exit (0);
    return 0;
   } else {
    return MPI_Send (buf, count, datatype, dest, tag, comm);
   }
}

unsigned char random_fail(void) {
  double dice_roll;
  int rank;
  MPI_Comm_rank (MPI_COMM_WORLD, &rank);
  if (rank == 1) return 0;
  dice_roll = (double)rand() / RAND_MAX;
  return (dice_roll > 0.9995);
}

void assign_job(int dest, job_queue_t *pendingQPtr, job_queue_t *workerQPtr, struct mw_api_spec *f) {
  job_data_t *node;
  node = dequeue(pendingQPtr);
  if (node == NULL) return;
  //printf("sending work...\n");
  SendWork(dest, node->work_ptr, f);
  //printf("Sent. Adding job to worker queue.\n");
  enqueue(workerQPtr, node);
  //printf("Enqueued.\n");
}

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
void KillWorkers(int n_proc) {
  int i, dummy;
  MPI_Status status;
  for(i=1; i<n_proc; i++) {
    MPI_Send(&dummy, 0, MPI_INT, i, TAG_KILL, MPI_COMM_WORLD);
  }
}

void WriteJob(FILE * jf_stream, job_data_t *job_ptr, struct mw_api_spec *f) {
  unsigned char * byte_stream;
  int length;
  if (jf_stream == NULL) {
    fprintf(stderr, "Error writing to job file.\n");
    exit(1);
  }
  printf("Serializing job_id %ld\n", job_ptr->job_id);
  fwrite(&(job_ptr->job_id), 1, sizeof(unsigned long), jf_stream);
  if (f->serialize_work2(job_ptr->work_ptr, &byte_stream, &length) == 0) {
    fprintf(stdout, "Error serializing work on master process.\n");
  }
  printf("serialized job\n");
  fwrite(&length, 1, sizeof(int), jf_stream);
  fwrite(byte_stream, 1, length, jf_stream);
  free(byte_stream);
  return;
}

long int ReadJob(FILE * jf_stream, long int offset, job_data_t **new_job, struct mw_api_spec *f) {
  long int bytes_read = 0;
  unsigned long temp_job_id;
  int work_size;
  unsigned char *byte_stream;
  fseek(jf_stream, offset, SEEK_SET);
  bytes_read += fread(&temp_job_id, 1, sizeof(unsigned long), jf_stream);
  if (bytes_read == 0)return 0;
  printf("mallocing job node.\n");
  if (NULL == (*new_job = (job_data_t*) malloc(sizeof(job_data_t)))) {
    fprintf(stderr, "malloc failed initializing job queue...\n");
    return 0;
  };
  (*new_job)->job_id = temp_job_id;
  bytes_read += fread(&work_size, 1, sizeof(int), jf_stream);
  printf("reading file, mallocing byte stream\n");
  printf("reading %d bytes.\n", work_size);
  if (NULL == (byte_stream = (unsigned char*) malloc(sizeof(unsigned char)*work_size))) {
    fprintf(stderr, "malloc failed allocating byte stream...\n");
    return 0;
  };
  bytes_read += fread(byte_stream, 1, sizeof(unsigned char) * work_size,jf_stream);
  f->deserialize_work2(&((*new_job)->work_ptr), byte_stream, work_size);
  free(byte_stream);
  (*new_job)->result_ptr = NULL;
  (*new_job)->next_job = NULL;
  printf("bytes read: %ld\n", bytes_read);
  return bytes_read;
}

void WriteResult(FILE * jf_stream, job_data_t *job_ptr, unsigned char *byte_stream, int length) {
  // FILE * jf_stream;
  // NEED TO OVERWRITE FIRST RUN
  // jf_stream = fopen("results.txt", "ab");

  if (jf_stream == NULL) {
    fprintf(stderr, "Error writing to job file.\n");
    exit(1);
  }
  fwrite(&(job_ptr->job_id), 1, sizeof(unsigned long), jf_stream);
  fwrite(&length, 1, sizeof(int), jf_stream);
  fwrite(byte_stream, 1, length, jf_stream);
  // fclose(jf_stream);
  return;
}

long int ReadResult(FILE * jf_stream, long int offset, job_data_t **new_job, struct mw_api_spec *f) {
  long int bytes_read = 0;
  unsigned long temp_job_id;
  int result_size;
  unsigned char *byte_stream;
  fseek(jf_stream, offset, SEEK_SET);
  bytes_read += fread(&temp_job_id, 1, sizeof(unsigned long), jf_stream);
  if (bytes_read == 0) return 0;
  printf("mallocing job node.\n");
  if (NULL == (*new_job = (job_data_t*) malloc(sizeof(job_data_t)))) {
    fprintf(stderr, "malloc failed initializing job queue...\n");
    return 0;
  };
  (*new_job)->job_id = temp_job_id;
  bytes_read += fread(&result_size, 1, sizeof(int), jf_stream);
  printf("reading file, mallocing byte stream\n");
  printf("reading %d bytes.\n", result_size);
  if (NULL == (byte_stream = (unsigned char*) malloc(sizeof(unsigned char)*result_size))) {
    fprintf(stderr, "malloc failed allocating byte stream...\n");
    return 0;
  };
  bytes_read += fread(byte_stream, 1, sizeof(unsigned char) * result_size, jf_stream);
  f->deserialize_results2(&((*new_job)->result_ptr), byte_stream, result_size);
  (*new_job)->work_ptr = NULL;
  (*new_job)->next_job = NULL;
  free(byte_stream);
  printf("bytes read: %ld\n", bytes_read);
  return bytes_read;
}

// job_data_t * RebuildJobQueue(struct mw_api_spec *f) {
//   job_data_t *head, *job_ptr, *last_node = NULL;
//   long int bytes_read, offset = 0;
//   FILE * jf_stream;
//   printf("rebuilding job queue...\n");
//   jf_stream = fopen("test.abc", "rb");
//   printf("opened file.\n");
//   do {
//     bytes_read = ReadJob(jf_stream, offset, &job_ptr, f);
//     if (bytes_read != 0) {
//       if (last_node == NULL) head = job_ptr;
//       else last_node->next_job = job_ptr;
//       job_ptr->result_ptr = NULL;
//       job_ptr->next_job = NULL;
//       last_node = job_ptr;
//     }
//     offset += bytes_read;
//   } while(bytes_read != 0);
//   fclose(jf_stream);
//   return head;
// }

void RebuildPendingResults(job_queue_t *pendingPtr, job_queue_t *resultsPtr, struct mw_api_spec *f) {
  long int bytes_read, offset = 0;
  FILE * results_jf_stream;
  printf("rebuilding done queue...\n");
  results_jf_stream = fopen("results.txt", "rb");
  printf("opened file.\n");
  job_data_t *new_result;
  do {
    bytes_read = ReadResult(results_jf_stream, offset, &new_result, f);
    enqueue(resultPtr, new_result);

    offset += bytes_read;
  } while(bytes_read != 0);
  fclose(results_jf_stream);
  return head;
}

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

void WriteAllJobs(job_data_t *job_list, struct mw_api_spec *f) {
  FILE * job_record;
  job_data_t *job_ptr = job_list;
  job_record = fopen("pool.txt", "wb");
  while (job_ptr != NULL) {
    WriteJob(job_record, job_ptr, f);
    job_ptr = job_ptr->next_job;
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
  if (f->serialize_work2(job, &send_buffer, &length) == 0) {
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
  //MPI_Send(send_buffer, length, MPI_UNSIGNED_CHAR, 0, TAG_COMPUTE, MPI_COMM_WORLD);
  F_Send(send_buffer, length, MPI_UNSIGNED_CHAR, 0, TAG_COMPUTE, MPI_COMM_WORLD);
  free(send_buffer);
}

void MW_Run(int argc, char **argv, struct mw_api_spec *f) {
  int rank, n_proc, i, length;
  unsigned char *receive_buffer;
  MPI_Status status;

  // Seed random # generator for failure testing
  //srand(time(NULL));


  // Get environment variables.
  MPI_Comm_size (MPI_COMM_WORLD, &n_proc);
  MPI_Comm_rank (MPI_COMM_WORLD, &rank);

  srand(rank*time(NULL));
  if (n_proc < 2) {
    fprintf(stderr, "Runtime Error: Master-Worker requires at least 2 processes.\n");
    return;
  }

  // Master program.
  if (rank == 0) {
    job_queue_t PendingQueue, DoneQueue;
    job_queue_t WorkerQueues[n_proc-1];
    job_queue_t *WorkerQPtrs[n_proc-1];
    job_data_t *temp_job;
    mw_result_t **result_queue;

    init_queue(&PendingQueue);
    init_queue(&DoneQueue);
    for (i=0; i< (n_proc-1); i++) init_queue(&WorkerQueues[i]);

    int source;
    double start, end;
    // start timer
    start = MPI_Wtime();

    // Init work queues
    mw_work_t **work_queue = f->create(argc, argv);
    //mw_work_t **next_job = work_queue;
    //mw_result_t *result_queue[MASTER_QUEUE_LENGTH];
    //mw_result_t **next_result = result_queue;
    //result_queue[0] = NULL;

    InitializeJobQueue(&PendingQueue, work_queue);

    // Worker status array.
    unsigned char worker_status[n_proc-1];

    // Initialize by giving tasks to all workers.
    for (i=1; i<n_proc; i++) {
      WorkerQPtrs[i-1] = &WorkerQueues[i-1];    //match ptrs
      assign_job(i, &PendingQueue, WorkerQPtrs[i-1], f);
      worker_status[i-1] = BUSY;
    }

    // Loop; give new jobs to workers as they return answers.
    while(1) {
      MPI_Probe(MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
      MPI_Get_count(&status, MPI_UNSIGNED_CHAR, &length);
      source = status.MPI_SOURCE;
      //printf("Got result from process %d of length %d\n", source, length);
      if (NULL == (receive_buffer = (unsigned char*) malloc(sizeof(unsigned char) * length))) {
        fprintf(stderr, "malloc failed on process %d...\n", rank);
        return;
      };
      MPI_Recv(receive_buffer, length, MPI_UNSIGNED_CHAR, source, MPI_ANY_TAG, MPI_COMM_WORLD,
            &status);
      temp_job = dequeue(WorkerQPtrs[source-1]);
      if (temp_job != NULL) {
        //printf("deserializing job %ld.\n", temp_job->job_id);
        if (f->deserialize_results2(&(temp_job->result_ptr), receive_buffer, length) == 0) {
          fprintf(stderr, "Error deserializing results on process %d\n", rank);
          return;
        }
      }
      free(receive_buffer);
      // Move job to DoneQueue.
      if (temp_job == NULL) {
        printf("null result from %d\n", source);
      }
      enqueue(&DoneQueue, temp_job);
      worker_status[source-1] = IDLE;

      if (queueEmpty(&PendingQueue)) {
        printf("No more pending work\n");
        for (i=0; i<n_proc-1; i++) {
          if (!queueEmpty(&WorkerQueues[i])) break;
          //if (worker_status[i]==BUSY) break;
        }
        if (i == (n_proc-1)) break;
        else {
          steal_jobs(i+1, source, &WorkerQueues[i], &WorkerQPtrs[source-1], f);
        }
      } else {
        //printf("Assigning next job.\n");
        assign_job(source, &PendingQueue, WorkerQPtrs[source-1], f);
        worker_status[source-1] = BUSY;
      }
    }
    // Done; terminate worker threads and calculate result.
    KillWorkers(n_proc);

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
    printf("Worker %d signing off.\n", rank);
    // free any dynamically allocated worker structures here.
  }
}
