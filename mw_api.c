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

/*** Worker Status Codes ***/
#define IDLE      0x00
#define BUSY      0x01

/***  Job Status Codes  ***/
#define NOT_DONE  0x00
#define PENDING   0x02
#define DONE      0x01

/***  Local data structures ***/
typedef struct job_data_t job_data_t;
struct job_data_t {
  mw_work_t * work_ptr;
  mw_result_t * result_ptr;
  unsigned long job_id;
  unsigned int job_status;
  job_data_t * next_job;
};

typedef struct job_queue_t job_queue_t;
struct job_queue_t {
  job_data_t * first;
  job_data_t * last;
  int count;
};

/*** Local Prototypes ***/
mw_work_t **get_next_job(mw_work_t **current_job, int count);
void KillWorkers(int n_proc);
//void SendWork(int dest, mw_work_t **first_job, int n_jobs, struct mw_api_spec *f);
void SendWork(int dest, mw_work_t *job, struct mw_api_spec *f);
void SendResults(int dest, mw_result_t **first_result, int n_results, struct mw_api_spec *f);

unsigned char queueEmpty(job_queue_t *queue) {
  return (queue->count == 0);
}

void InitQueue(job_queue_t * queue) {
  queue->first = NULL;
  queue->last = NULL;
  queue->count = 0;
}

void enqueue(job_queue_t *queuePtr, job_data_t *nodePtr) {
  job_data_t *old_last = queuePtr->last;
  // Scan to end of list if necessary
  if (nodePtr == NULL) return;
  if (queuePtr->first == NULL) {
    queuePtr->first = nodePtr;
  } else {
    old_last->next_job = nodePtr;
  }
  queuePtr->last = nodePtr;
  nodePtr->next_job = NULL;
  queuePtr->count++;
}

job_data_t * dequeue(job_queue_t *queuePtr) {
  if (queuePtr->first == NULL)return NULL;
  job_data_t *node = queuePtr->first;
  queuePtr->first = node->next_job;
  if (queuePtr->first == NULL) queuePtr->last = NULL;
  node->next_job = NULL;
  queuePtr->count--;
  return node;
}

void move_job(job_queue_t *in_queuePtr, job_queue_t *out_queuePtr) {
  job_data_t *node;
  node = dequeue(in_queuePtr);
  if (node == NULL) return;
  enqueue(out_queuePtr, node);
}

void assign_job(int dest, job_queue_t *workQPtr, job_queue_t *workerQPtr, struct mw_api_spec *f) {
  job_data_t *node;
  node = dequeue(workQPtr);
  if (node == NULL) return;
  SendWork(dest, node->work_ptr, f);
  enqueue(workerQPtr, node);
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
    job_ptr->job_status = NOT_DONE;
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
  printf("bytes read: %ld\n", bytes_read);
  return bytes_read;
}

job_data_t * RebuildJobQueue(struct mw_api_spec *f) {
  job_data_t *head, *job_ptr, *last_node = NULL;
  long int bytes_read, offset = 0;
  FILE * jf_stream;
  printf("rebuilding job queue...\n");
  jf_stream = fopen("test.abc", "rb");
  printf("opened file.\n");
  do {
    bytes_read = ReadJob(jf_stream, offset, &job_ptr, f);
    if (bytes_read != 0) {
      if (last_node == NULL) head = job_ptr;
      else last_node->next_job = job_ptr;
      job_ptr->result_ptr = NULL;
      job_ptr->job_status = NOT_DONE;
      job_ptr->next_job = NULL;
      last_node = job_ptr;
    }
    offset += bytes_read;
  } while(bytes_read != 0);
  fclose(jf_stream);
  return head;
}

void WriteAllJobs(job_data_t *job_list, struct mw_api_spec *f) {
  FILE * job_record;
  job_data_t *job_ptr = job_list;
  job_record = fopen("test.abc", "wb");
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
    job_queue_t PendingQueue, DoneQueue;
    job_queue_t WorkerQueues[n_proc-1];
    job_data_t *temp_job;
    mw_result_t **result_queue;

    InitQueue(&PendingQueue);
    InitQueue(&DoneQueue);
    for (i=0; i< (n_proc-1); i++) InitQueue(&WorkerQueues[i]);

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
      assign_job(i, &PendingQueue, &WorkerQueues[i-1], f);
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
      temp_job = dequeue(&WorkerQueues[source-1]);
      if (f->deserialize_results2(&(temp_job->result_ptr), receive_buffer, length) == 0) {
        fprintf(stderr, "Error deserializing results on process %d\n", rank);
        return;
      }
      free(receive_buffer);
      enqueue(&DoneQueue, temp_job);
      worker_status[source-1] = IDLE;
      if (queueEmpty(&PendingQueue)) {
        for (i=0; i<n_proc-1; i++) {
          if (worker_status[i]==BUSY) break;
        }
        if (i == (n_proc-1)) break;
      } else {
        assign_job(source, &PendingQueue, &WorkerQueues[i-1], f);
        worker_status[source-1] = BUSY;
      }
    }
    // Done; terminate worker threads and calculate result.
    KillWorkers(n_proc);

    // Hacky: Generate temp result queue for now.
    printf("completed jobs: %d\n", DoneQueue.count);
    if (NULL == (result_queue = (mw_result_t**) malloc(sizeof(mw_result_t*) * DoneQueue.count))) {
      fprintf(stderr, "malloc failed on process %d...", rank);
      return;
    };
    job_data_t * temp = DoneQueue.first;
    while (temp != NULL) {
      *result_queue = temp->result_ptr;
      result_queue++;
      temp = temp->next_job;
    }
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
    // free any dynamically allocated worker structures here.
  }
}
