/*******************************************************************************
*
* File: mw_test.c
* Description: Test application for mw_api.
*
*******************************************************************************/

#include <stdio.h>
#include <mpi.h>
#include <stdlib.h>
#include <time.h>
#include <string.h>
#include "mw_api.h"

// Must be less than max int!
#define VECTOR_LENGTH 1000000000
#define N_JOBS  10000

/***
Given pointer to vector of doubles and vector length, calculate L2 norm
by taking dot product with itself.
***/
double norm(double* vec, int len) {
  int i;
  double result = 0;
  for (i=0; i<len; i++) {
    result += (*vec)*(*vec);
    vec++;
  }
  return result;
}

// Debug function to print short vectors to console.
void printVector(double * vec, int len) {
  int i;
  if(len > 10) return;
  //printf("Vector = %X\n", (unsigned int)vec);
  printf("Got:\n");
  for (i=0; i<len; i++) {
    printf("%d: %f\n", i, *vec++);
  }
}

// Work structure; pointer to array of doubles and int length.
struct userdef_work_t {
  int length;
  double *vector;
};

// Result structure; just a wrapper around a double.
struct userdef_result_t {
  double product;
};

// 'create' function.  Returns a pointer to an array of pointers
// to work structures.  Vector is a long array of random doubles between
// 0 and 1.
struct userdef_work_t **create_jobs (int argc, char **argv) {
  int i, chunk_size;
  const unsigned int VectorLength = VECTOR_LENGTH; // in case VECTOR_LENGTH is expression
  double *vector;
  struct userdef_work_t **job_queue;
  struct userdef_work_t *jobs;

  if (NULL == (vector = (double*)malloc(sizeof(double) * VectorLength))) {
    printf ("malloc failed on allocating vector...\n");
    return NULL;
  };
  if (NULL == (jobs = (struct userdef_work_t*)malloc(sizeof(struct userdef_work_t) * N_JOBS))) {
    printf ("malloc failed on allocating jobs...\n");
    return NULL;
  };
  if (NULL == (job_queue = (struct userdef_work_t**)malloc(sizeof(struct userdef_work_t*) * N_JOBS + 1))) {
    printf ("malloc failed on allocating job queue...\n");
    return NULL;
  };

  srand(time(NULL));
  for (i = 0; i < VectorLength; i++) {
    vector[i] = (double)rand() / RAND_MAX;
  }

  printf("Generated vector of length %d.\n", i);
  printVector(vector, i);

  chunk_size = VectorLength / N_JOBS;

  for (i = 0; i < N_JOBS; i++) {
    jobs[i].length = chunk_size;
    if (i == N_JOBS-1) {
      jobs[i].length += VectorLength % N_JOBS;
    }
    jobs[i].vector = &vector[i*chunk_size];
    job_queue[i] = &(jobs[i]);
  }
  job_queue[i] = NULL;

  return job_queue;
}

// Convert n_jobs job structures into a byte array for transmission to another
// process.  Because the job structure contains only a pointer to a subarray,
// this function is responsible for tracking down that pointer and copying its
// values into the byte array *array.  Should set **array to point to allocated
// send buffer, *len to point to number of bytes to send.  Returns 1 on success.
int serialize_jobs(struct userdef_work_t **start_job, int n_jobs, unsigned char **array, int *len) {
  int i, length=0;
  unsigned char *destPtr;
  struct userdef_work_t **job = start_job;
  // annoying--iterate through pending jobs to calculate size of required buffer.
  for(i = 0; i < n_jobs; i++) {
    if(*job == NULL)break;
    length += (sizeof(double) * (*job)->length) + sizeof(int);
    job++;
  }
  // allocate send buffer. this is freed by the api after packet is sent.
  if (NULL == (*array = (unsigned char*)malloc(sizeof(unsigned char) * length))) {
    printf ("malloc failed on send buffer...\n");
    return 0;
  };
  // copy jobs into buffer.  first 32-bit length, then array of doubles.
  *len = length;
  job = start_job;
  destPtr = *array;
  for(i = 0; i < n_jobs; i++) {
    if(*job == NULL) break;
    destPtr = memcpy(destPtr, &((*job)->length), sizeof(long));
    destPtr += sizeof(int);
    destPtr = memcpy(destPtr, (*job)->vector, sizeof(double) * (*job)->length);
    destPtr += sizeof(double) * (*job)->length;
    job++;
  }
  return 1;
}

// Convert received byte array *array back to userdef_work_t structures.  len
// gives size of received array in bytes; **queue points to next available spot
// in work queue (if not, it will scan to the end).  Responsible for allocating
// space for each new work structure, as well as for associated subvectors.
// Returns 1 on success, 0 on failure.
int deserialize_jobs(struct userdef_work_t **queue, unsigned char *array, int len) {
  struct userdef_work_t *jobPtr;
  unsigned char *srcPtr = array;
  while (NULL != *queue)queue++;
  while(len) {
    if (NULL == (jobPtr = (struct userdef_work_t*)malloc(sizeof(struct userdef_work_t)))) {
      printf ("malloc failed on alloc job struct...\n");
      *queue = NULL;
      return 0;
    };
    memcpy(&(jobPtr->length), srcPtr, sizeof(int));
    srcPtr += sizeof(int);
    if (NULL == ((jobPtr->vector) = (double*)malloc(sizeof(double) * jobPtr->length))) {
      printf ("malloc failed on allocating vector of len %d\n...", jobPtr->length);
      *queue = NULL;
      return 0;
    };
    memcpy(jobPtr->vector, srcPtr, jobPtr->length * sizeof(double));
    srcPtr += jobPtr->length * sizeof(double);
    *queue++ = jobPtr;
    len-=(jobPtr->length * sizeof(double)) + sizeof(int);
  }
  *queue = NULL;
  return 1;
}

// Convert n_jobs result structures into a byte array for transmission to another
// process.  Simpler than serialize_work because the userdef_result_t structure
// contains the actual result (not just a pointer).
int serialize_results(struct userdef_result_t **start_result, int n_results, unsigned char **array, int *len) {
  struct userdef_result_t **result = start_result;
  int i, length=0;
  unsigned char *destPtr;

  for(i = 0; i < n_results; i++) {
    if(*result == NULL)break;
    length += sizeof(double);
    result++;
  }
  if (NULL == (*array = (unsigned char*)malloc(sizeof(unsigned char) * length))) {
    printf ("malloc failed on send buffer...\n");
    return 0;
  };
  *len = length;
  result = start_result;
  destPtr = *array;
  for(i = 0; i < n_results; i++) {
    if(*result == NULL) break;
    memcpy(destPtr, &((*result)->product), sizeof(double));
    destPtr += sizeof(double);
    result++;
  }
  return 1;
}

// Convert received byte array *array back to userdef_t result structures.  len
// gives size of received array in bytes; **queue points to next available spot
// in result queue (if not, it will scan to the end).  Responsible for allocating
// space for each new result structure.  Returns 1 on success, 0 on failure.
int deserialize_results(struct userdef_result_t **queue, unsigned char *array, int len) {
  struct userdef_result_t *resultPtr;
  unsigned char *srcPtr = array;
  while (NULL != *queue)queue++;
  while(len) {
    if (NULL == (resultPtr = (struct userdef_result_t*)malloc(sizeof(struct userdef_result_t)))) {
      printf ("malloc failed on receive buffer...\n");
      return 0;
    };
    memcpy(&(resultPtr->product), srcPtr, sizeof(double));
    srcPtr += sizeof(double);
    *queue++ = resultPtr;
    len-=sizeof(double);
  }
  *queue = NULL;
  return 1;
}

// Sums sub-products into final dot product and displays on screen.  Returns 1
// on success.
int dot_product_result (struct userdef_result_t **res) {
  double dot_product = 0;
  int i;

  while(*res != NULL) {
    dot_product += (*res)->product;
    res++;
  }
  printf("Dot product: %f\n", dot_product);
  return 1;
}

// Computes dot product of a single vector/sub vector.  Called on worker thread.
// Returns result structure containing product; responsible for allocating
// structure.  Frees associated work vector before work structure is freed in
// API.
struct userdef_result_t *compute_dot (struct userdef_work_t *work) {
  struct userdef_result_t *result;
  int i;
  if (NULL == (result = (struct userdef_result_t*)malloc(sizeof(struct userdef_result_t)))) {
    printf ("malloc failed on compute_dot...");
    return NULL;
  };
  printVector(work->vector, work->length);
  result->product = norm(work->vector, work->length);
  free(work->vector);
  return result;
}

// Frees work and result structures on master thread.  Called after final
// result is calculated, immediately before exit.  Returns 1 on success.
int cleanup(struct userdef_work_t **work, struct userdef_result_t **results) {
  free(work[0]->vector);
  free(work[0]);
  free(work);
  while(*results != NULL) free(*results++);
  return 1;
}

int main(int argc, char **argv) {
  struct mw_api_spec f;
  struct userdef_work_t **master_queue;
  struct userdef_work_t *worker_queue[50];
  double *receive_buffer;
  int length, i;
  double *value;

  MPI_Init(&argc, &argv);

  // Configure API.
  f.create = create_jobs;
  f.result = dot_product_result;
  f.compute = compute_dot;
  f.serialize_work = serialize_jobs;
  f.deserialize_work = deserialize_jobs;
  f.serialize_results = serialize_results;
  f.deserialize_results = deserialize_results;
  f.cleanup = cleanup;
  f.jobs_per_packet = 5;

  // Run job.
  MW_Run(argc, argv, &f);

  MPI_Finalize();

  return 0;
}
