/*******************************************************************************
*
* File: mw_factor.c
* Description: Large number factoring application for mw_api.
*
*******************************************************************************/

#include <stdio.h>
#include <mpi.h>
#include <stdlib.h>
#include <time.h>
#include <string.h>
#include "./mw_api.h"
#include <gmp.h>

// configuration variables:
// start_num = number we are factoring
// N_JOBS = number of jobs work is split up into (max is max_integer)
// JOBS_PER_PACKET = number of jobs sent in each mpi communication packet
// #define START_NUM "12345678912345678912"
#define START_NUM "12345678912345678912"
#define N_JOBS  10000
#define JOBS_PER_PACKET 900

// config variables for mpz_import and export
#define WORD_SIZE   1
#define ENDIAN      1
#define NAIL        0
#define ORDER       1

// function to print the resulting factors
void printFactors(unsigned long * vec, unsigned long len) {
  int i;
  for (i=0; i<len; i++) {
    printf("%lu\n", *vec++);
  }
}

// work structure
struct userdef_work_t {
  mpz_t target;
  unsigned long rangeStart;
  unsigned long rangeEnd;
};

// result structure
struct userdef_result_t {
  unsigned long length;
  unsigned long *factors;
};

// function returning the length of a mpz_number (see gmp docs)
int get_mpz_length(mpz_t bignum) {
  int numb = 8 * WORD_SIZE - NAIL;
  return (mpz_sizeinbase(bignum, 2) + numb-1)/numb;
}

// function to serialize an array of work pointers (**start_job) to a byte stream
// writes to **array and *len
int serialize_jobs(struct userdef_work_t **start_job, int n_jobs, unsigned char **array, int *len) {
  unsigned char *destPtr;
  unsigned char *temp_mpz;
  struct userdef_work_t **job = start_job;
  size_t mpz_size;
  // calculate total length of work pointed at by the work_queue
  int i, mpz_len, length = 0;
  for(i=0; i<n_jobs; i++) {
    if(*job == NULL) break;
    length += sizeof(int);    //for mpz size
    length += get_mpz_length((*job)->target);
    length += 2*sizeof(unsigned long);  //for start/end
    job++;
  }
  // allocate space for the byte array using the calculated length
  if (NULL == (*array = (unsigned char*)malloc(sizeof(unsigned char) * length))) {
    printf("malloc failed on allocating send buffer\n");
    return 0;
  }
  job = start_job;
  destPtr = *array;
  *len = length;
  // iterate through copying each work structure into the assigned buffer
  for(i=0; i<n_jobs; i++) {
    if(*job == NULL)break;
    mpz_len = get_mpz_length((*job)->target);
    memcpy(destPtr, &mpz_len, sizeof(int));
    destPtr += sizeof(int);
    // utilize the mpz_export function to export the gmp number
    mpz_export(destPtr, &mpz_size, ORDER, WORD_SIZE, ENDIAN, NAIL, (*job)->target);
    if (mpz_len != mpz_size) {
      printf("mpz error--different functions report different sizes.\n");
      return 0;
    }
    destPtr += mpz_len;
    memcpy(destPtr, &((*job)->rangeStart), sizeof(unsigned long));
    destPtr += sizeof(unsigned long);
    memcpy(destPtr, &((*job)->rangeEnd), sizeof(unsigned long));
    destPtr += sizeof(unsigned long);
    job++;
  }
  return 1;
}

// function to deserialize a byte stream (**queue) to an array of work
// writes to provided *array and len
int deserialize_jobs(struct userdef_work_t **queue, unsigned char *array, int len) {
  struct userdef_work_t *jobPtr;
  unsigned char *srcPtr = array;
  size_t mpz_size;
  int temp_size;
  while (NULL != *queue) queue++;
  while(len) {
    if (NULL == (jobPtr = (struct userdef_work_t*)malloc(sizeof(struct userdef_work_t)))) {
      printf ("malloc failed on receive buffer...\n");
      return 0;
    };
    memcpy(&temp_size, srcPtr, sizeof(int));
    mpz_size = temp_size;
    srcPtr += sizeof(int);
    mpz_init(jobPtr->target);
    mpz_import(jobPtr->target, mpz_size, ORDER, WORD_SIZE, ENDIAN, NAIL, srcPtr);
    // mpz_init_set_str(jobPtr->target, START_NUM, 10);
    srcPtr += temp_size;
    memcpy(&(jobPtr->rangeStart), srcPtr, sizeof(sizeof(unsigned long)));
    srcPtr += sizeof(unsigned long);
    memcpy(&(jobPtr->rangeEnd), srcPtr, sizeof(sizeof(unsigned long)));
    srcPtr += sizeof(unsigned long);
    *queue++ = jobPtr;
    len-= sizeof(int) + temp_size + 2*sizeof(unsigned long);
  }
  *queue = NULL;
  return 1;
}

// function to serialize result struct pointer array to a byte array
// writes to **array and *len
int serialize_results(struct userdef_result_t **start_result, int n_results, unsigned char **array, int *len) {
  int i, length=0;
  unsigned char *destPtr;
  long result_len;
  struct userdef_result_t **result = start_result;
  // loop through, allocate space for entire result message
  for(i = 0; i < n_results; i++) {
    if(*result == NULL) break;
    // result type is and array of unsigned longs
    length += (sizeof(unsigned long) * (*result)->length) + sizeof(unsigned long);
    result++;
  }
  if (NULL == (*array = (unsigned char*)malloc(sizeof(unsigned char) * length))) {
    printf ("malloc failed on send buffer...\n");
    return 0;
  };
  *len = length;
  result = start_result;
  destPtr = *array;
  // populate serialized array
  for(i = 0; i < n_results; i++) {
    if(*result == NULL) break;
    result_len = (*result)->length;
    destPtr = memcpy(destPtr, &result_len, sizeof(unsigned long));
    destPtr += sizeof(unsigned long);
    destPtr = memcpy(destPtr, (*result)->factors, sizeof(unsigned long) * (*result)->length);
    destPtr += sizeof(unsigned long) * (*result)->length;
    result++;
  }
  return 1;
}
// function to deserialize byte array into a result struct array to a
// writes to *array and len
int deserialize_results(struct userdef_result_t **queue, unsigned char *array, int len) {
  struct userdef_result_t *resultPtr;
  unsigned char *srcPtr = array;
  while (NULL != *queue) queue++;
  // populate results
  while(len) {
    if (NULL == (resultPtr = (struct userdef_result_t*)malloc(sizeof(struct userdef_result_t)))) {
      printf ("malloc failed on alloc result struct...\n");
      return 0;
    };
    // result in this case is an array of unsigned longs with an addition unsigned long for length
    memcpy(&(resultPtr->length), srcPtr, sizeof(unsigned long));
    srcPtr += sizeof(unsigned long);
    if (NULL == ((resultPtr->factors) = (unsigned long*)malloc(sizeof(unsigned long) * resultPtr->length))) {
      printf ("malloc failed on allocating factor of len %d\n...", resultPtr->length);
      return 0;
    };
    memcpy(resultPtr->factors, srcPtr, resultPtr->length * sizeof(unsigned long));
    srcPtr += resultPtr->length * sizeof(unsigned long);
    *queue++ = resultPtr;
    len-=(resultPtr->length * sizeof(unsigned long)) + sizeof(unsigned long);
  }
  *queue = NULL;
  return 1;
}

struct userdef_work_t **create_jobs(int argc, char **argv) {
  struct userdef_work_t **job_queue;
  struct userdef_work_t *jobs;

  if (NULL == (jobs = (struct userdef_work_t*)malloc(sizeof(struct userdef_work_t) * N_JOBS))) {
    printf("malloc failed on allocating jobs...");
    return NULL;
  }

  if (NULL == (job_queue = (struct userdef_work_t**)malloc(sizeof(struct userdef_work_t*) * N_JOBS + 1))) {
    printf("malloc failed on allocating job queue...");
    return NULL;
  }

  int i;
  unsigned long root_ul, chunk_size_ul;
  mpz_t n, root, chunk_size;

  // need to clear n
  mpz_init_set_str(n, START_NUM, 10);
  mpz_init(root);
  mpz_init(chunk_size);
  mpz_sqrt(root, n);
  mpz_tdiv_q_ui(chunk_size, root, N_JOBS);
  // at most root can be max_ul-1
  root_ul = mpz_get_ui(root);
  chunk_size_ul = mpz_get_ui(chunk_size);

  for (i = 0; i < N_JOBS; i++) {
    mpz_init_set(jobs[i].target, n);
    jobs[i].rangeStart = chunk_size_ul * i;
    if (i == N_JOBS-1) {
      jobs[i].rangeEnd = root_ul + 1;
    } else {
      jobs[i].rangeEnd = (chunk_size_ul * (i+1));
    }
    job_queue[i] = &(jobs[i]);
  }
  job_queue[i] = NULL;

  mpz_clear(root);
  mpz_clear(chunk_size);
  return job_queue;
}

// userdef result function for master
// compile and print the results returned from workers
int userdef_result(struct userdef_result_t **res) {
  struct userdef_result_t **ptr;
  ptr = res;
  int n_factors = 0;
  while(*ptr != NULL) {
    n_factors += (*ptr)->length;
    ptr++;
  }
  printf ("there are %d factors\n", n_factors);
  return 1;
}

// function that iterates through and calculates the total number of
// factors for the provided number, start, and end
// (not efficient but good for creating work)
unsigned long getFactorLength(mpz_t target, unsigned long start, unsigned long end) {
  unsigned long length = 0;
  mpz_t mod;
  unsigned long current;
  mpz_init(mod);
  for (current = start; current < end; current++) {
    if (!(current == 0) && !(current == 1)) {
      mpz_mod_ui(mod, target, current);
      if (mpz_cmp_ui(mod, 0) == 0) {
        length += 2;
      }
    }
  }
  mpz_clear(mod);
  return length;
}

// get factors compiles a n array of factors for
// given number, start, end, and length
unsigned long *getFactors(mpz_t target, unsigned long start, unsigned long end, unsigned long length) {
  int count = 0;
  unsigned long current;
  unsigned long *factors;
  mpz_t mod, factor;
  mpz_init(mod);
  mpz_init(factor);
  if (NULL == (factors = (unsigned long*)malloc(sizeof(unsigned long) * length))) {
    printf ("malloc failed on allocating factors array...");
    return NULL;
  };

  for (current = start; current < end; current++) {
    if (!(current == 0) && !(current == 1)) {
      mpz_mod_ui(mod, target, current);
      if (mpz_cmp_ui(mod, 0) == 0) {
        factors[count++] = current;
        mpz_tdiv_q_ui(factor, target, current);
        factors[count++] = mpz_get_ui(factor);
      }
    }
  }
  mpz_clear(mod);
  mpz_clear(factor);
  return factors;
}

// worker compute result function
struct userdef_result_t *userdef_compute(struct userdef_work_t *work) {
  struct userdef_result_t *result;
  // get the number of factors for the number, malloc an result for space
  unsigned long length = getFactorLength(work->target, work->rangeStart, work->rangeEnd);
  if (NULL == (result = (struct userdef_result_t*)malloc(sizeof(struct userdef_result_t)))) {
    printf("malloc failed on userdef_result...");
    return NULL;
  }
  // populate the assigned result
  result->length = length;
  result->factors = getFactors(work->target, work->rangeStart, work->rangeEnd, result->length);
  // need to clear the worker's work number (no worker cleanup function)
  mpz_clear(work->target);
  return result;
}

// userdef cleanup function to clean created work and results
int cleanup(struct userdef_work_t **work, struct userdef_result_t **res) {
  struct userdef_work_t **workPtr = work;

  while(*workPtr != NULL)mpz_clear((*workPtr++)->target);
  free(work[0]);
  free(work);

  struct userdef_result_t **res_ptr;
  res_ptr = res;
  while(*res != NULL) {
    free((*res)->factors);
    free(*res);
    res++;
  }
  return 1;
}

int main(int argc, char **argv) {
  struct mw_api_spec f;

  MPI_Init(&argc, &argv);

  f.create = create_jobs;
  f.result = userdef_result;
  f.compute = userdef_compute;
  f.serialize_work = serialize_jobs;
  f.deserialize_work = deserialize_jobs;
  f.serialize_results = serialize_results;
  f.deserialize_results = deserialize_results;
  f.cleanup = cleanup;
  f.jobs_per_packet = JOBS_PER_PACKET;

  MW_Run(argc, argv, &f);

  MPI_Finalize();
}
