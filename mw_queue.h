/*******************************************************************************
*
* File: mw_api.h
* Description: Header for master-worker API implementation.
*
*******************************************************************************/
#ifndef _MW_QUEUE_H_
#define _MW_QUEUE_H_

#include "mw_api.h"
//typedef struct userdef_work_t mw_work_t;
//typedef struct userdef_result_t mw_result_t;

typedef struct job_data_t job_data_t;
struct job_data_t {
  mw_work_t * work_ptr;
  mw_result_t * result_ptr;
  unsigned long job_id;
  job_data_t * next_job;
};

typedef struct job_queue_t job_queue_t;
struct job_queue_t {
  job_data_t * first;
  job_data_t * last;
  int count;
};

void init_queue(job_queue_t * queue);
void enqueue(job_queue_t *queuePtr, job_data_t *nodePtr);
job_data_t * dequeue(job_queue_t *queuePtr);
void move_job(job_queue_t *in_queuePtr, job_queue_t *out_queuePtr);

#endif
