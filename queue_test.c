#include <stdio.h>
#include <stdlib.h>
#include <mpi.h>
#include "mw_api.h"

typedef struct job_data_t job_data_t;

struct job_data_t {
  mw_work_t * work_ptr;
  mw_result_t * result_ptr;
  unsigned long job_id;
  unsigned int job_status;
  job_data_t * next_job;
};

void enqueue(job_data_t **queue, job_data_t *node) {
  job_data_t *nodePtr = *queue;
  // Scan to end of list if necessary
  if (node == NULL) return;
  if (*queue == NULL) {
    *queue = node;
  } else {
    while(nodePtr->next_job != NULL) nodePtr = nodePtr->next_job;
    nodePtr->next_job = node;
  }
  node->next_job = NULL;
}

job_data_t * dequeue(job_data_t **queue) {
  printf("as\n");
  if (*queue == NULL)return NULL;
  printf("df\n");
  job_data_t *node = *queue;
  *queue = node->next_job;
  node->next_job = NULL;
  return node;
}

void move_job(job_data_t *in_queue, job_data_t *out_queue) {
  job_data_t *node;
  node = dequeue(&in_queue);
  enqueue(&out_queue, node);
}

int main(int argc, char **argv) {
  job_data_t *queue = NULL;
  job_data_t *queue2 = NULL;

  job_data_t *item1 = NULL;
  if (NULL == (item1 = (job_data_t*) malloc(sizeof(job_data_t)))) {
    fprintf(stderr, "malloc failed initializing job queue...\n");
    return 1;
  };
  item1->job_id = 0;
  printf("allocated 1\n");

  job_data_t *item2 = NULL;
  if (NULL == (item2 = (job_data_t*) malloc(sizeof(job_data_t)))) {
    fprintf(stderr, "malloc failed initializing job queue...\n");
    return 1;
  };
  item2->job_id = 1;
  printf("allocated 2\n");

  job_data_t *item3 = NULL;
  if (NULL == (item3 = (job_data_t*) malloc(sizeof(job_data_t)))) {
    fprintf(stderr, "malloc failed initializing job queue...\n");
    return 1;
  };
  item3->job_id = 2;
  printf("allocated 3\n");

  // test enqueue/dequeue
  // job_data_t *thing = NULL;
  // enqueue(&queue, item1);
  // printf("queued 1\n");
  // enqueue(&queue, item2);
  // printf("queued 2\n");
  // thing = dequeue(&queue);
  // printf("first: %lu\n", thing->job_id);
  // enqueue(&queue, item3);
  // printf("queued 3\n");
  // thing = dequeue(&queue);
  // printf("second: %lu\n", thing->job_id);
  // thing = dequeue(&queue);
  // printf("third: %lu\n", thing->job_id);

  // testing move
  job_data_t *thing = NULL;
  enqueue(&queue, item1);
  enqueue(&queue, item2);
  enqueue(&queue, item3);
  move_job(queue, queue2);
  printf("moving 0\n");
  thing = dequeue(&queue2);
  printf("first: %lu\n", thing->job_id);
  move_job(queue, queue2);
  printf("moving 1\n");
  thing = dequeue(&queue2);
  printf("second: %lu\n", thing->job_id);
  move_job(queue, queue2);
  printf("moving 2\n");
  thing = dequeue(&queue2);
  printf("third: %lu\n", thing->job_id);

  free(item1);
  free(item2);
  free(item3);
  free(queue);
  free(queue2);
  return 0;
}
