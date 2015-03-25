#include <stdio.h>
#include <stdlib.h>
#include "mw_queue.h"

// Initialize a queue (linkedlist)
void init_queue(job_queue_t * queue) {
  queue->first = NULL;
  queue->last = NULL;
  queue->count = 0;
}

// Add a job_queue_t object to the provided queue (assumed to be initialized with init_queue)
// uses FIFO, so new nodes are added to the end of the linkedlist
void enqueue(job_queue_t *queuePtr, job_data_t *nodePtr) {
  job_data_t *old_last = queuePtr->last;
  // Scan to end of list if necessary
  if (nodePtr == NULL) {
    //printf("Attempted to queue NULL node.\n");
    return;
  }
  if (queuePtr->first == NULL) {
    queuePtr->first = nodePtr;
  } else {
    old_last->next_job = nodePtr;
  }
  queuePtr->last = nodePtr;
  nodePtr->next_job = NULL;
  queuePtr->count++;
}

// Returns the head job_queue_t node of the queue - first node of the linkedlist
job_data_t * dequeue(job_queue_t *queuePtr) {
  if (queuePtr->first == NULL) {
    //printf("Attempted dequeue of empty queue.\n");
    return NULL;
  }
  job_data_t *node = queuePtr->first;
  queuePtr->first = node->next_job;
  if (queuePtr->first == NULL) queuePtr->last = NULL;
  node->next_job = NULL;
  queuePtr->count--;
  return node;
}

// moves job node from one queue to the other (uses enqueue, dequeue)
void move_job(job_queue_t *in_queuePtr, job_queue_t *out_queuePtr) {
  job_data_t *node;
  node = dequeue(in_queuePtr);
  if (node == NULL) return;
  enqueue(out_queuePtr, node);
}
