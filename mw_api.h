/*******************************************************************************
*
* File: mw_api.h
* Description: Header for master-worker API implementation.
*
*******************************************************************************/
#ifndef _MW_API_H_
#define _MW_API_H_

#include <limits.h>

struct userdef_work_t;
struct userdef_result_t;
typedef struct userdef_work_t mw_work_t;
typedef struct userdef_result_t mw_result_t;

#define MAX_JOBS_PER_PACKET 1000
#define MAX_JOBS_MASTER     10000

struct mw_api_spec {
  mw_work_t **(*create) (int argc, char **argv);
  int (*result) (mw_result_t **res);
  mw_result_t *(*compute) (mw_work_t *work);
  int (*cleanup) (mw_work_t **work, mw_result_t **results);
  int (*serialize_work) (mw_work_t **start_job, int n_jobs, unsigned char **array, int *len);
  int (*deserialize_work) (mw_work_t **queue, unsigned char *array, int len);
  int (*serialize_results) (mw_result_t **start_result, int n_results, unsigned char **array, int *len);
  int (*deserialize_results) (mw_result_t **queue, unsigned char *array, int len);
  int jobs_per_packet;
};

void MW_Run(int argc, char **argv, struct mw_api_spec *f);

#endif
