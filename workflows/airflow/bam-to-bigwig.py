#!/usr/bin/env python

from airflow.models import DAG
from airflow.configuration import conf
from datetime import datetime, timedelta
from scidap.cwldag import CWLDAG
from scidap.jobdispatcher import JobDispatcher

start_day = datetime.combine(datetime.today() - timedelta(1),
                             datetime.min.time())

default_args = {
    'owner': 'Andrey, Kartashov',
    'start_date': start_day,
    'email': ['porter@porter.st'],
    'email_on_failure': False,
    'email_on_retry': False,
    # 'schedule_interval': timedelta(minutes = 1),
    # 'end_date': datetime(2016, 1, 1),
}

dag = CWLDAG(
    cwl_workflow="workflows/scidap/bam-genomecov-bigwig.cwl",
    default_args=default_args)

dag.assign_job_dispatcher(JobDispatcher(task_id=dag.dag_id + "_monitor", monitor_folder=conf.get('scidap', 'BIGWIG_JOBS')
                                        , dag=dag))

# if branches > 1:
#     # make a top lvl branching with pass trough xpush
#     pass

