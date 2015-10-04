#!/usr/bin/env python

from airflow.models import DAG
from airflow.utils import (apply_defaults)
from datetime import datetime, timedelta
from scidap.cwldag import CWLDAG
from scidap.jobfolderoperator import JobFolderOperator
# from scidap.jobfileoperator import JobFileOperator
# from scidap.cwlutils import shortname


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

JOB_FOLDER = "/absolute/path/to/the/directory/with/cwl/job/json/files/*"

dag = CWLDAG(
    cwl_workflow="workflows/scidap/bam-genomecov-bigwig.cwl",
    default_args=default_args)

# JobFileOperator(task_id=dag.dag_id + "_file", push_file="", dag=dag)

dag.assign_job_reader(JobFolderOperator(task_id=dag.dag_id + "_folder", monitor_folder=JOB_FOLDER, dag=dag))
