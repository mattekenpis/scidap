#!/usr/bin/env python

from airflow.models import DAG
from airflow.configuration import conf
from datetime import datetime, timedelta
from scidap.cwldag import CWLDAG
from scidap.jobfolderoperator import JobFolderOperator

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

dag.assign_job_reader(JobFolderOperator(task_id=dag.dag_id + "_folder", monitor_folder=conf.get('scidap', 'BIGWIG_JOB')
                                        , dag=dag))
