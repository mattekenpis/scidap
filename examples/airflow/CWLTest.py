#!/usr/bin/env python

from airflow.models import DAG
from airflow.utils import (apply_defaults)

from datetime import datetime, timedelta

# Temporary solution
# import sys
# sys.path.append('scidap/scidap/modules/scidap')
# from cwldag import CWLDAG

from scidap.cwldag import CWLDAG

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

CWL_BASE = "/Users/porter/Work/scidap/workflows/"

dag = CWLDAG(
 cwl_workflow="workflows/scidap/bam-genomecov-bigwig.cwl",
 cwl_job_folder=CWL_BASE+"workflows/scidap/job",
 cwl_base=CWL_BASE,
 default_args=default_args)
