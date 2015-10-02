from __future__ import print_function

from datetime import datetime, timedelta

# Temporary solution
import sys
sys.path.append('/Users/porter/Work/scidap/scidap/modules/scidap')
from cwldag import CWLDAG

# from airflow import DAG
# from scidap.cwloperator import CWLDAG

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
WORKING_DIR = "/Users/porter/Work/scidap/workflows/tools/test-files"

dag = CWLDAG(
    cwl_workflow="workflows/scidap/bam-genomecov-bigwig.cwl",
    cwl_base=CWL_BASE,
    cwl_job=CWL_BASE+"workflows/scidap/job",
    default_args=default_args)
