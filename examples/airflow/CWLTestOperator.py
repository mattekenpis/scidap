from airflow import DAG
# from scidap.cwloperator import CWLOperator
# from scidap.cwloperator import CWLDAG

from datetime import datetime, timedelta
import os
import yaml

import sys
sys.path.append('/Users/porter/Work/scidap/scidap/modules/scidap')

from cwloperator import CWLOperator

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

CWL_BASE = "/Users/porter/Work/scidap/workflows/tools/"
WORKING_DIR = "/Users/porter/Work/scidap/workflows/tools/test-files"

dags = {}

if os.path.isfile(WORKING_DIR+"/run_test") and os.access(WORKING_DIR+"/run_test", os.R_OK):
    os.remove(WORKING_DIR+"/run_test")

dag = DAG(dag_id='bam-to-bigwig-', default_args=default_args)

cwl_s1 = CWLOperator(
    cwl_command="bedtools-genomecov.cwl",
    cwl_base=CWL_BASE,
    working_dir=WORKING_DIR,
    cwl_job=yaml.load("""{
    "input": {
        "class": "File",
        "path": "rna.SRR948778.bam"
    },
    "genomeFile": {
        "class": "File",
        "path": "mm10-chrNameLength.txt"
    },
    "scale":1,
    "dept":"-bg",
    "genomecoverageout": "rna.SRR948778.bedGraph"
         }"""),
    dag=dag)

cwl_s2 = CWLOperator(
    cwl_command="linux-sort.cwl",
    cwl_base=CWL_BASE,
    working_dir=WORKING_DIR,
    cwl_job=yaml.load("""{
             "input": {
                 "class": "File",
                 "path": "rna.SRR948778.bedGraph"
             },
             "key": ["1,1","2,2n"],
         }"""),
    dag=dag)

cwl_s3 = CWLOperator(
    cwl_command="ucsc-bedGraphToBigWig.cwl",
    cwl_base=CWL_BASE,
    working_dir=WORKING_DIR,
    cwl_job=yaml.load("""{
    "input": {
        "class": "File",
        "path": "rna.SRR948778.bedGraph.sorted"
    },
    "genomeFile": {
        "class": "File",
        "path": "mm10-chrNameLength.txt"
    },
    "bigWig": "rna.SRR948778.{{ params.i }}.bigWig"
         }"""),
    params={'i': str(i)},
    dag=dag)

cwl_s1.set_downstream(cwl_s2)
cwl_s2.set_downstream(cwl_s3)
globals()[dag_id] = dag
