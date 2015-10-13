#!/usr/bin/env python

from airflow.models import DAG
from airflow.configuration import conf
from datetime import datetime, timedelta
from scidap.cwldag import CWLDAG
from scidap.jobdispatcher import JobDispatcher
from scidap.jobcleanup import JobCleanup
import glob
import os
import shutil
import json
import sys

start_day = datetime.combine(datetime.today() - timedelta(1),
                             datetime.min.time())
end_day = datetime.combine(datetime.today(),
                           datetime.min.time())

monitor_folder = conf.get('scidap', 'BIGWIG_JOBS')

folder_new = os.path.join(monitor_folder, "new/")
monitor_folder_new = os.path.join(folder_new, "*")

folder_running = os.path.join(monitor_folder, "running/")
monitor_folder_running = os.path.join(folder_running, "*")

folder_fail = os.path.join(monitor_folder, "fail/")
monitor_folder_fail = os.path.join(folder_fail, "*")

max_dags_to_run = 10

default_args = {}


def fail_callback(context):
    uid = context["dag"].dag_id.split("_")[0]
    fail_file = glob.glob(os.path.join(folder_running, uid+"*"))
    if len(fail_file) != 1:
        raise Exception("Muts be one failed file:{0}".format(fail_file))
    shutil.move(fail_file[0], folder_fail)
    print("Fail uid {0} file: {1}".format(uid, fail_file[0]))


def make_dag(file):
    with open(file, 'r') as f:
        job = json.load(f)

    if "uid" not in job:
        raise Exception("UID must be part of the job")

    uid = job["uid"]

    owner = 'SciDAP'
    if "author" in job:
        owner = job['author']

    email = 'scidap@scidap.com'
    if "email" in job:
        email = job['email']

    dag_id = uid+"_bam2bigwig"
    default_args = {
        'owner': owner,
        'start_date': start_day,
        'email': [email],
        'email_on_failure': False,
        'email_on_retry': False,
        'end_date': end_day,
        'on_failure_callback': fail_callback
    }

    dag = CWLDAG(
        dag_id=dag_id,
        cwl_workflow="workflows/scidap/bam-genomecov-bigwig.cwl",
        default_args=default_args)
    dag.create()
    dag.assign_job_dispatcher(JobDispatcher(task_id="read", read_file=file, dag=dag))
    dag.assign_job_cleanup(JobCleanup(task_id="cleanup", outputs=dag.get_output_list(), rm_files=[file], dag=dag))
    globals()[dag_id] = dag
    return dag


if not os.path.exists(folder_running):
    os.mkdir(folder_running, 0777)
if not os.path.exists(folder_fail):
    os.mkdir(folder_fail, 0777)

tot_files_run = len(glob.glob(monitor_folder_running))
tot_files_new = len(glob.glob(monitor_folder_new))
# add new  jobs into running
if tot_files_run < max_dags_to_run and tot_files_new > 0:
    for i in range(min(max_dags_to_run - tot_files_run, tot_files_new)):
        oldest = max(glob.iglob(monitor_folder_new), key=os.path.getctime)
        shutil.move(oldest, folder_running)

tot_files_run = len(glob.glob(monitor_folder_running))
if tot_files_run > 0:
    for fn in glob.glob(monitor_folder_running):
        make_dag(fn)


tot_files_fail = len(glob.glob(monitor_folder_fail))
if tot_files_fail > 0:
    for fn in glob.glob(monitor_folder_fail):
        make_dag(fn)


