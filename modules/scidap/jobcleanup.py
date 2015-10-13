import logging
from time import sleep
from airflow.models import BaseOperator, TaskInstance
from airflow.utils import apply_defaults, State
from airflow import settings
import sys
import os
import glob
import tempfile
import json
from jsonmerge import merge
import cwltool.errors
import shutil


class JobCleanup(BaseOperator):

    # ui_color = '#3E53B7'
    # ui_fgcolor = '#FFF'

    @apply_defaults
    def __init__(
            self,
            outputs,
            rm_files = None,
            op_args=None,
            op_kwargs=None,
            *args, **kwargs):
        super(JobCleanup, self).__init__(*args, **kwargs)

        self.op_args = op_args or []
        self.op_kwargs = op_kwargs or {}
        self.outputs = outputs
        self.outdir = None
        self.working_dir = None
        self.rm_files = rm_files or []

    def execute(self, context):
        # logging.info("Options {0}: {1}".format(self.task_id, str(sys.argv)))
        # logging.info(
        #     '{self.task_id}: Looking for files in {self.outputs}'.format(**locals()))

        # all_done = True
        # while all_done:
        #     all_done = False
        #     for t in self.upstream_list:
        #         ti = TaskInstance(
        #             t, execution_date=context['ti'].execution_date)
        #         if ti.state != State.SUCCESS:
        #             all_done = True
        #             break
        #     sleep(1)

        upstream_task_ids = [t.task_id for t in self.upstream_list]
        upstream_data = self.xcom_pull(context=context, task_ids=upstream_task_ids)

        promises = {}
        for j in upstream_data:
            data = j
            promises = merge(promises, data["promises"])
            if "outdir" in data:
                self.outdir = data["outdir"]

        if "working_folder" in promises:
            self.working_dir = promises["working_folder"]
        else:
            raise cwltool.errors.WorkflowException("working_folder is required")

        if not self.outdir:
            raise cwltool.errors.WorkflowException("Outdir is not provided, please use job dispatcher")

        logging.info(
            'Cleanup: {0}\n{1}\n{2}'.format(promises, self.outdir, self.outputs))

        for out in self.outputs:
            if out in promises and promises[out]["class"] == "File":
                shutil.move(promises[out]["path"], self.working_dir)

        for rmf in self.rm_files:
            if os.path.isfile(rmf):
                os.remove(rmf)

        shutil.rmtree(self.outdir, True)