#!/usr/bin/env python

import cwltool.main
import cwltool.workflow
import cwltool.errors

import logging

from airflow.models import DAG
from airflow.utils import (apply_defaults)

# Temporary solution
# import sys
# sys.path.append('scidap/scidap/modules/scidap')
# from cwlstepoperator import CWLStepOperator
# from jobfolderoperator import JobFolderOperator
# from jobfileoperator import JobFileOperator

from scidap.cwlstepoperator import CWLStepOperator
from scidap.jobfolderoperator import JobFolderOperator
from scidap.jobfileoperator import JobFileOperator

from datetime import datetime, timedelta
import os


def shortname(n):
    return n.split("#")[-1].split("/")[-1]


class CWLDAG(DAG):

    @apply_defaults
    def __init__(
            self,
            cwl_workflow,
            cwl_base=None,
            cwl_job_folder=None,
            cwl_job=None,
            dag_id=None,
            schedule_interval=timedelta(minutes=1),
            start_date=None, end_date=None,
            full_filepath=None,
            template_searchpath=None,
            user_defined_macros=None,
            default_args=None,
            params=None,
            *args, **kwargs):

        _dag_id = dag_id if dag_id else cwl_workflow.split("/")[-1].replace(".cwl", "").replace(".", "_dot_")
        super(CWLDAG, self).__init__(dag_id=_dag_id, schedule_interval=schedule_interval, default_args=default_args, *args, **kwargs)

        self.cwlwf = cwltool.main.load_tool(os.path.join(cwl_base, cwl_workflow), False, False, cwltool.workflow.defaultMakeTool, True)

        if type(self.cwlwf) == int or self.cwlwf.tool["class"] != "Workflow":
            raise cwltool.errors.WorkflowException("Class '%s' is not supported yet in CWLDAG" % (self.cwlwf.tool["class"]))

        # logging.info("Options dag {0}: {1}".format(self.dag_id, str(sys.argv)))

        outputs = {}
        promises = {}

        if cwl_job and os.path.isfile(cwl_job) and os.access(cwl_job, os.R_OK):
            cwl_mop = JobFileOperator(task_id=self.dag_id+"_file", push_file=cwl_job, dag=self)
        elif cwl_job_folder and os.path.isdir(cwl_job_folder) and os.access(cwl_job_folder, os.O_RDWR):
            cwl_mop = JobFolderOperator(task_id=self.dag_id+"_folder", monitor_folder=cwl_job_folder, dag=self)
        else:
            raise cwltool.errors.WorkflowException("Either file or directory has to be specified")

        for inp in self.cwlwf.tool["inputs"]:
            promises[inp["id"]] = None

        first_run = True
        #
        # Fullfill all the Vertecies of the DAG
        #
        _tasks_added = []
        while len(self.cwlwf.steps) != len(_tasks_added):
            for step in self.cwlwf.steps:
                step_id = shortname(step.tool["id"])
                if step_id in _tasks_added:
                    continue

                stepinputs_fufilled = True
                for inp in step.tool["inputs"]:
                    if "source" in inp and inp["source"] not in promises and inp["source"] not in outputs:
                        stepinputs_fufilled = False
                        break

                if stepinputs_fufilled:
                    cwl_task = CWLStepOperator(
                        cwl_step=step,
                        dag=self)

                    _tasks_added.append(step_id)
                    for inp in step.tool["inputs"]:
                        if "default" in inp:
                            promises[inp["id"]] = None
                        if "source" in inp and inp["source"] in outputs:
                            cwl_task.set_upstream(outputs[inp["source"]])

                    if first_run and len(cwl_task.upstream_list) == 0:
                        cwl_task.set_upstream(cwl_mop)

                    for out in step.tool["outputs"]:
                        outputs[out["id"]] = cwl_task
            first_run = False
