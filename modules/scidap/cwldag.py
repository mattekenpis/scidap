#!/usr/bin/env python

import cwltool.main
import cwltool.workflow
import cwltool.errors

import schema_salad.ref_resolver
import logging

from airflow.models import BaseOperator, TaskInstance, DAG
from airflow.utils import apply_defaults, State
from airflow import settings

import sys
sys.path.append('/Users/porter/Work/scidap/scidap/modules/scidap')
from cwloperator import CWLOperator
from cwloperator import JobMOperator

#from scidap.cwloperator import CWLOperator

from datetime import datetime, timedelta

import json
from argparse import ArgumentParser
import os
import sys
import yaml
import copy

# CWL_BASE = "/Users/porter/Work/scidap/workflows/tools/"
WORKING_DIR = "/Users/porter/Work/scidap/workflows/tools/test-files"

def shortname(n):
    return n.split("#")[-1].split("/")[-1]


class CWLDAG(DAG):
    def __init__(
            self,
            cwl_workflow,
            cwl_job,
            dag_id=None,
            cwl_base=None,
            schedule_interval=timedelta(days=1),
            start_date=None, end_date=None,
            full_filepath=None,
            template_searchpath=None,
            user_defined_macros=None,
            default_args=None,
            params=None,
            *args, **kwargs):

        logging.info("Options dag:" + str(sys.argv[1:]))

        _dag_id = dag_id if dag_id else cwl_workflow.split("/")[-1].replace(".cwl", "").replace(".", "_dot_")
        super(CWLDAG, self).__init__(dag_id=_dag_id, default_args=default_args, *args, **kwargs)

        self.cwl_base = os.path.abspath(cwl_base) if cwl_base else os.path.abspath('.')
        self.cwl_workflow = cwl_workflow

        self.cwlwf = cwltool.main.load_tool(os.path.join(self.cwl_base, self.cwl_workflow), False, False, cwltool.workflow.defaultMakeTool, True)

        if type(self.cwlwf) == int or self.cwlwf.tool["class"] != "Workflow":
            raise cwltool.errors.WorkflowException("Class '%s' is not supported yet in CWLDAG" % (self.cwlwf.tool["class"]))

        self._tasks = {}
        self.outputs = {}
        self.promises = {}
        self.dag_summits=[]

        cwl_mop = None
        if os.path.isfile(cwl_job) and os.access(cwl_job, os.R_OK):
            self.loader = schema_salad.ref_resolver.Loader({"id": "@id"})
            self.job, _ = self.loader.resolve_ref(cwl_job, 'file://%s' % self.cwl_base)
            # for inp in self.cwlwf.tool["inputs"]:
            #     if shortname(inp["id"]) in self.job:
            #         pass
            #     elif shortname(inp["id"]) not in self.job and "default" in inp:
            #         self.job[shortname(inp["id"])] = copy.copy(inp["default"])
            #     elif shortname(inp["id"]) not in self.job and inp["type"][0] == "null":
            #         pass
            #     else:
            #         raise Exception("Missing inputs `%s`" % shortname(inp["id"]))
            # for inp in self.cwlwf.tool["inputs"]:
            #     self.promises[inp["id"]] = self.job[shortname(inp["id"])]
            # logging.info("Go by step 1"+str(promises))

        elif os.path.isdir(cwl_job) and os.access(cwl_job, os.R_OK):
            cwl_mop = JobMOperator(task_id=self.dag_id+"_monitor", monitor_dir=cwl_job, dag=self)
        else:
            raise cwltool.errors.WorkflowException("Error access '%s'" % (cwl_job))

        for inp in self.cwlwf.tool["inputs"]:
            self.promises[inp["id"]] = None
        logging.info("Go by step"+str(self.promises))

        first_run = True
        #
        # Fullfill all the Vertecies of the DAG
        #
        while len(self.cwlwf.steps) != len(self._tasks):
            for step in self.cwlwf.steps:
                step_id = shortname(step.tool["id"])
                if step_id in self._tasks:
                    continue

                stepinputs_fufilled = True
                for inp in step.tool["inputs"]:
                    if "source" in inp and inp["source"] not in self.promises and inp["source"] not in self.outputs:
                        stepinputs_fufilled = False
                        break

                if stepinputs_fufilled:
                    # jobobj = {}
                    # if not cwl_mop:
                    #     # for inp in step.tool["inputs"]:
                    #     #     jobobj_id = shortname(inp["id"]).split(".")[-1]
                    #     #     if "source" in inp:
                    #     #         if inp["source"] in promises:
                    #     #             jobobj[jobobj_id] = promises[inp["source"]]
                    #     #         elif inp["source"] in outputs:
                    #     #             pass
                    #     #     elif "default" in inp:
                    #     #         d = copy.copy(inp["default"])
                    #     #         jobobj[jobobj_id] = d
                    #     #         promises[inp["id"]] = d
                    #
                    #     cwl_task = CWLOperator(
                    #         cwl_command=step.embedded_tool,
                    #         task_id=step_id,
                    #         # working_dir=WORKING_DIR,
                    #         # cwl_job=str(json.dumps(jobobj)),
                    #         dag=self)
                    # else:
                    cwl_task = CWLOperator(
                        cwl_command=step.embedded_tool,
                        task_id=step_id,
                        dag=self)

                    self._tasks[step_id] = step
                    if first_run and cwl_mop:
                        cwl_task.set_upstream(cwl_mop)
                    for inp in step.tool["inputs"]:
                        if "default" in inp and cwl_mop:
                            self.promises[inp["id"]] = inp["default"]
                        if "source" in inp and inp["source"] in self.outputs:
                            cwl_task.set_upstream(self.outputs[inp["source"]])

                    for out in step.tool["outputs"]:
                        self.outputs[out["id"]] = cwl_task
            first_run = False


# if not isinstance(step.embedded_tool, cwltool.draft2tool.CommandLineTool):
#     raise cwltool.errors.WorkflowException("Class '%s' is not supported yet in CWLDAG" % (step.tool["class"]))
