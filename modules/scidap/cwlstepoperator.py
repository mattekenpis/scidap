#!/usr/bin/env python
#
#

import schema_salad.ref_resolver
import cwltool.main
import cwltool.workflow
import cwltool.errors
import logging
from airflow.models import BaseOperator
from airflow.utils import (apply_defaults)
from jsonmerge import merge
import json
import sys
import os
import copy
from cwlutils import shortname


class CWLStepOperator(BaseOperator):

    ui_color = '#3E53B7'
    ui_fgcolor = '#FFF'

    @apply_defaults
    def __init__(
            self,
            cwl_step,
            cwl_base=None,

            cwl_job=None,

            ui_color=None,
            op_args=None,
            op_kwargs=None,
            *args, **kwargs):

        self.cwl_base = os.path.abspath(cwl_base) if cwl_base else os.path.abspath('.')
        self.working_dir = os.path.abspath('.')

        self.cwl_step = cwl_step
        step_id = shortname(cwl_step.tool["id"])

        super(CWLStepOperator, self).__init__(task_id=step_id, *args, **kwargs)

        self.op_args = op_args or []
        self.op_kwargs = op_kwargs or {}

        if ui_color:
            self.ui_color = ui_color

    def execute(self, context):
        # logging.info("Options {0}:{1}".format(self.task_id, str(sys.argv)))

        cur_dir = os.getcwd()

        loader = schema_salad.ref_resolver.Loader({"id": "@id"})

        upstream_task_ids = [t.task_id for t in self.upstream_list]
        jobs = self.xcom_pull(context=context, task_ids=upstream_task_ids)

        promises = {}
        for j in jobs:
            promises = merge(promises, json.loads(j))

        if "working_folder" in promises:
            self.working_dir = promises["working_folder"]
            os.chdir(self.working_dir)
        else:
            raise cwltool.errors.WorkflowException("working_folder is required")

        jobobj = {}
        for inp in self.cwl_step.tool["inputs"]:
            inp_id = shortname(inp["id"])
            jobobj_id = inp_id.split(".")[-1]

            if "source" in inp:
                source_id = shortname(inp["source"])
                if source_id in promises:
                    jobobj[jobobj_id] = promises[source_id]
            elif "default" in inp:
                d = copy.copy(inp["default"])
                jobobj[jobobj_id] = d
                promises[inp_id] = d

        # logging.info("Promises {self.task_id}: {promises}".format(**locals()))
        # logging.info("Job {self.task_id}: {jobobj}".format(**locals()))

        job, _ = loader.resolve_all(jobobj, 'file://%s' % self.cwl_base)
        output = cwltool.main.single_job_executor(self.cwl_step.embedded_tool, job, self.working_dir, None,
                                                  outdir=self.working_dir)

        for out in self.cwl_step.tool["outputs"]:
            out_id = shortname(out["id"])
            jobout_id = out_id.split(".")[-1]
            promises[out_id] = output[jobout_id]

        os.chdir(cur_dir)
        return json.dumps(promises)
