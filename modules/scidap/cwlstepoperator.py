#
# CWLStepOperator is required for CWLDAG
#   CWLStepOperator execute stage expects input job from xcom_pull
#   and post output by xcom_push

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
            ui_color=None,
            op_args=None,
            op_kwargs=None,
            *args, **kwargs):

        # self.cwl_base = os.path.abspath(cwl_base) if cwl_base else os.path.abspath('.')
        self.working_dir = None
        self.outdir = None

        self.cwl_step = cwl_step
        step_id = shortname(cwl_step.tool["id"])

        super(self.__class__, self).__init__(task_id=step_id, *args, **kwargs)

        self.op_args = op_args or []
        self.op_kwargs = op_kwargs or {}

        if ui_color:
            self.ui_color = ui_color

    def execute(self, context):
        # logging.info("Options {0}:{1}".format(self.task_id, str(sys.argv)))

        cur_dir = os.getcwd()

        loader = schema_salad.ref_resolver.Loader({"id": "@id"})

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

        if self.outdir:
            if not os.path.exists(self.outdir):
                os.makedirs(self.outdir)
            os.chdir(self.outdir)
        else:
            raise cwltool.errors.WorkflowException("Outdir is not provided, please use job dispatcher")


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

        logging.info("Job {self.task_id}: {jobobj}".format(**locals()))

        job, _ = loader.resolve_all(jobobj, 'file://%s' % os.path.abspath('.'))
        output = cwltool.main.single_job_executor(self.cwl_step.embedded_tool, job, self.working_dir, None,
                                                  outdir=self.outdir)

        for out in self.cwl_step.tool["outputs"]:
            out_id = shortname(out["id"])
            jobout_id = out_id.split(".")[-1]
            promises[out_id] = output[jobout_id]

        os.chdir(cur_dir)
        data = {}
        data["promises"] = promises
        data["outdir"] = self.outdir
        logging.info("Data: {0}".format(data))
        return data
