#!/usr/bin/env python

import cwltool.main
import cwltool.workflow
import cwltool.errors

import schema_salad.ref_resolver
import logging

from airflow.models import BaseOperator, TaskInstance
from airflow.utils import apply_defaults, State
#from airflow import settings
import json

import os

_logger = logging.getLogger("cwloperator")
_logger.setLevel(logging.ERROR)

# def shortname(n):
#     return n.split("#")[-1].split("/")[-1].split(".")[-1]

supportedProcessRequirements = ["DockerRequirement",
                                "ExpressionEngineRequirement",
                                "SchemaDefRequirement",
                                "EnvVarRequirement",
                                "CreateFileRequirement",
                                "SubworkflowFeatureRequirement",
                                "ScatterFeatureRequirement"]

def checkRequirements(rec):
    if isinstance(rec, dict):
        if "requirements" in rec:
            for r in rec["requirements"]:
                if r["class"] not in supportedProcessRequirements:
                    raise Exception("Unsupported requirement %s" % r["class"])
        if "scatter" in rec:
            if isinstance(rec["scatter"], list) and rec["scatter"] > 1:
                raise Exception("Unsupported complex scatter type '%s'" % rec.get("scatterMethod"))
        for d in rec:
            checkRequirements(rec[d])
    if isinstance(rec, list):
        for d in rec:
            checkRequirements(d)

class CWLOperator(BaseOperator):

    template_fields = ('cwl_job',)
    template_ext = tuple()

    ui_color = '#3E53B7'
    ui_fgcolor = '#FFF'

    @apply_defaults
    def __init__(
            self,
            cwl_command,
            cwl_job,
            ui_color=None,
            working_dir=None,
            cwl_base=None,
            post_execute=None,
            pre_execute=None,
            provide_context=True,
            op_args=None,
            op_kwargs=None,
            *args, **kwargs):

        task_id = cwl_command.split("/")[-1].replace(".cwl","").replace(".", "_dot_")
        super(CWLOperator, self).__init__(task_id=task_id,*args, **kwargs)

        self.cwl_command = cwl_command
        self.cwl_job = cwl_job

        self.cwl_base = os.path.abspath(cwl_base) if cwl_base else os.path.abspath('.')
        self.working_dir = os.path.abspath(working_dir) if working_dir else os.path.abspath('.')

        self.pre_execute_call = pre_execute or self.empty_f()
        self.post_execute_call = post_execute or self.empty_f()
        self.op_args = op_args or []
        self.op_kwargs = op_kwargs or {}

        if ui_color:
            self.ui_color = ui_color

    def empty_f(self):
        return

    def execute(self, context):
        context.update(self.op_kwargs)
        context['cwl_job'] = self.cwl_job
        self.op_kwargs = context
        cur_dir = os.getcwd()
        os.chdir(self.working_dir)

        t = cwltool.main.load_tool(os.path.join(self.cwl_base, self.cwl_command), False, False, cwltool.workflow.defaultMakeTool, True)
        if type(t) == int or t.tool["class"] != "CommandLineTool":
            raise cwltool.errors.WorkflowException("Class '%s' is not supported yet in CWLOperator" % (t.tool["class"]))

        # self.pre_execute(*self.op_args, **self.op_kwargs)

        loader = schema_salad.ref_resolver.Loader({"id": "@id"})
        job, _ = loader.resolve_all(self.cwl_job,'file://%s' % self.cwl_base)

        output = cwltool.main.single_job_executor(t, job, self.working_dir, None, outdir=os.path.abspath('.'))
        # self.post_execute(*self.op_args, **self.op_kwargs)
        logging.info("Done. Returned value was: " + str(json.dumps(output)))
        os.chdir(cur_dir)
        return json.dumps(output)
