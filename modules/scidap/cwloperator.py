#!/usr/bin/env python
#
#

import cwltool.main
import cwltool.workflow
import cwltool.errors

import schema_salad.ref_resolver
import logging
from airflow.models import BaseOperator
from airflow.utils import (apply_defaults)

import yaml
import os


def shortname(n):
    return n.split("#")[-1].split("/")[-1]


class CWLOperator(BaseOperator):
    template_ext = tuple()
    template_fields = ('cwl_job',)

    ui_color = '#3E53B7'
    ui_fgcolor = '#FFF'

    @apply_defaults
    def __init__(
            self,
            cwl_command,
            cwl_base=None,

            cwl_job=None,
            task_id=None,

            ui_color=None,
            op_args=None,
            op_kwargs=None,
            *args, **kwargs):

        self.cwl_base = os.path.abspath(cwl_base) if cwl_base else os.path.abspath('.')
        self.working_dir = os.path.abspath('.')

        _task_id = cwl_command.split("/")[-1].replace(".cwl", "").replace(".", "_dot_")
        self.cwl_command = os.path.join(self.cwl_base, cwl_command)
        if not (os.path.isfile(self.cwl_command) and os.access(cwl_job, os.R_OK)):
            raise cwltool.errors.WorkflowException("File does not exist: %s" % (self.cwl_command))
        if not cwl_job:
            raise cwltool.errors.WorkflowException("cwl_job is required when run operator alone")
        self.cwl_job = cwl_job

        if task_id:
            _task_id = task_id
        super(CWLOperator, self).__init__(task_id=_task_id, *args, **kwargs)

        # self.pre_execute_call = pre_execute or self.empty_f
        # self.post_execute_call = post_execute or self.empty_f
        self.op_args = op_args or []
        self.op_kwargs = op_kwargs or {}

        if ui_color:
            self.ui_color = ui_color

    def empty_f(self, *args, **kwargs):
        return None

    def execute(self, context):
        # logging.info("Options {0}:{1}".format(self.task_id, str(sys.argv)))

        self.op_kwargs = context
        cur_dir = os.getcwd()

        loader = schema_salad.ref_resolver.Loader({"id": "@id"})

        context.update(self.op_kwargs)
        context['cwl_job'] = self.cwl_job
        t = cwltool.main.load_tool(self.cwl_command, False, False, cwltool.workflow.defaultMakeTool, True)
        if type(t) == int or t.tool["class"] != "CommandLineTool":
            raise cwltool.errors.WorkflowException(
                "Class '%s' is not supported yet in CWLOperator" % (t.tool["class"]))
        job, _ = loader.resolve_all(yaml.load(self.cwl_job), 'file://%s' % self.cwl_base)
        if "scidap_working_folder" in job:
            self.working_dir = job["scidap_working_folder"]
            os.chdir(self.working_dir)
        output = cwltool.main.single_job_executor(t, job, self.working_dir, None,
                                                  outdir=self.working_dir)
        # logging.debug("Returned value {0}: {1}".format(self.task_id, str(json.dumps(output))))

        os.chdir(cur_dir)
