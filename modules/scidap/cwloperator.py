#!/usr/bin/env python
#
#

import cwltool.main
import cwltool.workflow
import cwltool.errors

import schema_salad.ref_resolver
import logging
from time import sleep
from datetime import datetime
from airflow.models import BaseOperator, TaskInstance
from airflow.utils import apply_defaults, State
from airflow.utils import (
    apply_defaults, AirflowException, AirflowSensorTimeout)

import yaml
import json
import sys
import os
import shutil

_logger = logging.getLogger("cwloperator")
_logger.setLevel(logging.ERROR)

class CWLOperator(BaseOperator):

    template_fields = ('cwl_job',)
    template_ext = tuple()

    ui_color = '#3E53B7'
    ui_fgcolor = '#FFF'

    @apply_defaults
    def __init__(
            self,
            cwl_command,
            cwl_job=None,
            task_id=None,

            working_dir=None,
            cwl_base=None,

            post_execute=None,
            pre_execute=None,
            provide_context=True,
            ui_color=None,
            op_args=None,
            op_kwargs=None,
            *args, **kwargs):

        if task_id:
            _task_id = task_id
        else:
            if not isinstance(cwl_command, cwltool.draft2tool.CommandLineTool):
                _task_id = cwl_command.split("/")[-1].replace(".cwl","").replace(".", "_dot_")
            else:
                _task_id = None
        super(CWLOperator, self).__init__(task_id=_task_id, *args, **kwargs)

        self.cwl_command = cwl_command
        self.cwl_job = cwl_job

        self.cwl_base = os.path.abspath(cwl_base) if cwl_base else os.path.abspath('.')
        self.working_dir = os.path.abspath(working_dir) if working_dir else os.path.abspath('.')

        self.pre_execute_call = pre_execute or self.empty_f
        self.post_execute_call = post_execute or self.empty_f
        self.op_args = op_args or []
        self.op_kwargs = op_kwargs or {}

        if ui_color:
            self.ui_color = ui_color

    def empty_f(self,*args, **kwargs):
        return None

    def execute(self, context):
        logging.info("Options op:" + str(sys.argv[1:]))

        context.update(self.op_kwargs)
        context['cwl_job'] = self.cwl_job
        self.op_kwargs = context
        cur_dir = os.getcwd()
        os.chdir(self.working_dir)

        if not isinstance(self.cwl_command, cwltool.draft2tool.CommandLineTool):
            t = cwltool.main.load_tool(os.path.join(self.cwl_base, self.cwl_command), False, False, cwltool.workflow.defaultMakeTool, True)
            if type(t) == int or t.tool["class"] != "CommandLineTool":
                raise cwltool.errors.WorkflowException("Class '%s' is not supported yet in CWLOperator" % (t.tool["class"]))
        else:
            t = self.cwl_command
        self.pre_execute_call(*self.op_args, **self.op_kwargs)

        loader = schema_salad.ref_resolver.Loader({"id": "@id"})
        job, _ = loader.resolve_all(yaml.load(self.cwl_job),'file://%s' % self.cwl_base)
        logging.info("Job file: " + str(self.cwl_job))

        output = cwltool.main.single_job_executor(t, job, self.working_dir, None, outdir=self.working_dir)
        self.post_execute_call(*self.op_args, **self.op_kwargs)
        logging.info("Done. Returned value was: " + str(json.dumps(output)))
        os.chdir(cur_dir)
        return json.dumps(output)

class JobMOperator(BaseOperator):

    #ui_color = '#3E53B7'
    # ui_fgcolor = '#FFF'

    @apply_defaults
    def __init__(
            self,
            monitor_dir,
            poke_interval=60,
            op_args=None,
            op_kwargs=None,
            *args, **kwargs):
        super(JobMOperator, self).__init__(*args, **kwargs)

        self.monitor_dir = monitor_dir
        self.op_args = op_args or []
        self.op_kwargs = op_kwargs or {}
        self.poke_interval = poke_interval
        self.timeout = 120

    def execute(self, context):
        logging.info("Options JobMOperator:" + str(sys.argv[1:]))
        started_at = datetime.now()
        while True:
            sleep(self.poke_interval)
            if (datetime.now() - started_at).seconds > self.timeout:
                raise AirflowSensorTimeout('Snap. Time is OUT.')
                logging.info(
                    'Looking for files in {self.monitor_dir} {self.task_id}'.format(**locals()))
                if os.path.isdir(self.monitor_dir) and os.access(self.monitor_dir, os.R_OK) \
                        and len(glob.glob(self.monitor_dir + '/*')) != 0:

                    oldest = max(glob.iglob(self.monitor_dir+'/*'), key=os.path.getctime)

                    with open(oldest, 'r') as content_file:
                        content = content_file.read()

                    self.xcom_push(key=task_id, value=content)
                    logging.info(
                        'Delete {oldest} {content}'.format(**locals()))

                    os.remove(oldest)

                        # shutil.move(oldest,TMP_DIR)
    # return pformat(kwargs)
    #                 return
