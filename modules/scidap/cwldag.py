#!/usr/bin/env python

import cwltool.main
import cwltool.workflow
import cwltool.errors
import logging
from airflow.models import DAG
from airflow.utils import (apply_defaults)
from airflow.configuration import conf
from cwlstepoperator import CWLStepOperator
from cwlutils import shortname
from datetime import timedelta
import os
import sys

class CWLDAG(DAG):
    @apply_defaults
    def __init__(
            self,
            cwl_workflow,
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
        super(CWLDAG, self).__init__(dag_id=_dag_id, schedule_interval=schedule_interval, default_args=default_args,
                                     *args, **kwargs)

        if os.path.isabs(cwl_workflow):
            cwl_base = ""
        else:
            cwl_base = conf.get('cwl', 'CWL_HOME')

        self.cwlwf = cwltool.main.load_tool(os.path.join(cwl_base, cwl_workflow), False, False,
                                            cwltool.workflow.defaultMakeTool, True)

        if type(self.cwlwf) == int or self.cwlwf.tool["class"] != "Workflow":
            raise cwltool.errors.WorkflowException(
                "Class '{0}' is not supported yet in CWLDAG".format(self.cwlwf.tool["class"]))

        outputs = {}
        promises = {}

        for inp in self.cwlwf.tool["inputs"]:
            promises[inp["id"]] = None

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

                    for out in step.tool["outputs"]:
                        outputs[out["id"]] = cwl_task
        # logging.info("CWLDag {0} has been loaded {1}".format(self.dag_id, str(sys.argv)))

    @property
    def tops(self):
        return [t for t in self.tasks if not t.upstream_list]

    def assign_job_reader(self, task):
        for t in self.tops:
            if t == task:
                continue
            t.set_upstream(task)
