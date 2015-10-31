#
# CWLDAG creates airfow DAG where each step is CWLStepOperator
#  to assign steps upfront of CWL
#

import cwltool.main
import cwltool.workflow
import cwltool.errors
import logging
from airflow.models import DAG
from airflow.utils import (apply_defaults)
from airflow.configuration import conf
from cwlstepoperator import CWLStepOperator
from cwlutils import shortname
from jobcleanup import JobCleanup

from datetime import timedelta
import os
import sys

__cwl__tools_loaded__ = {}


class CWLDAG(DAG):

    def __init__(
            self,
            cwl_workflow,
            dag_id=None,
            default_args=None,
            *args, **kwargs):

        _dag_id = dag_id if dag_id else cwl_workflow.split("/")[-1].replace(".cwl", "").replace(".", "_dot_")
        super(self.__class__, self).__init__(dag_id=_dag_id,
                                             default_args=default_args, *args, **kwargs)

        if cwl_workflow not in __cwl__tools_loaded__:
            if os.path.isabs(cwl_workflow):
                cwl_base = ""
            else:
                cwl_base = conf.get('cwl', 'CWL_HOME')

            __cwl__tools_loaded__[cwl_workflow] = cwltool.main.load_tool(os.path.join(cwl_base, cwl_workflow), False,
                                                                         False,
                                                                         cwltool.workflow.defaultMakeTool, True)

            if type(__cwl__tools_loaded__[cwl_workflow]) == int \
                    or __cwl__tools_loaded__[cwl_workflow].tool["class"] != "Workflow":
                raise cwltool.errors.WorkflowException(
                    "Class '{0}' is not supported yet in CWLDAG".format(
                        __cwl__tools_loaded__[cwl_workflow].tool["class"]))

        self.cwlwf = __cwl__tools_loaded__[cwl_workflow]

    def create(self):
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

    def assign_job_dispatcher(self, task):
        for t in self.tops:
            if t == task:
                continue
            t.set_upstream(task)

    def assign_job_cleanup(self, task):
        for t in self.roots:
            if t == task:
                continue
            task.set_upstream(t)

    def get_output_list(self):
        # return [shortname(o) for o in self.cwlwf.tool["outputs"] ]
        outputs = {}
        for out in self.cwlwf.tool["outputs"]:
            outputs[shortname(out["source"])] = shortname(out["id"])
        return outputs
