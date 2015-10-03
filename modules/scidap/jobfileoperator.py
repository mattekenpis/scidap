import logging
from airflow.models import BaseOperator
from airflow.utils import (apply_defaults)
import sys
import os


class JobFileOperator(BaseOperator):

    # ui_color = '#3E53B7'
    # ui_fgcolor = '#FFF'

    @apply_defaults
    def __init__(
            self,
            push_file,
            op_args=None,
            op_kwargs=None,
            *args, **kwargs):
        super(JobFileOperator, self).__init__(*args, **kwargs)

        self.push_file = push_file
        self.op_args = op_args or []
        self.op_kwargs = op_kwargs or {}

    def execute(self, context):
        # logging.info("Options {0}: {1}".format(self.task_id, str(sys.argv)))
        # logging.info(
        #     '{self.task_id} is looking for file {self.push_file} '.format(**locals()))
        if os.path.isfile(self.push_file) and os.access(self.push_file, os.R_OK):

            with open(self.push_file, 'r') as f:
                cwl_job_json = f.read()

            # logging.info(
            #     'Found: {0} \n With context: {1}'.format(self.push_file, cwl_job_json))

            return cwl_job_json
