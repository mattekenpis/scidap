import logging
from time import sleep
from airflow.models import BaseOperator
from airflow.utils import (apply_defaults)
import sys
import os
import glob


class JobFolderOperator(BaseOperator):

    # ui_color = '#3E53B7'
    # ui_fgcolor = '#FFF'

    @apply_defaults
    def __init__(
            self,
            monitor_folder,
            poke_interval=30,
            op_args=None,
            op_kwargs=None,
            *args, **kwargs):
        super(JobFolderOperator, self).__init__(*args, **kwargs)

        self.monitor_folder = monitor_folder
        self.op_args = op_args or []
        self.op_kwargs = op_kwargs or {}
        self.poke_interval = poke_interval

    def execute(self, context):
        logging.info("Options {0}: {1}".format(self.task_id, str(sys.argv)))
        logging.info(
            'Looking for files in {self.monitor_folder} {self.task_id}'.format(**locals()))
        while True:
            sleep(self.poke_interval)
            if os.path.isdir(self.monitor_folder) and len(glob.glob(self.monitor_folder + '/*')) != 0:
                # and os.access(self.monitor_folder, os.O_RDWR)
                oldest = max(glob.iglob(self.monitor_folder+'/*'), key=os.path.getctime)

                with open(oldest, 'r') as f:
                    cwl_job_json = f.read()

                logging.info(
                    'Found: {0} \n With context: {1}'.format(oldest, cwl_job_json))

                os.remove(oldest)
                return cwl_job_json
