import logging
from time import sleep
from airflow.models import BaseOperator
from airflow.utils import (apply_defaults)
import sys
import os
from os.path import expanduser
import glob
import tempfile
import json


class JobDispatcher(BaseOperator):

    # ui_color = '#3E53B7'
    # ui_fgcolor = '#FFF'

    @apply_defaults
    def __init__(
            self,
            monitor_folder=None,
            read_file=None,
            branches=4,
            poke_interval=30,
            op_args=None,
            op_kwargs=None,
            *args, **kwargs):

        if (not read_file and not monitor_folder) or (read_file and monitor_folder):
            raise Exception("monitor_folder or read_file is required")

        super(JobDispatcher, self).__init__(*args, **kwargs)

        self.monitor_folder = monitor_folder
        self.read_file = read_file
        self.op_args = op_args or []
        self.op_kwargs = op_kwargs or {}
        self.poke_interval = poke_interval
        self.branches = branches

    def mktmp(self):
        if 'darwin' in sys.platform:
            home = expanduser("~")
            if not os.path.exists(os.path.abspath(home + "/cwl_tmp/")):
                os.mkdir(os.path.abspath(home + "/cwl_tmp/", 0777))
            outdir = tempfile.mkdtemp(prefix=os.path.abspath(home + "/cwl_tmp/c"))
        else:
            outdir = tempfile.mkdtemp()
        return outdir

    def execute(self, context):
        # logging.info("Options {0}: {1}".format(self.task_id, str(sys.argv)))
        cwl_context = {}

        if self.monitor_folder:
            logging.info(
                '{self.task_id}: Looking for files in {self.monitor_folder}'.format(**locals()))
            while True:
                tot_files = len(glob.glob(self.monitor_folder))
                if tot_files != 0:
                    oldest = max(glob.iglob(self.monitor_folder), key=os.path.getctime)

                    with open(oldest, 'r') as f:
                        cwl_context['promises'] = json.load(f)

                    cwl_context['outdir'] = self.mktmp()
                    cwl_context['branch'] = tot_files % self.branches

                    logging.info(
                        'Found: {0} \n With context: {1}'.format(oldest, json.dumps(cwl_context)))

                    os.remove(oldest)
                    return cwl_context
                else:
                    sleep(self.poke_interval)

        elif self.read_file:
            logging.info(
                '{self.task_id}: Looking for files in {self.read_file}'.format(**locals()))
            with open(self.read_file, 'r') as f:
                cwl_context['promises'] = json.load(f)

            cwl_context['outdir'] = self.mktmp()

            logging.info(
                'Found: {0} \n With context: {1}'.format(self.read_file, json.dumps(cwl_context)))

            return cwl_context
