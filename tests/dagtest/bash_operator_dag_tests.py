"""
    End to end tests of simple DAG composed of basic bash_operators
"""

import os
import time
import unittest
from datetime import datetime

from airflow import jobs
from dag_tester import DagBackfillTest


class BashOperatorSingle_oneDay(unittest.TestCase, DagBackfillTest):

    def get_dag_id(self):
        return "bash_operator_single"

    def build_job(self, dag):
        return jobs.BackfillJob(
                dag=dag,
                start_date=datetime(2015, 1, 1),
                end_date=datetime(2015, 1, 1))

    def post_check(self, working_dir):
        with open("%s/out.2015-01-01.txt" % working_dir) as f:
            assert "success\n" == f.readline()


class BashOperatorSingle_3Days(unittest.TestCase, DagBackfillTest):

    dates = ["2015-01-01", "2015-01-02", "2015-01-03"]

    def get_dag_id(self):
        return "bash_operator_single"

    def build_job(self, dag):
        return jobs.BackfillJob(
                dag=dag,
                start_date=datetime(2015, 1, 1),
                end_date=datetime(2015, 1, 3))

    def post_check(self, working_dir):
        for date in self.dates:
            out_file = "%s/out.%s.txt" % (working_dir, date)
            with open(out_file) as f:
                assert "success\n" == f.readline(), \
                    "The file %s doesn't contain the success line" % out_file







