"""
    End to end tests of some simple DAGs composed of basic bash_operators
"""

import os
import time
import unittest
from datetime import datetime

from airflow import jobs
from .dag_tester import DagBackfillTest


class BashOperatorSingleOneDay(unittest.TestCase, DagBackfillTest):

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


class BashOperatorSingle3Days(unittest.TestCase, DagBackfillTest):

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


class BashOperatorABDownStream(unittest.TestCase, DagBackfillTest):

    dates = ["2015-01-01", "2015-01-02", "2015-01-03", "2015-01-04",
             "2015-01-05", "2015-01-06", "2015-01-07", "2015-01-08",
             "2015-01-09", "2015-01-10"]

    file_a = "%s/out.a.%s.txt"
    file_b = "%s/out.b.%s.txt"

    def get_dag_id(self):
        return "bash_operator_ab"

    def get_test_context(self):
        return {"dep_direction": "downstream"}

    def build_job(self, dag):
        return jobs.BackfillJob(
                dag=dag,
                start_date=datetime(2015, 1, 1),
                end_date=datetime(2015, 1, 10))

    def post_check(self, working_dir):
        for date in self.dates:

            file_a_date = self.file_a % (working_dir, date)
            file_b_date = self.file_b % (working_dir, date)

            with open(file_a_date) as f:
                assert "success_a\n" == f.readline(), \
                    "The file %s doesn't contain the success line" % file_a_date

            with open(file_b_date) as f:
                assert "success_b\n" == f.readline(), \
                    "The file %s doesn't contain the success line" % file_b_date

            time_a = time.ctime(os.path.getmtime(file_a_date))
            time_b = time.ctime(os.path.getmtime(file_b_date))
            assert time_a < time_b, \
                "Task a was not executed before Task b for date %s" % date


class BashOperatorABUpstream(BashOperatorABDownStream, DagBackfillTest):

    def get_dag_id(self):
        return "bash_operator_ab"

    def get_test_context(self):
        return {"dep_direction": "upstream"}


class BashOperatorABRetries(BashOperatorABDownStream, DagBackfillTest):

    def get_dag_id(self):
        return "bash_operator_ab_retries"

    def get_test_context(self):
        return {"depends_on_past": False,
                "wait_for_downstream": False}


class BashOperatorABDependsOnPast(BashOperatorABDownStream, DagBackfillTest):

    dates = ["2015-01-01", "2015-01-02", "2015-01-03", "2015-01-04",
             "2015-01-05", "2015-01-06", "2015-01-07", "2015-01-08",
             "2015-01-09", "2015-01-10"]

    file_a = "%s/out.a.%s.txt"
    file_b = "%s/out.b.%s.txt"

    def get_dag_id(self):
        return "bash_operator_ab_retries"

    def get_test_context(self):
        return {"depends_on_past": True,
                "wait_for_downstream": False}

    def post_check(self, working_dir):

        first_date = True
        prev_time_b = None

        for date in self.dates:
            file_a_date = self.file_a % (working_dir, date)
            file_b_date = self.file_b % (working_dir, date)

            with open(file_a_date) as f:
                assert "success_a\n" == f.readline(), \
                    "The file %s doesn't contain the success line" % file_a_date

            with open(file_b_date) as f:
                assert "success_b\n" == f.readline(), \
                    "The file %s doesn't contain the success line" % file_b_date

            time_a = time.ctime(os.path.getmtime(file_a_date))
            time_b = time.ctime(os.path.getmtime(file_b_date))
            assert time_a < time_b, \
                "Task a was not executed before Task b for date %s" % date

            if not first_date:
                assert (time_b > prev_time_b), \
                    "Task b of date %s did not wait for his past" % date

            first_date = False
            prev_time_b = time_b


class BashOperatorABWaitForDownstream(BashOperatorABDownStream, DagBackfillTest):

    dates = ["2015-01-01", "2015-01-02", "2015-01-03", "2015-01-04",
             "2015-01-05", "2015-01-06", "2015-01-07", "2015-01-08",
             "2015-01-09", "2015-01-10"]

    file_a = "%s/out.a.%s.txt"
    file_b = "%s/out.b.%s.txt"

    def get_dag_id(self):
        return "bash_operator_ab_retries"

    def get_test_context(self):
        return {"depends_on_past": False,
                "wait_for_downstream": True}

    def post_check(self, working_dir):

        first_date = True
        prev_time_b = None

        for date in self.dates:
            file_a_date = self.file_a % (working_dir, date)
            file_b_date = self.file_b % (working_dir, date)

            with open(file_a_date) as f:
                assert "success_a\n" == f.readline(), \
                    "The file %s doesn't contain the success line" % file_a_date

            with open(file_b_date) as f:
                assert "success_b\n" == f.readline(), \
                    "The file %s doesn't contain the success line" % file_b_date

            time_a = time.ctime(os.path.getmtime(file_a_date))
            time_b = time.ctime(os.path.getmtime(file_b_date))
            assert time_a < time_b, \
                "Task a was not executed before Task b for date %s" % date

            if not first_date:
                assert (time_a > prev_time_b), \
                    "Task a of date %s did not wait for his previous " \
                    "downstream tasks to finish" % date

            first_date = False
            prev_time_b = time_b
