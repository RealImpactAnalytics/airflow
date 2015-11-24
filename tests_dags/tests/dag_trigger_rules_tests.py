from datetime import datetime
from unittest import TestCase

from airflow.utils import TriggerRule
from ..dag_tester import EndToEndBackfillJobTest, validate_file_content


class AllFailureDetectionJobShouldRunIfAllUpstreamFail(TestCase,
                                                       EndToEndBackfillJobTest):
    """
        Builds and validate the behaviour of a dag with 2 task that fail,
        and a third one that should run only if the first two have indeed
        failed.

        The Dag is executed only on one single date
    """

    def get_dag_file_names(self):
        return ["bash_operator_failed_detection.py"]

    def get_backfill_params(self):
        return {"start_date": datetime(2015, 1, 1),
                "end_date": datetime(2015, 1, 1)}

    def get_context(self):
        return {"3rd_task_trigger_rule": TriggerRule.ALL_FAILED}

    def post_check(self, working_dir):
        validate_file_content(working_dir, "out.failing_a.2015-01-01.txt",
                              "executed\n")
        validate_file_content(working_dir, "out.failing_b.2015-01-01.txt",
                              "executed\n")

        # BUG: this one fails because the 3rd task never gets triggered
        validate_file_content(working_dir,
                              "out.last_operator.2015-01-01.txt",
                              "executed\n")


class AnyFailureDetectionJobShouldRunIfAllUpstreamFail(
        AllFailureDetectionJobShouldRunIfAllUpstreamFail):
    """
        Builds and validate the behaviour of a dag with 2 task that fail,
        and a third one that should run only if any of the first two have
        failed.

        The Dag is executed only on one single date
    """

    def get_context(self):
        return {"3rd_task_trigger_rule": TriggerRule.ONE_FAILED}

    def post_check(self, working_dir):
        validate_file_content(working_dir, "out.failing_a.2015-01-01.txt",
                              "executed\n")
        validate_file_content(working_dir, "out.failing_b.2015-01-01.txt",
                              "executed\n")

        # BUG: this one fails because the 3rd task never gets triggered
        validate_file_content(working_dir,
                              "out.last_operator.2015-01-01.txt",
                              "executed\n")


class DoneDetectionJobShouldRunIfAllUpstreamFail(
        AllFailureDetectionJobShouldRunIfAllUpstreamFail):
    """
        Builds and validate the behaviour of a dag with 2 task that fail,
        and a third one that should run only if the first two have completed

        The Dag is executed only on one single date
    """

    def get_context(self):
        return {"3rd_task_trigger_rule": TriggerRule.ALL_DONE}

    def post_check(self, working_dir):
        validate_file_content(working_dir, "out.failing_a.2015-01-01.txt",
                              "executed\n")
        validate_file_content(working_dir, "out.failing_b.2015-01-01.txt",
                              "executed\n")

        # BUG: this one fails because the 3rd task never gets triggered
        validate_file_content(working_dir,
                              "out.last_operator.2015-01-01.txt",
                              "executed\n")


