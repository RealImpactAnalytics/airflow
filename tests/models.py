import os
import tempfile
import unittest
import datetime

from airflow import models
from airflow.models import Variable


class DagRunTest(unittest.TestCase):
    def test_id_for_date(self):
        run_id = models.DagRun.id_for_date(
            datetime.datetime(2015, 01, 02, 03, 04, 05, 06, None))
        assert run_id == 'scheduled__2015-01-02T03:04:05', (
            'Generated run_id did not match expectations: {0}'.format(run_id))


class DagBagTest(unittest.TestCase):

    def test_successful_read_tested_dags(self):
        """
        success test: validates were're able to parse parse all the DAGs in the
        dagtest/dags folder
        """

        # Build a temp dir for the test DAG, so that loading them succeeds
        tmp_dir = tempfile.mkdtemp()
        Variable.set("unit_test_tmp_dir", tmp_dir , serialize_json=True)

        dag_folder = "{}/dagtest/dags".format(os.path.dirname(__file__))
        dagbag = models.DagBag(dag_folder=dag_folder,
                               include_examples=False)

        expected_dag_ids = ["bash_operator_ab", "bash_operator_ab_retries",
                            "bash_operator_single"]

        for dag_id in expected_dag_ids:
            dag = dagbag.get_dag(dag_id)

            assert dag is not None
            assert dag.dag_id == dag_id

        assert dagbag.size() == len(expected_dag_ids)

        non_existing_dag_id = "Fake DaG iD - th1s ID dOezz noo_t exiSts, rite?"
        assert dagbag.get_dag(non_existing_dag_id) is None





