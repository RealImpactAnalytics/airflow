
import unittest
from datetime import datetime

from airflow.bin import cli
from airflow.models import TaskInstance
from airflow.settings import Session
from airflow.utils import State
from tests.dagtest.dag_tester import BackFillJobRunner, is_config_db_concurrent
from airflow.jobs import BaseJob


class ClearTestHelper():

    def __init__(self, dag_id):
        self.dag_id = dag_id
        self.original_task_instances = []

    def validate_run(self):
        session = Session()

        # Make sure all went well with the job
        self.assert_number_of_tis(15)
        self.assert_all_tis_are_successful()

        job_ids = [ti.job_id for ti in self.get_tis()]
        jobs = session.query(BaseJob) \
            .filter(BaseJob.id.in_(job_ids)) \
            .all()

        for job in jobs:
            assert job.state == State.SUCCESS

        session.close()

    def save_original_task_instances(self):
        self.original_task_instances = self.get_tis()

    def setUp(self):
        session = Session()
        # Restore DB to initial state
        for original_task_instance in self.original_task_instances:
            session.merge(original_task_instance)
        session.commit()
        session.close()

    def get_tis(self):
        session = Session()
        tis = session.query(TaskInstance) \
            .filter(TaskInstance.dag_id == self.dag_id) \
            .all()
        session.close()
        return tis

    def assert_number_of_tis(self, expected_number):
        assert len(self.get_tis()) == expected_number

    def assert_all_tis_are_successful(self):
        for task_instance in self.get_tis():
            assert task_instance.state == State.SUCCESS

    def assert_find_ti_with(self, date, task_id):
        found = False
        for ti in self.get_tis():
            if ti.execution_date == date and ti.task_id == task_id:
                found = True
                break
        assert found, "{}:{} was not found".format(task_id, date)


class SimpleClearTests(unittest.TestCase):

    skip_message = "DB Backend must support concurrent access"

    @classmethod
    def setUpClass(cls):

        if is_config_db_concurrent():
            backfill_params = {"start_date": datetime(2015, 1, 1),
                               "end_date": datetime(2015, 1, 5)}

            dag_file_names = ["bash_operator_abc.py"]

            # Run a fake job to make get an initial DB state we can work with
            cls._runner = BackFillJobRunner(backfilljob_params=backfill_params,
                                            dag_file_names=dag_file_names)

            cls._dagbag = cls._runner.dagbag
            cls._dag_id = cls._runner.dag.dag_id
            cls._dag_folder = "DAGS_FOLDER/{}".format(dag_file_names[0])

            cls._runner.run()

            cls.helper = ClearTestHelper(cls._dag_id)
            cls.helper.validate_run()

            # Save DB state
            cls.helper.save_original_task_instances()

            cls.parser = cli.get_parser()

    @classmethod
    def tearDownClass(self):
        if is_config_db_concurrent():
            self._runner.cleanup()

    def setUp(self):
        self.helper.setUp()

    @unittest.skipIf(not is_config_db_concurrent(), skip_message)
    def test_clear_all(self):

        cli.clear(self.parser.parse_args([
            'clear',
            '-sd', self._dag_folder,
            '--no_confirm',
            self._dag_id]))

        self.helper.assert_number_of_tis(0)

    @unittest.skipIf(not is_config_db_concurrent(), skip_message)
    def test_clear_with_start(self):

        start_date = datetime(2015, 1, 3).isoformat()
        cli.clear(self.parser.parse_args([
            'clear',
            '-s', start_date,
            '-sd', self._dag_folder,
            '--no_confirm',
            self._dag_id]))

        remaining_dates = [datetime(2015, 1, 1), datetime(2015, 1, 2)]
        remaining_task_ids = ['echo_a', 'echo_b', 'echo_c']

        self.helper.assert_number_of_tis(
            len(remaining_dates) * len(remaining_task_ids))

        self.helper.assert_all_tis_are_successful()

        for date in remaining_dates:
            for task_id in remaining_task_ids:
                self.helper.assert_find_ti_with(date, task_id)

    @unittest.skipIf(not is_config_db_concurrent(), skip_message)
    def test_clear_with_end(self):

        end_date = datetime(2015, 1, 4).isoformat()
        cli.clear(self.parser.parse_args([
            'clear',
            '-e', end_date,
            '-sd', self._dag_folder,
            '--no_confirm',
            self._dag_id]))

        remaining_dates = [datetime(2015, 1, 5)]
        remaining_task_ids = ['echo_a', 'echo_b', 'echo_c']

        self.helper.assert_number_of_tis(
            len(remaining_dates) * len(remaining_task_ids))

        self.helper.assert_all_tis_are_successful()

        for date in remaining_dates:
            for task_id in remaining_task_ids:
                self.helper.assert_find_ti_with(date, task_id)

    @unittest.skipIf(not is_config_db_concurrent(), skip_message)
    def test_clear_with_start_end(self):

        start_date = datetime(2015, 1, 3).isoformat()
        end_date = datetime(2015, 1, 4).isoformat()
        cli.clear(self.parser.parse_args([
            'clear',
            '-s', start_date,
            '-e', end_date,
            '-sd', self._dag_folder,
            '--no_confirm',
            self._dag_id]))

        remaining_dates = [datetime(2015, 1, 1),
                           datetime(2015, 1, 2),
                           datetime(2015, 1, 5)]
        remaining_task_ids = ['echo_a', 'echo_b', 'echo_c']

        self.helper.assert_number_of_tis(
            len(remaining_dates) * len(remaining_task_ids))

        self.helper.assert_all_tis_are_successful()

        for date in remaining_dates:
            for task_id in remaining_task_ids:
                self.helper.assert_find_ti_with(date, task_id)

    @unittest.skipIf(not is_config_db_concurrent(), skip_message)
    def test_clear_with_start_end_task(self):

        start_date = datetime(2015, 1, 3).isoformat()
        end_date = datetime(2015, 1, 4).isoformat()
        cli.clear(self.parser.parse_args([
            'clear',
            '-t', 'echo_b',
            '-s', start_date,
            '-e', end_date,
            '-sd', self._dag_folder,
            '--no_confirm',
            self._dag_id]))

        all_dates = [datetime(2015, 1, 1),
                     datetime(2015, 1, 2),
                     datetime(2015, 1, 3),
                     datetime(2015, 1, 4),
                     datetime(2015, 1, 5)]

        remaining_dates = [datetime(2015, 1, 1),
                           datetime(2015, 1, 2),
                           datetime(2015, 1, 5)]

        self.helper.assert_number_of_tis(
            len(remaining_dates) + 2*len(all_dates))

        self.helper.assert_all_tis_are_successful()

        for date in all_dates:
            self.helper.assert_find_ti_with(date, 'echo_a')
            self.helper.assert_find_ti_with(date, 'echo_c')

        for date in remaining_dates:
            self.helper.assert_find_ti_with(date, 'echo_b')

    @unittest.skipIf(not is_config_db_concurrent(), skip_message)
    def test_clear_with_start_task_upstream(self):

        start_date = datetime(2015, 1, 3).isoformat()
        cli.clear(self.parser.parse_args([
            'clear',
            '-t', 'echo_b',
            '-s', start_date,
            '--upstream',
            '-sd', self._dag_folder,
            '--no_confirm',
            self._dag_id]))

        remaining_task_and_dates = [('echo_a', datetime(2015, 1, 1)),
                                    ('echo_b', datetime(2015, 1, 1)),
                                    ('echo_c', datetime(2015, 1, 1)),
                                    ('echo_a', datetime(2015, 1, 2)),
                                    ('echo_b', datetime(2015, 1, 2)),
                                    ('echo_c', datetime(2015, 1, 2)),
                                    ('echo_c', datetime(2015, 1, 3)),
                                    ('echo_c', datetime(2015, 1, 4)),
                                    ('echo_c', datetime(2015, 1, 5))]

        self.helper.assert_number_of_tis(len(remaining_task_and_dates))

        self.helper.assert_all_tis_are_successful()

        for (task_id, date) in remaining_task_and_dates:
            self.helper.assert_find_ti_with(date, task_id)

    @unittest.skipIf(not is_config_db_concurrent(), skip_message)
    def test_clear_with_end_task_downstream(self):

        end_date = datetime(2015, 1, 4).isoformat()
        cli.clear(self.parser.parse_args([
            'clear',
            '-t', 'echo_b',
            '-e', end_date,
            '--downstream',
            '-sd', self._dag_folder,
            '--no_confirm',
            self._dag_id]))

        remaining_task_and_dates = [('echo_a', datetime(2015, 1, 1)),
                                    ('echo_a', datetime(2015, 1, 2)),
                                    ('echo_a', datetime(2015, 1, 3)),
                                    ('echo_a', datetime(2015, 1, 4)),
                                    ('echo_a', datetime(2015, 1, 5)),
                                    ('echo_b', datetime(2015, 1, 5)),
                                    ('echo_c', datetime(2015, 1, 5))]

        self.helper.assert_number_of_tis(len(remaining_task_and_dates))

        self.helper.assert_all_tis_are_successful()

        for (task_id, date) in remaining_task_and_dates:
            self.helper.assert_find_ti_with(date, task_id)

    @unittest.skipIf(not is_config_db_concurrent(), skip_message)
    def test_clear_with_end_task_upstream_downstream(self):

        end_date = datetime(2015, 1, 4).isoformat()
        cli.clear(self.parser.parse_args([
            'clear',
            '-t', 'echo_b',
            '-e', end_date,
            '--upstream',
            '--downstream',
            '-sd', self._dag_folder,
            '--no_confirm',
            self._dag_id]))

        remaining_task_and_dates = [('echo_a', datetime(2015, 1, 5)),
                                    ('echo_b', datetime(2015, 1, 5)),
                                    ('echo_c', datetime(2015, 1, 5))]

        self.helper.assert_number_of_tis(len(remaining_task_and_dates))
        self.helper.assert_all_tis_are_successful()

        for (task_id, date) in remaining_task_and_dates:
            self.helper.assert_find_ti_with(date, task_id)


class ComplexClearTests(unittest.TestCase):

    skip_message = "DB Backend must support concurrent access"

    @classmethod
    def setUpClass(cls):

        if is_config_db_concurrent():
            backfill_params = {"start_date": datetime(2015, 1, 1),
                               "end_date": datetime(2015, 1, 5)}

            dag_file_names = ["bash_operator_abc.py"]

            context = {'add_trigger_on_past': True}

            # Run a fake job to make get an initial DB state we can work with
            cls._runner = BackFillJobRunner(backfilljob_params=backfill_params,
                                            dag_file_names=dag_file_names,
                                            context=context)

            cls._dagbag = cls._runner.dagbag
            cls._dag_id = cls._runner.dag.dag_id
            cls._dag_folder = "DAGS_FOLDER/{}".format(dag_file_names[0])

            cls._runner.run()

            cls.helper = ClearTestHelper(cls._dag_id)
            cls.helper.validate_run()

            # Save DB state
            cls.helper.save_original_task_instances()

            cls.parser = cli.get_parser()

    @classmethod
    def tearDownClass(self):
        if is_config_db_concurrent():
            self._runner.cleanup()

    def setUp(self):
        self.helper.setUp()

    @unittest.skipIf(not is_config_db_concurrent(), skip_message)
    def test_clear_all(self):

        cli.clear(self.parser.parse_args([
            'clear',
            '-sd', self._dag_folder,
            '--no_confirm',
            self._dag_id]))

        self.helper.assert_number_of_tis(0)

    @unittest.skipIf(not is_config_db_concurrent(), skip_message)
    def test_clear_with_start_end(self):

        start_date = datetime(2015, 1, 3).isoformat()
        end_date = datetime(2015, 1, 4).isoformat()
        cli.clear(self.parser.parse_args([
            'clear',
            '-s', start_date,
            '-e', end_date,
            '-sd', self._dag_folder,
            '--no_confirm',
            self._dag_id]))

        remaining_dates = [datetime(2015, 1, 1),
                           datetime(2015, 1, 2),
                           datetime(2015, 1, 5)]
        remaining_task_ids = ['echo_a', 'echo_b', 'echo_c']

        self.helper.assert_number_of_tis(
            len(remaining_dates) * len(remaining_task_ids))

        self.helper.assert_all_tis_are_successful()

        for date in remaining_dates:
            for task_id in remaining_task_ids:
                self.helper.assert_find_ti_with(date, task_id)

    @unittest.skipIf(not is_config_db_concurrent(), skip_message)
    def test_clear_with_start_end_task(self):

        start_date = datetime(2015, 1, 3).isoformat()
        end_date = datetime(2015, 1, 4).isoformat()
        cli.clear(self.parser.parse_args([
            'clear',
            '-t', 'echo_b',
            '-s', start_date,
            '-e', end_date,
            '-sd', self._dag_folder,
            '--no_confirm',
            self._dag_id]))

        all_dates = [datetime(2015, 1, 1),
                     datetime(2015, 1, 2),
                     datetime(2015, 1, 3),
                     datetime(2015, 1, 4),
                     datetime(2015, 1, 5)]

        remaining_dates = [datetime(2015, 1, 1),
                           datetime(2015, 1, 2),
                           datetime(2015, 1, 5)]

        self.helper.assert_number_of_tis(
            len(remaining_dates) + 2*len(all_dates))

        self.helper.assert_all_tis_are_successful()

        for date in all_dates:
            self.helper.assert_find_ti_with(date, 'echo_a')
            self.helper.assert_find_ti_with(date, 'echo_c')

        for date in remaining_dates:
            self.helper.assert_find_ti_with(date, 'echo_b')

    @unittest.skipIf(not is_config_db_concurrent(), skip_message)
    def test_clear_with_start_end_task_upstream(self):

        start_date = datetime(2015, 1, 3).isoformat()
        end_date = datetime(2015, 1, 4).isoformat()
        cli.clear(self.parser.parse_args([
            'clear',
            '-t', 'echo_b',
            '-s', start_date,
            '-e', end_date,
            '-sd', self._dag_folder,
            '--no_confirm',
            self._dag_id]))

        all_dates = [datetime(2015, 1, 1),
                     datetime(2015, 1, 2),
                     datetime(2015, 1, 3),
                     datetime(2015, 1, 4),
                     datetime(2015, 1, 5)]

        remaining_dates = [datetime(2015, 1, 1),
                           datetime(2015, 1, 2),
                           datetime(2015, 1, 5)]

        self.helper.assert_number_of_tis(
            len(remaining_dates) + 2*len(all_dates))

        self.helper.assert_all_tis_are_successful()

        for date in all_dates:
            self.helper.assert_find_ti_with(date, 'echo_a')
            self.helper.assert_find_ti_with(date, 'echo_c')

        for date in remaining_dates:
            self.helper.assert_find_ti_with(date, 'echo_b')
