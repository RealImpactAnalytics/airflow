"""
General entry point for testing end to end dags
"""
import logging
import os
import re
import shutil
import tempfile
import time

from airflow import configuration
from airflow import executors
from airflow.configuration import TEST_CONFIG_FILE
from airflow.jobs import BackfillJob, SchedulerJob
from airflow.models import DagBag, Variable
from ..core import reset


class AbstractEndToEndTest():
    """
    Convenience super class with common abstract methods between
    EndToEndBackfillJobTest and EndToEndSchedulerJobTest
    """

    def get_dag_file_names(self):
        """
        :return: a non empty list of python file names containing dag(s), to
          to tested in the context of this test.
        """

        raise NotImplementedError()

    def get_context(self):
        """
        :return: a dictionary of variables to be stored such that the
        tested DAG can access them through a Variable.get("key") statement
        """
        return {}

    def post_check(self, working_dir):
        """
        :param working_dir: the tmp file where the tested DAG has been
        executed

        Child classes should implement here any post-check and raise exceptions
         to trigger a test failure.
        """

        raise NotImplementedError()


class EndToEndBackfillJobTest(AbstractEndToEndTest):
    """
    Abstract class to implement in order to execute an end-to-end DAG test based
    on a BackfillJob.
    """

    def get_backfill_params(self):
        """
        :return: dictionary **kwargs argument for building the BackfillJob
        execution of this test.
        """
        raise NotImplementedError()

    def test_backfilljob(self):

        with BackFillJobRunner(self.get_backfill_params(),
                               dag_file_names=self.get_dag_file_names(),
                               context=self.get_context()) as runner:

            if runner.run():
                self.post_check(runner.working_dir)


class EndToEndSchedulerJobTest(AbstractEndToEndTest):
    """
    Abstract class to implement in order to execute an end-to-end DAG test based
    on a SchedulerJob.
    """

    def get_schedulerjob_params(self):
        """
        :return: dictionary **kwargs argument for building the BackfillJob
        execution of this test.
        """
        raise NotImplementedError()

    def test_schedulerjob(self):

        with SchedulerJobRunner(self.get_schedulerjob_params(),
                                dag_file_names=self.get_dag_file_names(),
                                context=self.get_context()) as runner:

            if runner.run():
                self.post_check(runner.working_dir)


class Runner(object):
    """
    Abstract Runner that prepares a working temp dir and all necessary context
    variables in order to execute a job in its own isolated folder.
    """

    def __init__(self,
                 dag_file_names,
                 context,
                 ref_config_file=TEST_CONFIG_FILE):

        self.dag_file_names = dag_file_names

        # makes sure the default context is a different instance for each Runner
        self.context = context if context else {}

        # this is initialized in the constructor of the child class
        self.tested_job = None

        # preparing a folder where to execute the tests, with all the DAGs
        # temp folder where to execute the tests
        all_dags_folder = "{}/dags".format(os.path.dirname(__file__))

        self.working_dir = tempfile.mkdtemp()
        it_dag_folder = os.path.join(self.working_dir, "dags")
        os.mkdir(it_dag_folder)
        for file_name in self.dag_file_names:
            src = os.path.join(all_dags_folder, file_name)
            shutil.copy2(src, it_dag_folder)

        # saving the context to Variable so the child test can access it
        for key, val in self.context.items():
            Variable.set(key, val, serialize_json=True)
        Variable.set("unit_test_tmp_dir", self.working_dir)

        self.config_file = os.path.join(self.working_dir, "airflow_IT.cfg")
        self._create_it_config_file(ref_config_file, self.config_file,
                                    it_dag_folder)

        # aligns current config with test config (this of course would fail
        # if several dag tests are executed in parallel threads)
        configuration.AIRFLOW_CONFIG = self.config_file
        configuration.load_config()

        self.dagbag = DagBag(self.working_dir, include_examples=False)
        self.concurrent_db = is_config_db_concurrent(self.config_file)

    def run(self):
        """
        Starts the execution of the tested job.
        :return: true
        """
        if self.concurrent_db:
            self.tested_job.run()
            return True
        else:
            logging.warning("skipping execution of test since backend DB does "
                            "not support concurrent access")
            return False

    def cleanup(self):
        """
        Deletes all traces of execution of the tested job.
        This is called automatically if the Runner is used inside a with
        statement
        """
        if self.tested_job:
            logging.info("cleaning up {}".format(self.tested_job))
            reset(self.tested_job.dag.dag_id)
            os.system("rm -rf {}".format(self.working_dir))
        else:
            logging.info("(no clean up necessary)")

    ##########################
    # private methods

    def _create_it_config_file(self, ref_config_file, ut_file_location,
                               dag_folder):
        """
        Creates a custom config file for integration tests in the specified
        location, overriding the dag_folder value.
        """

        with open(ref_config_file) as test_config_file:
            config = test_config_file.read()

            config = re.sub("dags_folder =.*",
                            "dags_folder = {}".format(dag_folder), config)
            config = re.sub("job_heartbeat_sec =.*",
                            "job_heartbeat_sec = 1", config)

            # this is the config file that will be used by the child process
            # config_location = "{}/dag_test_airflow.cfg".format(AIRFLOW_HOME)
            with open(ut_file_location, "w") as cfg_file:
                cfg_file.write(config)

    ###########################
    # loan pattern to make any runner easily usable inside a with statement

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.cleanup()


class BackFillJobRunner(Runner):
    """
    Executes a backfillJob on the specified dagfiles, in its own tmp folder,
    with all necessary Variables specified in context persisted so that the
    child context has access to it.
    """

    def __init__(self, backfilljob_params, **kwargs):
        super(BackFillJobRunner, self).__init__(**kwargs)

        if self.dagbag.size() > 1:
            os.system("rm -rf {}".format(self.working_dir))
            assert False, "more than one dag found in BackfillJob test"

        if self.concurrent_db:
            dag = list(self.dagbag.dags.values())[0]
            tested_job = BackfillJob(dag=dag, **backfilljob_params)

            test_env = os.environ.copy()
            test_env.update({"AIRFLOW_CONFIG": self.config_file})
            tested_job.executor = executors.SequentialExecutor(env=test_env)

            reset(tested_job.dag.dag_id)
            tested_job.dag.clear()

            self.tested_job = tested_job


class SchedulerJobRunner(Runner):
    """
    Executes a backfillJob on the specified dagfiles, in its own tmp folder,
    with all necessary Variables specified in context persisted so that the
    child context has access to it.
    """

    def __init__(self, job_params, **kwargs):
        super(SchedulerJobRunner, self).__init__(**kwargs)

        if self.concurrent_db:
            self.tested_job = SchedulerJob(subdir=self.working_dir,
                                           **job_params)
            self.tested_job.executor = executors.LocalExecutor()

        # TODO: hack the start_date of the job in order to make the test
        #   outcome predictable (at the moment, start_date=now() )
        # (or not, see SchedulerJob.schedule: does not seem to look at
        # start_date)

        # TODO: make sure there is no trace of this dag ID in DB: dag_run,...


def is_config_db_concurrent(config_file):
    """
    :param config_file: path to some airflow config file
    :return: true if this config files points sqlalchemy to a DB that support
     concurrent access (i.e. not sqlite)
    """
    with open(config_file) as cfg:
        for line in cfg.readlines():

            if line.strip().startswith("#"):
                continue

            if "sql_alchemy_conn" in line:
                return "sqlite://" not in line

    return False


#############
# some useful post-check validation utils

def validate_file_content(folder, filename, expected_content):
    """
    Raise an exception if the specified file does not have the expected
    content, or returns silently otherwise
    """
    path = "{0}/{1}".format(folder, filename)
    with open(path) as f:
        content = f.read()
        assert expected_content == content, \
            "Unexpected content of {path}\n" \
            "  Expected content : {expected_content}\n" \
            "  Actual content : {content}".format(**locals())


def validate_order(folder, early, late):
    """
    Raise an exception if the last modification of the early file happened
    after the last modification of the late file
    """
    path_early = "{0}/{1}".format(folder, early)
    path_late = "{0}/{1}".format(folder, late)

    time_early = time.ctime(os.path.getmtime(path_early))
    time_late = time.ctime(os.path.getmtime(path_late))
    assert time_early < time_late, \
        "The last modification time of {path_early} should be before the " \
        "last modification time of {path_late} but it was not the case." \
        "".format(**locals())
