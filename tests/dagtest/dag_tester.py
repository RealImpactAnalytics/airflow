"""
    General entry point for testing end to end dags
"""
import tempfile

import os
import re
from airflow import configuration, AirflowException
from airflow import executors, models, settings
from airflow.configuration import AIRFLOW_HOME, TEST_CONFIG_FILE
from airflow.models import DagBag, Variable
from airflow.settings import Session


class DagBackfillTest(object):
    """
        Framework to setup, run and check end to end executions of a DAG controlled
    """

    def build_job(self, dag):
        raise NotImplementedError()

    def get_dag_id(self):
        raise NotImplementedError()

    def post_check(self, working_dir):
        raise NotImplementedError()

    def reset(self, dag_id):
        session = Session()
        tis = session.query(models.TaskInstance).filter_by(dag_id=dag_id)
        tis.delete()
        session.commit()
        session.close()

    def copy_config(self, dags_folder):

        with open(TEST_CONFIG_FILE) as test_config_file:
            config = test_config_file.read()

            config = re.sub("dags_folder =.*",
                            "dags_folder = {}".format(dags_folder), config)
            config = re.sub("job_heartbeat_sec =.*",
                            "job_heartbeat_sec = 1", config)

            # this is the config file that will be used by the child process
            config_location = "{}/dag_test_airflow.cfg".format(AIRFLOW_HOME)
            with open(config_location, "w") as cfg_file:
                cfg_file.write(config)

        # this is the config that is currently present in memory
        configuration.conf.set("core", "DAGS_FOLDER", dags_folder)

        return config_location

    def test_run(self):

        temp_dir = self.add_tmp_dir_variable()
        dags_folder = "{}/dags".format(os.path.dirname(__file__))
        config_location = self.copy_config(dags_folder)

        dagbag = DagBag(dags_folder, include_examples=False)

        if self.get_dag_id() not in dagbag.dags:
            msg = "DAG id {id} not found in folder {folder}" \
                  "".format(id=self.get_dag_id(), folder=dags_folder)
            raise AirflowException(msg)

        dag = dagbag.dags[self.get_dag_id()]
        job = self.build_job(dag)

        # we must set the sequential environment ourselves to control
        if job.executor != executors.DEFAULT_EXECUTOR:
            raise AirflowException("DAG test may not set the executor")

        test_env = os.environ.copy()
        test_env.update({"AIRFLOW_CONFIG": config_location})
        job.executor = executors.SequentialExecutor(env=test_env)

        self.reset(self.get_dag_id())

        job.dag.clear()
        job.run()

        self.post_check(temp_dir)

        os.system("rm -rf {temp_dir}".format(**locals()))
        self.reset(job.dag.dag_id)

    def add_tmp_dir_variable(self):

        unit_test_tmp_dir = tempfile.mkdtemp()

        session = settings.Session()

        old_var = session.query(Variable).filter_by(
            key="unit_test_tmp_dir").first()

        if old_var is not None:
            session.delete(old_var)
            session.commit()

        var = Variable(key="unit_test_tmp_dir", val=unit_test_tmp_dir)
        session.add(var)
        session.commit()

        return unit_test_tmp_dir
