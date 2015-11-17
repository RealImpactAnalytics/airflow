"""
    General entry point for testing end to end dags
"""

from airflow import configuration, AirflowException
from airflow import executors, models, settings, utils
from airflow.configuration import DEFAULT_CONFIG, AIRFLOW_HOME
from airflow.models import DagBag
from airflow.settings import Session
import os
import re


class DagBackfillTest(object):
    """
        Framework to setup, run and check end to end executions of a DAG controlled
    """

    def build_job(self, dag):
        raise NotImplementedError()

    def get_dag_id(self):
        raise NotImplementedError()

    def post_check(self):
        raise NotImplementedError()

    def reset(self, dag_id):
        session = Session()
        tis = session.query(models.TaskInstance).filter_by(dag_id=dag_id)
        tis.delete()
        session.commit()
        session.close()

    def copy_config(self, dags_folder):

        # build a config file with a dag folder pointing to the tested dags
        config = configuration.default_config()
        config = re.sub("dags_folder =.*", "dags_folder = %s" % dags_folder, config)

        # this is the config file that will be used by the child process
        config_location = "%s/dag_test_airflow.cfg" % AIRFLOW_HOME
        with open (config_location, "w") as cfg_file:
            cfg_file.write(config)

        # this is the config that is currently present in memory
        configuration.conf.set("core", "DAGS_FOLDER", dags_folder)

        return config_location


    def test_run(self):

        #configuration.test_mode()
        dags_folder = "%s/dags" % os.path.dirname(__file__)
        config_location = self.copy_config(dags_folder )

        dagbag = DagBag(dags_folder, include_examples=False)

        if self.get_dag_id() not in dagbag.dags:
            raise AirflowException("DAG id %s not found in folder %s" % (self.get_dag_id(), dags_folder))

        dag = dagbag.dags[self.get_dag_id()]
        job = self.build_job(dag)

        # we must set the sequencial environment ourselves to control
        if job.executor != executors.DEFAULT_EXECUTOR:
            raise AirflowException("DAG test may not set the executor")

        test_env = os.environ.copy()
        test_env.update({"AIRFLOW_CONFIG": config_location})
        job.executor = executors.SequentialExecutor(env=test_env)

        self.reset(self.get_dag_id())

        job.dag.clear()
        job.run()

        self.post_check()
        self.reset(job.dag.dag_id)
