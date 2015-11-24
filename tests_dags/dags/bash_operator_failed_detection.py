"""
This parameterized DAG contains:
 - two operators that systematically fail
 - one operator which depends on the two above with a trigger_rule specified
 from context

=> this is essentially a test of the various TriggerRules (see
dag_trigger_rules_tests.py for the actual tests)

"""

from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash_operator import BashOperator

default_args = {
    'owner': 'unittest',
    'start_date': datetime(2015, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False
    }

dag = DAG("bash_operator_run_if_failure", default_args=default_args)

tempDir = Variable.get("unit_test_tmp_dir")

# first building 2 operator that output some side effect, and then fail:
def failing_bash(name, parentDag):
    """
    builds a bash operator that write to disk and then fails
    """
    bash_command = """
    echo executed > %s/out.%s.{{ ds }}.txt
    exit 1
    """ % (tempDir, name)

    return BashOperator(
        task_id='failing_{0}'.format(name),
        bash_command=bash_command,
        dag=parentDag)

failing_a = failing_bash("failing_a", dag)
failing_b = failing_bash("failing_b", dag)

trigger_rule = Variable.get("3rd_task_trigger_rule")

last_operator = BashOperator(
    task_id='last_operator',
    bash_command='echo executed > %s/out.last_operator.{{ ds }}.txt' %
                 tempDir,
    trigger_rule=trigger_rule,
    dag=dag)

last_operator.set_upstream([failing_a, failing_b])
