from datetime import datetime

from airflow import DAG
from airflow.models import Variable, Trigger
from airflow.operators.bash_operator import BashOperator

default_args = {
    'owner': 'unittest',
    'start_date': datetime(2015, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False
    }

dag = DAG("bash_operator_abc", default_args=default_args)

# no default value for those: it is a bug to try to load this DAG without
# preparing a tmp folder for it
tempDir = Variable.get("unit_test_tmp_dir")

a = BashOperator(
    task_id='echo_a',
    bash_command='echo success_a > %s/out.a.{{ ds }}.txt' % tempDir,
    dag=dag)

b = BashOperator(
    task_id='echo_b',
    bash_command='echo success_b > %s/out.b.{{ ds }}.txt' % tempDir,
    dag=dag)

c = BashOperator(
    task_id='echo_c',
    bash_command='echo success_c > %s/out.c.{{ ds }}.txt' % tempDir,
    dag=dag)

a.set_downstream(b)

add_trigger_on_past = Variable.get('add_trigger_on_past',
                                   default_var=False,
                                   deserialize_json=True)

if add_trigger_on_past:
    c.set_trigger(Trigger(b, past_executions=(-1, 0)))
else:
    c.set_upstream(b)
