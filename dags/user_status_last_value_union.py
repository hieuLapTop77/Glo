from airflow.decorators import dag, task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.dates import days_ago
from common.helper import call_procedure

default_args = {
    "owner": "dbnguyen",
    "email": ["dbnguyen@glo.com"],
    "email_on_failure": False,
}


@dag(
    default_args=default_args,
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    tags=["Last Value Union", "redshift"],
    max_active_runs=1
)
def Last_Value_Union():

    Stripe_Status_Qa_Suite = TriggerDagRunOperator(
        task_id="trigger_Stripe_Status_Qa_Suite",
        trigger_dag_id="Stripe_Status_Qa_Suite",
        wait_for_completion=True
    )

    @task
    def call_last_value_union():
        print("Calling last_value_union")
        proc_name = "analytics.load_last_value_union()"
        call_procedure(proc_name=proc_name)

    ###############################
    Stripe_Status_Qa_Suite >> call_last_value_union()


dag = Last_Value_Union()
