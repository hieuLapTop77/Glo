from airflow.decorators import dag, task
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.dates import days_ago
from common.helper import call_procedure

default_args = {
    "owner": "dbnguyen",
    "email": ["dbnguyen@glo.com"],
    "email_on_failure": False,
}


@dag(
    default_args=default_args,
    schedule_interval="0 16 * * *",
    start_date=days_ago(1),
    catchup=False,
    tags=["Last Value Union", "redshift"],
    max_active_runs=1
)
def Last_Value_Union():
    Stripe_Status_Qa_Suite = ExternalTaskSensor(
        task_id="wait_for_Stripe_Status_Qa_Suite",
        external_dag_id="Stripe_Status_Qa_Suite",
        external_task_id=None,
        mode="poke",
        timeout=600,
    )

    @task
    def call_last_value_union():
        print("Calling last_value_union")
        proc_name = "analytics.load_last_value_union()"
        call_procedure(proc_name=proc_name)

    ###############################
    Stripe_Status_Qa_Suite >> call_last_value_union()


dag = Last_Value_Union()
