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
    tags=["Stripe on Hold Status", "redshift"],
    max_active_runs=1
)
def Stripe_on_Hold_Status():

    Stripe_Free_Time_Status = ExternalTaskSensor(
        task_id="wait_for_Stripe_Free_Time_Status",
        external_dag_id="Stripe_Free_Time_Status",
        external_task_id=None,
        mode="poke",
        timeout=600,
    )

    @task
    def call_stripe_on_hold_status():
        print("Calling stripe_on_hold_status")
        proc_name = "analytics.glue_load_stripe_on_hold_status()"
        call_procedure(proc_name=proc_name)

    ###############################
    Stripe_Free_Time_Status >> call_stripe_on_hold_status()


dag = Stripe_on_Hold_Status()
