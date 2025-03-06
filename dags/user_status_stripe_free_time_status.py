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
    tags=["Stripe Free Time Status", "redshift"],
    max_active_runs=1
)
def Stripe_Free_Time_Status():

    Stripe_Active_Without_Invoice_Status = ExternalTaskSensor(
        task_id="wait_for_Stripe_Active_Without_Invoice_Status",
        external_dag_id="Stripe_Active_Without_Invoice_Status",
        external_task_id=None,
        mode="poke",
        timeout=600,
    )

    @task
    def call_stripe_free_time_status():
        print("Calling stripe_free_time_status")
        proc_name = "analytics.glue_load_stripe_free_time_status()"
        call_procedure(proc_name=proc_name)

    ###############################
    Stripe_Active_Without_Invoice_Status >> call_stripe_free_time_status()


dag = Stripe_Free_Time_Status()
