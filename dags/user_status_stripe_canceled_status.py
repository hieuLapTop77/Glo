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
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    tags=["Stripe Canceled Status", "redshift"],
    max_active_runs=1
)
def Stripe_Canceled_Status():

    Stripe_Past_Due_Status = ExternalTaskSensor(
        task_id="wait_for_Stripe_Past_Due_Status",
        external_dag_id="Stripe_Past_Due_Status",
        external_task_id=None,
        mode="poke",
        timeout=600,
    )

    @task
    def call_stripe_canceled_status():
        print("Calling stripe_canceled_status")
        proc_name = "analytics.glue_load_stripe_canceled_status()"
        call_procedure(proc_name=proc_name)

    ###############################
    Stripe_Past_Due_Status >> call_stripe_canceled_status()


dag = Stripe_Canceled_Status()
