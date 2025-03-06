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
    tags=["Stripe Status Qa Suite", "redshift"],
    max_active_runs=1
)
def Stripe_Status_Qa_Suite():
    Subscription_Status = ExternalTaskSensor(
        task_id="wait_for_Subscription_Status",
        external_dag_id="Subscription_Status",
        external_task_id=None,
        mode="poke",
        timeout=600,
    )

    @task
    def call_stripe_status_qa_suite():
        print("Calling stripe_status_qa_suite")
        proc_name = "analytics.glue_load_stripe_status_qa_suite()"
        call_procedure(proc_name=proc_name)

    ###############################
    Subscription_Status >> call_stripe_status_qa_suite()


dag = Stripe_Status_Qa_Suite()
