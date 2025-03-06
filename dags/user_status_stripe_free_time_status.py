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
    tags=["Stripe Free Time Status", "redshift"],
    max_active_runs=1
)
def Stripe_Free_Time_Status():

    Stripe_Active_Without_Invoice_Status = TriggerDagRunOperator(
        task_id="trigger_Stripe_Active_Without_Invoice_Status",
        trigger_dag_id="Stripe_Active_Without_Invoice_Status",
        wait_for_completion=True
    )

    @task
    def call_stripe_free_time_status():
        print("Calling stripe_free_time_status")
        proc_name = "analytics.glue_load_stripe_free_time_status()"
        call_procedure(proc_name=proc_name)

    ###############################
    Stripe_Active_Without_Invoice_Status >> call_stripe_free_time_status()


dag = Stripe_Free_Time_Status()
