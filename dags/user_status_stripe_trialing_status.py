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
    tags=["Stripe Trialing Status", "redshift"],
    max_active_runs=1
)
def Stripe_Trialing_Status():

    Stg_Consolidated_Stripe_Data = TriggerDagRunOperator(
        task_id="trigger_Stg_Consolidated_Stripe_Data",
        trigger_dag_id="Stg_Consolidated_Stripe_Data",
        wait_for_completion=True
    )

    @task
    def call_stripe_trialing_status():
        print("Calling stripe_trialing_status")
        proc_name = "analytics.glue_load_stripe_trialing_status()"
        call_procedure(proc_name=proc_name)

    ###############################
    Stg_Consolidated_Stripe_Data >> call_stripe_trialing_status()


dag = Stripe_Trialing_Status()
