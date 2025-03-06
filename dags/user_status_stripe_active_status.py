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
    tags=["Stripe Active Status", "redshift"],
    max_active_runs=1
)
def Stripe_Active_Status():

    Stripe_Trialing_Status = TriggerDagRunOperator(
        task_id="trigger_Stripe_Trialing_Status",
        trigger_dag_id="Stripe_Trialing_Status",
        wait_for_completion=True
    )

    @task
    def call_stripe_active_status():
        print("Calling stripe_active_status")
        proc_name = "analytics.glue_load_stripe_active_status()"
        call_procedure(proc_name=proc_name)

    ###############################
    Stripe_Trialing_Status >> call_stripe_active_status()


dag = Stripe_Active_Status()
