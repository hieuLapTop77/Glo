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
    tags=["Stripe Past Due Status", "redshift"],
    max_active_runs=1
)
def Stripe_Past_Due_Status():

    Stripe_on_Hold_Status = TriggerDagRunOperator(
        task_id="trigger_Stripe_on_Hold_Status",
        trigger_dag_id="Stripe_on_Hold_Status",
        wait_for_completion=True
    )

    @task
    def call_stripe_past_due_status():
        print("Calling stripe_past_due_status")
        proc_name = "analytics.glue_load_stripe_past_due_status()"
        call_procedure(proc_name=proc_name)

    ###############################
    Stripe_on_Hold_Status >> call_stripe_past_due_status()


dag = Stripe_Past_Due_Status()
