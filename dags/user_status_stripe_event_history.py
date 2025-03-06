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
    tags=["Stripe Event History", "redshift"],
    max_active_runs=1
)
def Stripe_Event_History():

    Stripe_Unclear_Status = TriggerDagRunOperator(
        task_id="trigger_Stripe_Unclear_Status",
        trigger_dag_id="Stripe_Unclear_Status",
        wait_for_completion=True
    )

    @task
    def call_stripe_event_history():
        print("Calling stripe_event_history")
        proc_name = "analytics.glue_load_stripe_event_history()"
        call_procedure(proc_name=proc_name)

    ###############################
    Stripe_Unclear_Status >> call_stripe_event_history()


dag = Stripe_Event_History()
