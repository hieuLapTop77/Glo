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
    tags=["Dim Glo Subscription Id", "redshift"],
    max_active_runs=1
)
def Dim_Glo_Subscription_Id():

    @task
    def call_dim_glo_subscription_id():
        print("Calling dim_glo_subscription_id")
        proc_name = "analytics.glue_load_dim_glo_subscription_id()"
        call_procedure(proc_name=proc_name)

    Dm_Apple_Daily_Summary = TriggerDagRunOperator(
        task_id="trigger_Dm_Apple_Daily_Summary",
        trigger_dag_id="Dm_Apple_Daily_Summary",
        wait_for_completion=True
    )
    Stripe_Event_History = TriggerDagRunOperator(
        task_id="trigger_Stripe_Event_History",
        trigger_dag_id="Stripe_Event_History",
        wait_for_completion=True
    )
    ###############################
    [Stripe_Event_History, Dm_Apple_Daily_Summary] >> call_dim_glo_subscription_id()


dag = Dim_Glo_Subscription_Id()
