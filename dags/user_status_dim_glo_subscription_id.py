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
    tags=["Dim Glo Subscription Id", "redshift"],
    max_active_runs=1
)
def Dim_Glo_Subscription_Id():
    Dm_Apple_Daily_Summary = ExternalTaskSensor(
        task_id="wait_for_Dm_Apple_Daily_Summary",
        external_dag_id="Dm_Apple_Daily_Summary",
        external_task_id=None,
        mode="poke",
        timeout=600,
    )
    Stripe_Event_History = ExternalTaskSensor(
        task_id="wait_for_Stripe_Event_History",
        external_dag_id="Stripe_Event_History",
        external_task_id=None,
        mode="poke",
        timeout=600,
    )

    @task
    def call_dim_glo_subscription_id():
        print("Calling dim_glo_subscription_id")
        proc_name = "analytics.glue_load_dim_glo_subscription_id()"
        call_procedure(proc_name=proc_name)

    ###############################
    [Stripe_Event_History, Dm_Apple_Daily_Summary] >> call_dim_glo_subscription_id()


dag = Dim_Glo_Subscription_Id()
