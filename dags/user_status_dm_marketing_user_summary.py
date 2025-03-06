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
    tags=["Dm Marketing User Summary", "redshift"],
    max_active_runs=1
)
def Dm_Marketing_User_Summary():
    Stg_Subscription_Subscriber_Transitions = ExternalTaskSensor(
        task_id="wait_for_Stg_Subscription_Subscriber_Transitions",
        external_dag_id="Stg_Subscription_Subscriber_Transitions",
        external_task_id=None,
        mode="poke",
        timeout=600,
    )

    @task
    def call_dm_marketing_user_summary():
        print("Calling dm_marketing_user_summary")
        proc_name = "analytics.glue_load_dm_marketing_user_summary()"
        call_procedure(proc_name=proc_name)

    ###############################
    Stg_Subscription_Subscriber_Transitions >> call_dm_marketing_user_summary()


dag = Dm_Marketing_User_Summary()
