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
    tags=["Apple User Paid Months", "redshift"],
    max_active_runs=1
)
def Apple_User_Paid_Months():
    Apple_Event_History = ExternalTaskSensor(
        task_id="wait_for_Apple_Event_History",
        external_dag_id="Apple_Event_History",
        external_task_id=None,
        mode="poke",
        timeout=600,
    )

    @task
    def call_apple_user_paid_months():
        print("Calling apple_user_paid_months")
        proc_name = "analytics.glue_load_apple_user_paid_months()"
        call_procedure(proc_name=proc_name)

    ###############################
    Apple_Event_History >> call_apple_user_paid_months()


dag = Apple_User_Paid_Months()
