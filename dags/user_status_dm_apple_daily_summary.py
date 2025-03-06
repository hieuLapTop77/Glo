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
    tags=["Dm Apple Daily Summary", "redshift"],
    max_active_runs=1
)
def Dm_Apple_Daily_Summary():
    Apple_User_Paid_Months = ExternalTaskSensor(
        task_id="wait_for_Apple_User_Paid_Months",
        external_dag_id="Apple_User_Paid_Months",
        external_task_id=None,
        mode="poke",
        timeout=600,
    )

    @task
    def call_dm_apple_daily_summary():
        print("Calling dm_apple_daily_summary")
        proc_name = "analytics.glue_load_dm_apple_daily_summary()"
        call_procedure(proc_name=proc_name)

    ###############################
    Apple_User_Paid_Months >> call_dm_apple_daily_summary()


dag = Dm_Apple_Daily_Summary()
