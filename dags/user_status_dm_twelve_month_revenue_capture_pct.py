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
    tags=["Dm Twelve Month Revenue Capture Pct", "redshift"],
    max_active_runs=1
)
def Dm_Twelve_Month_Revenue_Capture_Pct():
    Dm_Rolling_Thirty_Day_Trial_Conversion_Rate = ExternalTaskSensor(
        task_id="wait_for_Dm_Rolling_Thirty_Day_Trial_Conversion_Rate",
        external_dag_id="Dm_Rolling_Thirty_Day_Trial_Conversion_Rate",
        external_task_id=None,
        mode="poke",
        timeout=600,
    )

    @task
    def call_dm_twelve_month_revenue_capture_pct():
        print("Calling dm_twelve_month_revenue_capture_pct")
        proc_name = "analytics.glue_load_dm_twelve_month_revenue_capture_pct()"
        call_procedure(proc_name=proc_name)

    ###############################
    Dm_Rolling_Thirty_Day_Trial_Conversion_Rate >> call_dm_twelve_month_revenue_capture_pct()


dag = Dm_Twelve_Month_Revenue_Capture_Pct()
