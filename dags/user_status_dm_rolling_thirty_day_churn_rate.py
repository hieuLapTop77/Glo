from airflow.decorators import dag, task
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
    tags=["Dm Rolling Thirty Day Churn Rate", "redshift"],
    max_active_runs=1
)
def Dm_Rolling_Thirty_Day_Churn_Rate():

    @task
    def call_dm_rolling_thirty_day_churn_rate():
        print("Calling dm_rolling_thirty_day_churn_rate")
        proc_name = "analytics.glue_load_dm_rolling_thirty_day_churn_rate()"
        call_procedure(proc_name=proc_name)

    ###############################
    call_dm_rolling_thirty_day_churn_rate()


dag = Dm_Rolling_Thirty_Day_Churn_Rate()
