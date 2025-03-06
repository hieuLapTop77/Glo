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
    tags=["Dm Engagement Month Dashboard", "redshift"],
    max_active_runs=1
)
def Dm_Engagement_Month_Dashboard():

    @task
    def call_dm_engagement_month_dashboard():
        print("Calling dm_engagement_month_dashboard")
        proc_name = "analytics.glue_load_dm_engagement_month_dashboard()"
        call_procedure(proc_name=proc_name)

    ###############################
    call_dm_engagement_month_dashboard()


dag = Dm_Engagement_Month_Dashboard()
