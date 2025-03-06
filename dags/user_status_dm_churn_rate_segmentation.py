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
    tags=["Dm Churn Rate Segmentation", "redshift"],
    max_active_runs=1
)
def Dm_Churn_Rate_Segmentation():

    @task
    def call_dm_churn_rate_segmentation():
        print("Calling dm_churn_rate_segmentation")
        proc_name = "analytics.glue_load_dm_churn_rate_segmentation()"
        call_procedure(proc_name=proc_name)

    ###############################
    call_dm_churn_rate_segmentation()


dag = Dm_Churn_Rate_Segmentation()
