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
    tags=["Subscription Status", "redshift"],
    max_active_runs=1
)
def Subscription_Status():

    @task
    def call_subscription_status():
        print("Calling subscription_status")
        proc_name = "analytics.glue_load_subscription_status()"
        call_procedure(proc_name=proc_name)

    ###############################
    call_subscription_status()


dag = Subscription_Status()
