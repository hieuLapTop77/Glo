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
    tags=["Apple Event History", "redshift"],
    max_active_runs=1
)
def Apple_Event_History():

    @task
    def call_apple_event_history():
        print("Calling apple_event_history")
        proc_name = "analytics.glue_load_apple_event_history()"
        call_procedure(proc_name=proc_name)

    ###############################
    call_apple_event_history()


dag = Apple_Event_History()
