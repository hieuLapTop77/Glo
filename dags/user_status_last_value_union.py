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
    tags=["Last Value Union", "redshift"],
    max_active_runs=1
)
def Last_Value_Union():

    @task
    def call_last_value_union():
        print("Calling last_value_union")
        proc_name = "analytics.load_last_value_union()"
        call_procedure(proc_name=proc_name)

    ###############################
    call_last_value_union()


dag = Last_Value_Union()
