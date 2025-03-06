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
    tags=["Stg Mrr Dashboard", "redshift"],
    max_active_runs=1
)
def Stg_Mrr_Dashboard():
    Dm_Trial_Outcomes = ExternalTaskSensor(
        task_id="wait_for_Dm_Trial_Outcomes",
        external_dag_id="Dm_Trial_Outcomes",
        external_task_id=None,
        mode="poke",
        timeout=600,
    )

    @task
    def call_stg_mrr_dashboard():
        print("Calling stg_mrr_dashboard")
        proc_name = "analytics.glue_load_stg_mrr_dashboard()"
        call_procedure(proc_name=proc_name)

    ###############################
    Dm_Trial_Outcomes >> call_stg_mrr_dashboard()


dag = Stg_Mrr_Dashboard()
