from airflow.decorators import dag, task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
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
    tags=["Stg Mrr Dashboard", "redshift"],
    max_active_runs=1
)
def Stg_Mrr_Dashboard():

    Dm_Trial_Outcomes = TriggerDagRunOperator(
        task_id="trigger_Dm_Trial_Outcomes",
        trigger_dag_id="Dm_Trial_Outcomes",
        wait_for_completion=True
    )

    @task
    def call_stg_mrr_dashboard():
        print("Calling stg_mrr_dashboard")
        proc_name = "analytics.glue_load_stg_mrr_dashboard()"
        call_procedure(proc_name=proc_name)

    ###############################
    Dm_Trial_Outcomes >> call_stg_mrr_dashboard()


dag = Stg_Mrr_Dashboard()
