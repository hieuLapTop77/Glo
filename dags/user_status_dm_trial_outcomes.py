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
    tags=["Dm Trial Outcomes", "redshift"],
    max_active_runs=1
)
def Dm_Trial_Outcomes():
    Dm_Engagement_Month_Dashboard = ExternalTaskSensor(
        task_id="wait_for_Dm_Engagement_Month_Dashboard",
        external_dag_id="Dm_Engagement_Month_Dashboard",
        external_task_id=None,
        mode="poke",
        timeout=600,
    )

    @task
    def call_dm_trial_outcomes():
        print("Calling dm_trial_outcomes")
        proc_name = "analytics.glue_load_dm_trial_outcomes()"
        call_procedure(proc_name=proc_name)

    ###############################
    Dm_Engagement_Month_Dashboard >> call_dm_trial_outcomes()


dag = Dm_Trial_Outcomes()
