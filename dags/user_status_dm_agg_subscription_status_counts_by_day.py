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
    tags=["Dm Agg Subscription Status Counts by Day", "redshift"],
    max_active_runs=1
)
def Dm_Agg_Subscription_Status_Counts_by_Day():

    Last_Value_Union = TriggerDagRunOperator(
        task_id="trigger_Last_Value_Union",
        trigger_dag_id="Last_Value_Union",
        wait_for_completion=True
    )

    @task
    def call_glue_load_dm_agg_subscription_status_counts_by_day():
        print("Calling dm_agg_subscription_status_counts_by_day")
        proc_name = "analytics.glue_load_dm_agg_subscription_status_counts_by_day_new()"
        call_procedure(proc_name=proc_name)

    ###############################
    Last_Value_Union >> call_glue_load_dm_agg_subscription_status_counts_by_day()


dag = Dm_Agg_Subscription_Status_Counts_by_Day()
