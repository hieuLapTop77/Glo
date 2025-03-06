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
    schedule_interval="0 16 * * *",
    start_date=days_ago(1),
    catchup=False,
    tags=["Stg Consolidated Stripe Data", "redshift"],
    max_active_runs=1
)
def Stg_Consolidated_Stripe_Data():

    @task
    def call_stg_consolidated_stripe_data():
        print("Calling stg_consolidated_stripe_data")
        proc_name = "analytics.glue_load_stg_consolidated_stripe_data()"
        call_procedure(proc_name=proc_name)

    ###############################
    call_stg_consolidated_stripe_data()


dag = Stg_Consolidated_Stripe_Data()
