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
    tags=["Stripe Status Qa Suite", "redshift"],
    max_active_runs=1
)
def Stripe_Status_Qa_Suite():

    @task
    def call_stripe_status_qa_suite():
        print("Calling stripe_status_qa_suite")
        proc_name = "analytics.glue_load_stripe_status_qa_suite()"
        call_procedure(proc_name=proc_name)

    ###############################
    call_stripe_status_qa_suite()


dag = Stripe_Status_Qa_Suite()
