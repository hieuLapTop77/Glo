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
    tags=["Stripe Active Without Invoice Status", "redshift"],
    max_active_runs=1
)
def Stripe_Active_Without_Invoice_Status():

    @task
    def call_stripe_active_without_invoice_status():
        print("Calling stripe_active_without_invoice_status")
        proc_name = "analytics.glue_load_stripe_active_without_invoice_status()"
        call_procedure(proc_name=proc_name)

    ###############################
    call_stripe_active_without_invoice_status()


dag = Stripe_Active_Without_Invoice_Status()
