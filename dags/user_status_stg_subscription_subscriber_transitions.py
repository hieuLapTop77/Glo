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
    tags=["Stg Subscription Subscriber Transitions", "redshift"],
    max_active_runs=1
)
def Stg_Subscription_Subscriber_Transitions():

    @task
    def call_stg_subscription_subscriber_transitions():
        print("Calling stg_subscription_subscriber_transitions")
        proc_name = "analytics.glue_load_stg_subscription_subscriber_transitions()"
        call_procedure(proc_name=proc_name)

    ###############################
    call_stg_subscription_subscriber_transitions()


dag = Stg_Subscription_Subscriber_Transitions()
