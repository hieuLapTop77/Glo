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
    schedule_interval="0 16 * * *",
    start_date=days_ago(1),
    catchup=False,
    tags=["Dm Marketing User Summary", "redshift"],
    max_active_runs=1
)
def Dm_Marketing_User_Summary():
    Apple_Event_History = TriggerDagRunOperator(
        task_id="trigger_Apple_Event_History",
        trigger_dag_id="Apple_Event_History",
        wait_for_completion=True
    )
    Apple_User_Paid_Months = TriggerDagRunOperator(
        task_id="trigger_Apple_User_Paid_Months",
        trigger_dag_id="Apple_User_Paid_Months",
        wait_for_completion=True
    )
    ################################################
    Stg_Consolidated_Stripe_Data = TriggerDagRunOperator(
        task_id="trigger_Stg_Consolidated_Stripe_Data",
        trigger_dag_id="Stg_Consolidated_Stripe_Data",
        wait_for_completion=True
    )
    Stripe_Trialing_Status = TriggerDagRunOperator(
        task_id="trigger_Stripe_Trialing_Status",
        trigger_dag_id="Stripe_Trialing_Status",
        wait_for_completion=True
    )
    Stripe_Active_Status = TriggerDagRunOperator(
        task_id="trigger_Stripe_Active_Status",
        trigger_dag_id="Stripe_Active_Status",
        wait_for_completion=True
    )
    Stripe_Active_Without_Invoice_Status = TriggerDagRunOperator(
        task_id="trigger_Stripe_Active_Without_Invoice_Status",
        trigger_dag_id="Stripe_Active_Without_Invoice_Status",
        wait_for_completion=True
    )
    Stripe_Free_Time_Status = TriggerDagRunOperator(
        task_id="trigger_Stripe_Free_Time_Status",
        trigger_dag_id="Stripe_Free_Time_Status",
        wait_for_completion=True
    )
    Stripe_on_Hold_Status = TriggerDagRunOperator(
        task_id="trigger_Stripe_on_Hold_Status",
        trigger_dag_id="Stripe_on_Hold_Status",
        wait_for_completion=True
    )
    Stripe_Past_Due_Status = TriggerDagRunOperator(
        task_id="trigger_Stripe_Past_Due_Status",
        trigger_dag_id="Stripe_Past_Due_Status",
        wait_for_completion=True
    )
    Stripe_Canceled_Status = TriggerDagRunOperator(
        task_id="trigger_Stripe_Canceled_Status",
        trigger_dag_id="Stripe_Canceled_Status",
        wait_for_completion=True
    )
    Stripe_Unpaid_Status = TriggerDagRunOperator(
        task_id="trigger_Stripe_Unpaid_Status",
        trigger_dag_id="Stripe_Unpaid_Status",
        wait_for_completion=True
    )
    Stripe_Unclear_Status = TriggerDagRunOperator(
        task_id="trigger_Stripe_Unclear_Status",
        trigger_dag_id="Stripe_Unclear_Status",
        wait_for_completion=True
    )
    Dm_Apple_Daily_Summary = TriggerDagRunOperator(
        task_id="trigger_Dm_Apple_Daily_Summary",
        trigger_dag_id="Dm_Apple_Daily_Summary",
        wait_for_completion=True
    )
    Stripe_Event_History = TriggerDagRunOperator(
        task_id="trigger_Stripe_Event_History",
        trigger_dag_id="Stripe_Event_History",
        wait_for_completion=True
    )
    Dim_Glo_Subscription_Id = TriggerDagRunOperator(
        task_id="trigger_Dim_Glo_Subscription_Id",
        trigger_dag_id="Dim_Glo_Subscription_Id",
        wait_for_completion=True
    )
    Subscription_Status = TriggerDagRunOperator(
        task_id="trigger_Subscription_Status",
        trigger_dag_id="Subscription_Status",
        wait_for_completion=True
    )
    Stripe_Status_Qa_Suite = TriggerDagRunOperator(
        task_id="trigger_Stripe_Status_Qa_Suite",
        trigger_dag_id="Stripe_Status_Qa_Suite",
        wait_for_completion=True
    )
    Last_Value_Union = TriggerDagRunOperator(
        task_id="trigger_Last_Value_Union",
        trigger_dag_id="Last_Value_Union",
        wait_for_completion=True
    )
    Dm_Agg_Subscription_Status_Counts_by_Day = TriggerDagRunOperator(
        task_id="trigger_Dm_Agg_Subscription_Status_Counts_by_Day",
        trigger_dag_id="Dm_Agg_Subscription_Status_Counts_by_Day",
        wait_for_completion=True
    )
    Dm_Churn_Rate_Segmentation = TriggerDagRunOperator(
        task_id="trigger_Dm_Churn_Rate_Segmentations",
        trigger_dag_id="Dm_Churn_Rate_Segmentation",
        wait_for_completion=True
    )
    Dm_Rolling_Thirty_Day_Churn_Rate = TriggerDagRunOperator(
        task_id="trigger_Dm_Rolling_Thirty_Day_Churn_Rate",
        trigger_dag_id="Dm_Rolling_Thirty_Day_Churn_Rate",
        wait_for_completion=True
    )
    Dm_Rolling_Thirty_Day_Trial_Conversion_Rate = TriggerDagRunOperator(
        task_id="trigger_Dm_Rolling_Thirty_Day_Trial_Conversion_Rate",
        trigger_dag_id="Dm_Rolling_Thirty_Day_Trial_Conversion_Rate",
        wait_for_completion=True
    )
    Dm_Twelve_Month_Revenue_Capture_Pct = TriggerDagRunOperator(
        task_id="trigger_Dm_Twelve_Month_Revenue_Capture_Pct",
        trigger_dag_id="Dm_Twelve_Month_Revenue_Capture_Pct",
        wait_for_completion=True
    )
    Dm_Engagement_Month_Dashboard = TriggerDagRunOperator(
        task_id="trigger_Dm_Engagement_Month_Dashboard",
        trigger_dag_id="Dm_Engagement_Month_Dashboard",
        wait_for_completion=True
    )

    Dm_Trial_Outcomes = TriggerDagRunOperator(
        task_id="trigger_Dm_Trial_Outcomes",
        trigger_dag_id="Dm_Trial_Outcomes",
        wait_for_completion=True
    )
    Stg_Mrr_Dashboard = TriggerDagRunOperator(
        task_id="trigger_Stg_Mrr_Dashboard",
        trigger_dag_id="Stg_Mrr_Dashboard",
        wait_for_completion=True
    )
    Stg_Subscription_Subscriber_Transitions = TriggerDagRunOperator(
        task_id="trigger_Stg_Subscription_Subscriber_Transitions",
        trigger_dag_id="Stg_Subscription_Subscriber_Transitions",
        wait_for_completion=True
    )

    @task
    def call_dm_marketing_user_summary():
        print("Calling dm_marketing_user_summary")
        proc_name = "analytics.glue_load_dm_marketing_user_summary()"
        call_procedure(proc_name=proc_name)

    ###############################
    Stg_Consolidated_Stripe_Data >> Stripe_Trialing_Status >> Stripe_Active_Status >> Stripe_Active_Without_Invoice_Status >> Stripe_Free_Time_Status >> Stripe_on_Hold_Status >> Stripe_Past_Due_Status >> Stripe_Canceled_Status >> Stripe_Unpaid_Status >> Stripe_Unclear_Status >> Stripe_Event_History
    Apple_Event_History >> Apple_User_Paid_Months >> Dm_Apple_Daily_Summary
    [Stripe_Event_History, Dm_Apple_Daily_Summary] >> Dim_Glo_Subscription_Id >> Subscription_Status >> Stripe_Status_Qa_Suite >> Last_Value_Union >> Dm_Agg_Subscription_Status_Counts_by_Day >> Dm_Churn_Rate_Segmentation >> Dm_Rolling_Thirty_Day_Churn_Rate >> Dm_Rolling_Thirty_Day_Trial_Conversion_Rate >> Dm_Twelve_Month_Revenue_Capture_Pct >> Dm_Engagement_Month_Dashboard >> Dm_Trial_Outcomes >> Stg_Mrr_Dashboard >> Stg_Subscription_Subscriber_Transitions >> call_dm_marketing_user_summary()


dag = Dm_Marketing_User_Summary()
