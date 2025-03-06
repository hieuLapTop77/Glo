from airflow.providers.postgres.hooks.postgres import PostgresHook
from common.variables import GLO_REDSHIFT_DB

# REDSHIFT_CONNECTION
rs_hook = PostgresHook(postgres_conn_id=GLO_REDSHIFT_DB)