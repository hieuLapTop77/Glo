from airflow.models import Variable

# Constanst Definitions
############################################## Connection ##############################################
GLO_REDSHIFT_DB = Variable.get('glo_redshift_db')
