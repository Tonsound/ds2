import streamlit as st
import boto3
import pandas as pd
import time
from datetime import datetime, timedelta

region_name= 'us-east-1'
region_name =  'us-east-1'
output_bucket = 's3://br-dm-prod-us-east-1-837538682169-athena/'
database_tracing = 'br_dm_prod_bigdata_analytics_tracing_db'
table_jobs = 'gluejob_logs'

athena_client = boto3.client('athena', region_name=region_name)
iam_client = boto3.client('iam', region_name=region_name)
client_lf = boto3.client('lakeformation', region_name=region_name)
glue_client = boto3.client('glue', region_name=region_name)

corte = datetime.now().replace(day=1) - timedelta(days=120)
corte_str = corte.strftime('%Y-%m-%d')


def execute_query(query, database, output):
    response = athena_client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={'Database': database},
        ResultConfiguration={'OutputLocation': output}
    )
    # Get query execution ID
    query_execution_id = response['QueryExecutionId']
    # Poll until query execution is complete
    while True:
        query_execution = athena_client.get_query_execution(QueryExecutionId=query_execution_id)
        status = query_execution['QueryExecution']['Status']['State']
        if status in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
            break
        time.sleep(1)  # Wait for 1 second before checking again
    if status == 'SUCCEEDED':
        # Get results
        results = athena_client.get_query_results(QueryExecutionId=query_execution_id)
        # Parse columns
        columns = [column['VarCharValue'] for column in results['ResultSet']['Rows'][0]['Data']]
        # Parse data
        data = []
        for row in results['ResultSet']['Rows'][1:]:  # skip the header row
            values = []
            for column in row['Data']:
                if 'VarCharValue' in column:
                    values.append(column['VarCharValue'])
                elif 'BigIntValue' in column:
                    values.append(column['BigIntValue'])
                elif 'DoubleValue' in column:
                    values.append(column['DoubleValue'])
                else:
                    values.append(None)  # or handle the unknown types as needed
            data.append(values)
        # Create DataFrame
        df = pd.DataFrame(data, columns=columns)
        return df
    else:
        results = athena_client.get_query_results(QueryExecutionId=query_execution_id)
        print(results)
        raise Exception("Query execution failed.")
# def execute_query(query, database, output):
#     response = athena_client.start_query_execution(
#         QueryString=query,
#         QueryExecutionContext={'Database': database},
#         ResultConfiguration={'OutputLocation': output})
#     # Get query execution ID
#     query_execution_id = response['QueryExecutionId']
#     # Poll until query execution is complete
#     while True:
#         query_execution = athena_client.get_query_execution(QueryExecutionId=query_execution_id)
#         status = query_execution['QueryExecution']['Status']['State']
#         if status in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
#             break
#         time.sleep(1)  # Wait for 1 second before checking again
#     if status == 'SUCCEEDED':
#         # Get results
#         results = athena_client.get_query_results(QueryExecutionId=query_execution_id)
#         # Parse columns
#         columns = [column['VarCharValue'] for column in results['ResultSet']['Rows'][0]['Data']]
#         # Parse data
#         data = []
#         for row in results['ResultSet']['Rows'][1:]:  # skip the header row
#             values = [column['VarCharValue'] for column in row['Data']]
#             data.append(values)
#         # Create DataFrame
#         df = pd.DataFrame(data, columns=columns)
#         return df
#     else:
#         results = athena_client.get_query_results(QueryExecutionId=query_execution_id)
#         print(results)
#         raise Exception("Query execution failed.")

st.title('Info sobre Gluejobs')

st.subheader('Filtros')
tab1, tab2, tab3 = st.tabs(['Jobs Fallados',  '📋 Costos', 'Recientes'])

data_load_state = st.text('Cargando info...')
print(corte_str)
query = f"SELECT * FROM {database_tracing}.{table_jobs} WHERE startedon >= DATE('{corte_str}')"
print(query)
 
glue_jobs_data = execute_query(query, database_tracing, output_bucket)
glue_jobs_data = glue_jobs_data.sort_values(by='startedon', ascending=False)
print('semi ok')
gluejobs_fallados = glue_jobs_data[glue_jobs_data['jobrunstate']=='FAILED']
 
data_load_state = st.text("Listo!")

with tab1:
    page_number = st.number_input('¿Cuantos quieres ver?', min_value=1, max_value=len(glue_jobs_data), step=1, value=40)
    st.table(gluejobs_fallados[0:page_number][['startedon', 'jobname', 'id', 'cost']])
with tab3:
    page_number2 = st.number_input('¿Cuantos quieres ver?', min_value=1, max_value=len(glue_jobs_data), step=1, value=40)
    st.table(glue_jobs_data[0:page_number2][['startedon', 'jobname', 'id', 'cost']])