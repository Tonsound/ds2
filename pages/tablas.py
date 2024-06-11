import streamlit as st
import os
import boto3
import time
import pandas as pd




st.title('Información sobre Tablas') #, br_dm_prod_bigdata_reporitf_db-MPDT012

region_name =  'us-east-1'
output_bucket = 's3://br-dm-prod-us-east-1-837538682169-athena/'

# And the root-level secrets are also accessible as environment variables:




# Athena configuration
database_tracing = 'br_dm_prod_bigdata_analytics_tracing_db'
load_info = 'load_info'
sp_info = "sp_retroactive_dependencies"
data_quality = 'lake_data_quality'
linages = 'sp_retroactive_dependencies'
permissions = ''


prod = True

if prod == True:
    athena_client = boto3.client('athena', region_name=region_name)
    client_lf = boto3.client('lakeformation', region_name=region_name)
else:
    aws_access_key_id = st.secrets["AWS_ACCESS_KEY_ID"]
    aws_secret_access_key = st.secrets["AWS_SECRET_ACCESS_KEY"]
    aws_session_token = st.secrets["AWS_SESSION_TOKEN"] 
    athena_client = boto3.client('athena', aws_access_key_id=aws_access_key_id,
                             aws_secret_access_key=aws_secret_access_key,aws_session_token=aws_session_token,
                             region_name=region_name)
    iam_client = boto3.client('iam', aws_access_key_id=aws_access_key_id,
                              aws_secret_access_key=aws_secret_access_key,aws_session_token=aws_session_token)
    client_lf = boto3.client('lakeformation', aws_access_key_id=aws_access_key_id,
                             aws_secret_access_key=aws_secret_access_key,aws_session_token=aws_session_token,
                             region_name=region_name)





def execute_query(query):
    response = athena_client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={'Database': database_tracing},
        ResultConfiguration={'OutputLocation': output_bucket}
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
            values = [column['VarCharValue'] for column in row['Data']]
            data.append(values)

        # Create DataFrame
        df = pd.DataFrame(data, columns=columns)
        return df
    else:
        results = athena_client.get_query_results(QueryExecutionId=query_execution_id)
        print(results)
        raise Exception("Query execution failed.")

@st.cache_data
def get_update_from_athena(table_data):
    print(table_data)
    table_name = table_data.split('-')[1]
    query_load = f"SELECT load_timestamp FROM {database_tracing}.{load_info } where \"table\"='{table_name.lower()}' order by load_timestamp desc limit 1"

    load_data = execute_query(query_load)

    return load_data

@st.cache_data
def get_permissions_from_lf(table_data):
    print(table_data)
    table_name = table_data.split('-')[1]
    database_name = table_data.split('-')[0]
    response_p = client_lf.list_permissions(ResourceType='TABLE', Resource={"Table": {"DatabaseName": database_name , "Name":table_name}})
    permisos = []
    a = 0
    for i in range(len(response_p['PrincipalResourcePermissions'])):
        permisos.append(response_p['PrincipalResourcePermissions'][i]['Principal']['DataLakePrincipalIdentifier'])

    while a == 0:
        try:
            token = response_p['NextToken']
            response_p = client_lf.list_permissions(ResourceType='TABLE', Resource={"Table": {"DatabaseName": database_name , "Name":table_name}}, NextToken= token)
            for i in range(len(response_p['PrincipalResourcePermissions'])):
                permisos.append(response_p['PrincipalResourcePermissions'][i]['Principal']['DataLakePrincipalIdentifier'])
        except:
            a = 1
            
    permisos = list(set(permisos))
    return permisos


@st.cache_data
def get_sp_from_athena(table_data):
    print(table_data)
    table_name = table_data.split('-')[1]
    query_sp = f"SELECT sp FROM {database_tracing}.{sp_info} where \"table\"='{table_name.lower()}' "
    sp_data = execute_query(query_sp)

    print(sp_data)

    return sp_data


table_data = st.text_input('base-tabla')


def rol_page_switch_state(rol_update):
    st.session_state['rol'] = rol_update
    st.switch_page('pages/roles.py')



if table_data:

    
    data_load_state = st.text('Cargando data permisos...')

    permisos = get_permissions_from_lf(table_data)

    data_load_state.text("Listo! (usando st.cache_data)")

    if st.checkbox('Mostrar'):
        st.subheader('Listado de Roles con acceso a la tabla')
        for i in permisos:
            with st.container(height=110, border=None):
                if st.button(label=i, key=i, use_container_width=True):
                    rol_page_switch_state(i)

            
    
    data_load_state = st.text('Cargando data sp...')

    sp_data = get_sp_from_athena(table_data)

    data_load_state.text("Listo! (usando st.cache_data)")


    if st.checkbox('Mostrar data'):
        st.subheader('Listado')
        if len(sp_data['sp']) > 0:
            for i in sp_data['sp']:
                st.markdown("- " + i)
        else:
            st.write("No depende ningún SP de esta tabla")

    data_load_state = st.text('Cargando data actualizacion...')

    load_data = get_update_from_athena(table_data)
    data_load_state.text("Listo! (usando st.cache_data)")
    print(load_data)
    st.write(f"La ultima vez que se actualizó fue: {load_data.to_dict()['load_timestamp'][0]}")


