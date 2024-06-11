import streamlit as st
import boto3



region_name =  'us-east-1'
output_bucket = 's3://br-dm-prod-us-east-1-837538682169-athena/'

rol_prueba = 'arn:aws:iam::837538682169:role/ROLE-BR-PROD-COBRANZAS-DATACATALOG-REDSHIFT'


 




st.title('Info sobre Roles')

if 'rol' in st.session_state:
    print('there is rol')
    rol_data = st.session_state['rol']

rol_data = st.text_input('Rol', key='rol')


prod = True

if prod == True:
    athena_client = boto3.client('athena', region_name=region_name)
    iam_client = boto3.client('iam')
    client_lf = boto3.client('lakeformation')
    glue_client = boto3.client('glue')
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


def get_tables(rol):
    response = glue_client.get_databases()
    final_data = {}

    for database in response['DatabaseList']:
        tablas = glue_client.get_tables(DatabaseName=database['Name'])
        listado = []
        a = 0
        while a == 0:
            for element in tablas['TableList']:
                listado.append(element['Name'])
                result = client_lf.list_permissions(Principal={'DataLakePrincipalIdentifier': rol}, ResourceType='TABLE', Resource={"Table": {"DatabaseName": database['Name'], "Name":element['Name']}})
                try:
                    print(database['Name'], element['Name'], result['PrincipalResourcePermissions'][0]['Permissions'])
                except:
                    pass
            try:
                tablas = glue_client.get_tables(DatabaseName=database['Name'], NextToken=tablas['NextToken'])
            except:
                a = 1
    final_data[database['Name']] = listado
    return final_data

def get_databases_full(rol):
    response = glue_client.get_databases()
    listado = []
    permisos = []
    for database in response['DatabaseList']:
        result = client_lf.list_permissions(Principal={'DataLakePrincipalIdentifier': rol}, ResourceType='DATABASE', Resource={"Database": {"Name":database['Name']}})
        try:
            print(database['Name'], result['PrincipalResourcePermissions'][0]['Permissions'])
            listado.append(database['Name'])
            permisos.append(result['PrincipalResourcePermissions'][0]['Permissions'])
        except:
            pass
    return listado, permisos

if rol_data:
    print(rol_data)

    ddbbs, permisos = get_databases_full(rol_data)
    tab1, tab2 = st.tabs(["General", "Detalle",])
    with tab1:
        for i in range(len(ddbbs)):
            if len(permisos[i])> 0:
                col1, col2 = st.columns(2)
                print(ddbbs[i])
                print(permisos[i])
                with col1:
                    st.markdown("- " + ddbbs[i])
                with col2:
                    for element in permisos[i]:
                        st.markdown("- " + element)
    with tab2:

        permisos = get_tables(rol_data)

        for element in permisos:
            st.markdown("- " + element)
