import os
import re
from datetime import datetime, date
import pandas as pd
import nums_from_string
from io import StringIO
from google.cloud import bigquery
from google.cloud.storage import Client as storage_client

my_bucket = 'bucket-gcp-challenge'


def create_dataset_and_tables(data, context):
    today = date.today()
    client = bigquery.Client()
    dataset_name = format(data['name'])
    file_name = format(data['name'])
    str_date = str(today)
    dataset_name_only = str_date + os.path.splitext(dataset_name)[0]
    dataset_name_only_clear = re.sub(r'[^a-zA-Z0-9_\s]+', '', dataset_name_only)
    dataset_name_only = dataset_name_only_clear[0:16]
    print("name file:----------------------------------- " + file_name)
    print("name dataset:----------------------------------- " + dataset_name_only)
    dataset_id = "{}.{}".format(client.project, dataset_name_only)
    dataset = bigquery.Dataset(dataset_id)
    dataset.location = "us-central1"
    dataset = client.create_dataset(dataset=dataset, exists_ok=True)
    print("Created dataset {}.{}".format(client.project, dataset.dataset_id))

    # run function to move data from bucket to dataset
    __move_bucket_to_data_query(client, dataset_name_only, file_name)
    # run function to move data from bucket to dataset
    print("data go to table Big query")


def __move_bucket_to_data_query(client, dataset_name_only, file_name):
    print("dataset_name_only" + dataset_name_only)
    dataset = dataset_name_only
    dataset_ref = client.dataset(dataset)
    job_config = bigquery.LoadJobConfig()
    file_name_clean = file_name.split("-")[2][0:-4]
    if file_name_clean == 'Compras':
        job_config.schema = [
            bigquery.SchemaField("Id", "INTEGER"),
            bigquery.SchemaField("Cust_id", "INTEGER"),
            bigquery.SchemaField("Prod_id", "INTEGER"),
            bigquery.SchemaField("Gasto", "INTEGER"),
            bigquery.SchemaField("Fecha_Compra", "DATE"),
            bigquery.SchemaField("Medio_Pago", "STRING"),
        ]
    elif file_name_clean == 'Clientes':
        job_config.schema = [
            bigquery.SchemaField("Id", "INTEGER"),
            bigquery.SchemaField("Nombre", "STRING"),
            bigquery.SchemaField("Pais", "STRING"),
            bigquery.SchemaField("Edad", "INTEGER"),
            bigquery.SchemaField("Ocupacion", "STRING"),
            bigquery.SchemaField("Score", "INTEGER"),
            bigquery.SchemaField("Salario_net_USD", "INTEGER"),
            bigquery.SchemaField("Estado_Civil", "STRING"),
            bigquery.SchemaField("Estado_1_Activo", "STRING"),
            bigquery.SchemaField("Fecha_Inactividad", "DATETIME"),
            bigquery.SchemaField("Genero", "STRING"),
            bigquery.SchemaField("Device", "STRING"),
            bigquery.SchemaField("Nivel_Educativo", "STRING"),
            bigquery.SchemaField("Carrera", "STRING"),
        ]
    elif file_name_clean == 'Producto':
        job_config.schema = [
            bigquery.SchemaField("Id", "INTEGER"),
            bigquery.SchemaField("Nombre", "STRING"),
            bigquery.SchemaField("Valor_USD", "INTEGER"),
            bigquery.SchemaField("Cantidad_Datos_MB", "INTEGER"),
            bigquery.SchemaField("Vigencia_Dias", "INTEGER"),
            bigquery.SchemaField("Telefonia", "STRING"),
        ]
    job_config.skip_leading_rows = 1
    job_config.source_format = bigquery.SourceFormat.CSV
    job_config.write_disposition = 'WRITE_TRUNCATE'
    # uri = f'gs://{my_bucket}/' + str(file_name)

    print('before get bucket')
    df = __get_csv_bucket(my_bucket, file_name)

    df = clean_dataframe(df, file_name_clean)

    print(f'el dataframe despues de limpiarlo {df.head()}')

    table_ref = dataset_ref.table(file_name_clean)

    load_job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    # load_job = client.load_table_from_uri(
    #     uri, dataset_ref.table(file_name_clean), job_config=job_config
    # )

    print("Starting job {}".format(load_job.job_id))

    load_job.result()
    print("Job finished.")

    destination_table = client.get_table(dataset_ref.table(file_name_clean))
    print("Loaded {} rows.".format(destination_table.num_rows))


def clean_dataframe(df: pd.DataFrame, obj: str) -> pd.DataFrame:
    if obj == 'Compras':
        df = df.rename(columns={"id": "Id",
                                "cust_id": "Cust_id",
                                "prod_id": "Prod_id",
                                "Gasto": "Gasto",
                                "FechaCompra": "Fecha_Compra",
                                "Mediopago (Tarjeta o Cash)": "Medio_Pago",
                                })

        df['Id'] = df['Id'].astype(str).apply(__validate_number)
        df['Cust_id'] = df['Cust_id'].fillna(0).astype(str).apply(__validate_number)
        df['Prod_id'] = df['Prod_id'].fillna(0).astype(str).apply(__validate_number)
        df['Gasto'] = df['Gasto'].fillna(0).astype(str).apply(__validate_number)
        df['Fecha_Compra'] = df['Fecha_Compra'].astype(str).apply(__validate_date)
        df['Medio_Pago'] = df['Medio_Pago'].fillna('Missing').astype(str).apply(__alphanum)

    elif obj == 'Clientes':
        df = df.rename(columns={"id": "Id",
                                "Nombre": "Nombre",
                                "Pais": "Pais",
                                "edad": "Edad",
                                "Ocupacion": "Ocupacion",
                                "Score": "Score",
                                "Salario net USD": "Salario_net_USD",
                                "Estado Civil": "Estado_Civil",
                                "Estado (1 activo)": "Estado_1_Activo",
                                "Fecha Inactividad": "Fecha_Inactividad",
                                "Genero": "Genero",
                                "Device": "Device",
                                "Nivel Educativo": "Nivel_Educativo",
                                "Carrera": "Carrera",
                                })
        df['Id'] = df['Id'].astype(str).apply(__validate_number)
        df['Nombre'] = df['Nombre'].fillna('Missing').astype(str).apply(__alphanum)
        df['Pais'] = df['Pais'].fillna('Missing').astype(str).apply(__alphanum)
        df['Edad'] = df['Edad'].fillna('Missing').astype(str).apply(__validate_number)
        df['Ocupacion'] = df['Ocupacion'].fillna('Missing').astype(str).apply(__alphanum).apply(__validate_ocupacion)
        df['Score'] = df['Score'].fillna(0).astype(str).apply(__validate_number)
        df['Salario_net_USD'] = df['Salario_net_USD'].fillna(0).astype(str).apply(__validate_number)
        df['Estado_Civil'] = df['Estado_Civil'].fillna('Missing').astype(str).apply(__alphanum).apply(
            __validate_estado_civil)
        df['Estado_1_Activo'] = df['Estado_1_Activo'].fillna('Missing').astype(str).apply(__alphanum)
        df['Fecha_Inactividad'] = df['Fecha_Inactividad'].fillna('01/01/1900').astype(str).apply(__validate_date)
        df['Genero'] = df['Genero'].fillna('Missing').astype(str).apply(__alphanum).apply(__validate_genero)
        df['Device'] = df['Device'].fillna('Missing').astype(str).apply(__alphanum)
        df['Nivel_Educativo'] = df['Nivel_Educativo'].fillna('Missing').astype(str).apply(__alphanum)
        df['Carrera'] = df['Carrera'].fillna('Missing').astype(str).apply(__alphanum)
    elif obj == 'Producto':
        df = df.rename(columns={"id": "Id",
                                "nombre": "Nombre",
                                "ValorUSD": "Valor_USD",
                                "Cantidad Datos MB": "Cantidad_Datos_MB",
                                "Vigencia (dias)": "Vigencia_Dias"})

        df['Id'] = df['Id'].astype(str).apply(__validate_number)
        df['Nombre'] = df['Nombre'].fillna('Missing').astype(str).apply(__alphanum)
        df['Valor_USD'] = df['Valor_USD'].fillna(0).astype(str).apply(__validate_number)
        df['Cantidad_Datos_MB'] = df['Cantidad_Datos_MB'].fillna(0).astype(str).apply(__validate_number)
        df['Vigencia_Dias'] = df['Vigencia_Dias'].fillna(0).astype(str).apply(__validate_number)
        df['Telefonia'] = df['Telefonia'].fillna('Missing').astype(str).apply(__alphanum)
    return df


def __get_csv_bucket(path, sfile):
    print(':-------->' + sfile)
    client = storage_client()
    bucket = client.get_bucket(my_bucket)
    blob = bucket.get_blob(sfile)

    # Download the contents of the CSV file as a string
    csv_string = blob.download_as_string()

    # Convert the CSV string to a Pandas DataFrame
    return pd.read_csv(StringIO(csv_string.decode('utf-8')))


def __alphanum(element):
    try:
        if element is not None:
            return "".join(filter(str.isalnum, element)).title()
    except ValueError as e:
        print(f'Error convert alphanum String: {element}', e)


def __validate_number(element):
    if element is not None:
        return int(nums_from_string.get_nums(element)[0])


def __validate_date(date_text):
    try:
        if re.match("^\d{2}/\d{2}/\d{4}$", date_text):
            return datetime.strptime(datetime.strptime(date_text, "%d/%m/%Y").strftime('%Y/%m/%d'), "%Y/%m/%d").date()
        elif re.match("^\d{2}-\d{2}-\d{4}$", date_text):
            return datetime.strptime(datetime.strptime(date_text, "%d-%m-%Y").strftime('%Y/%m/%d'), "%Y/%m/%d").date()
        else:
            return datetime.strptime('1900/01/01', "%Y/%m/%d").date()
    except ValueError as e:
        raise e


def __validate_genero(element: str):
    try:
        gen = 'UNDEFINED'

        if len(element) >= 1:
            element = element[:1].upper()
            if element in genero:
                gen = element.upper()
        return gen
    except ValueError as e:
        raise e


def __validate_estado_civil(element: str):
    try:
        ec = 'UNDEFINED'
        if element.lower() in estado_civil:
            ec = element
        else:
            if re.search(r'\W*(cas)\W*', element.title()):
                ec = estado_civil[0]
            elif re.search(r'\W*(cas)\W*', element.title()):
                ec = estado_civil[1]
        return ec
    except ValueError as e:
        raise e


def __validate_ocupacion(element: str):
    try:
        ec = 'UNDEFINED'
        if element.lower() in ocupacion:
            ec = element
        else:
            if re.search(r'\W*(em)\W*', element.title()):
                ec = ocupacion[0]
            elif re.search(r'\W*(es)\W*', element.title()):
                ec = ocupacion[1]
            elif re.search(r'\W*(in)\W*', element.title()):
                ec = ocupacion[2]
        return ec
    except ValueError as e:
        raise e


ocupacion = ['empleado', 'estudiante','independiente']
estado_civil = ['soltero', 'casado']
genero = ['F', 'M']
