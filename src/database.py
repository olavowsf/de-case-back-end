from google.cloud import bigquery
from datetime import datetime
from statistics import mean, stdev
from pytz import timezone
from google.oauth2 import service_account


key_path = "playground-olavo-387508-29715e94163b.json"


def create_dataset(dataset_name: str):
    credentials = service_account.Credentials.from_service_account_file(
    key_path, scopes=["https://www.googleapis.com/auth/cloud-platform"])

    client = bigquery.Client(credentials=credentials, project=credentials.project_id)
    dataset_id = "{}.{}".format(client.project,dataset_name)
    
    dataset = bigquery.Dataset(dataset_id)
    dataset.location = "US"
    dataset_created = client.create_dataset(dataset, timeout=30, exists_ok=True)
    
    return dataset_id

def create_table(dataset_name: str, table_name: str):
    credentials = service_account.Credentials.from_service_account_file(
    key_path, scopes=["https://www.googleapis.com/auth/cloud-platform"])

    client = bigquery.Client(credentials=credentials, project=credentials.project_id)

    dataset_id = create_dataset(dataset_name)

    table_id = f"{dataset_id}.{table_name}"

    table_schema = {"name": "time_stamp", "type": "STRING", "mode": "REQUIRED"}, {
        "name": "data",
        "type": "STRING",
        "mode": "REQUIRED",
    }

    table = bigquery.Table(table_id, schema=table_schema)
    table = client.create_table(table, exists_ok=True)

    return dataset_id, table_id


def transform(response: dict):
    dt = datetime.fromisoformat(response["time_stamp"])
    utc_dt = dt.astimezone(timezone("UTC"))

    response["time_stamp"] = str(utc_dt)
    response["data"] = [mean(response["data"]), stdev(response["data"])]

    response["data"] = str(response["data"])
    data = []
    data.append(response)
    return data


def insert_row(dataset_name: str, table_name: str, response: list):
    credentials = service_account.Credentials.from_service_account_file(
    key_path, scopes=["https://www.googleapis.com/auth/cloud-platform"])

    client = bigquery.Client(credentials=credentials, project=credentials.project_id)
    
    table_id = f"{client.project}.{dataset_name}.{table_name}"

    errors = client.insert_rows_json(table_id, response)
    return errors