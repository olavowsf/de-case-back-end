import pytest
from src.database import transform, create_table, create_dataset
from google.cloud import bigquery
from google.oauth2 import service_account

key_path = "playground-olavo-387508-29715e94163b.json"

def test_transform():
    response = {'time_stamp': '2019-05-01T06:00:00-04:00', 'data': [1, 2, 3, 4, 5]}

    expected_output = [{'time_stamp': '2019-05-01 10:00:00+00:00', 'data': '[3, 1.5811388300841898]'}]

    result = transform(response)
    assert result == expected_output 

@pytest.fixture(scope="module")
def test_table():
    # Setup: Create a test table
    dataset_id, table_id = create_table("test_dataset", "test_table")

    # Provide the dataset ID & table ID to the test case
    yield table_id

    # Teardown: Delete the test table
    credentials = service_account.Credentials.from_service_account_file(
    key_path, scopes=["https://www.googleapis.com/auth/cloud-platform"])

    client = bigquery.Client(credentials=credentials, project=credentials.project_id)

    table_ref = client.get_table(table_id)
    dataset_ref = client.get_dataset(dataset_id)
    
    client.delete_table(table_ref)
    client.delete_dataset(dataset_ref)

def test_create_table(test_table):
    # Test: Check if the table exists
    credentials = service_account.Credentials.from_service_account_file(
    key_path, scopes=["https://www.googleapis.com/auth/cloud-platform"])

    client = bigquery.Client(credentials=credentials, project=credentials.project_id)    
    
    table_ref = client.get_table(test_table)
    assert table_ref.table_id == "test_table"
    assert table_ref.dataset_id == "test_dataset"
    assert table_ref.project == client.project
    