import io
import os
import requests
import pandas as pd
from google.cloud import storage
from google.oauth2 import service_account

# Initialize GCS credentials
credentials = service_account.Credentials.from_service_account_file(
    "google_credentials.json"
)

init_url = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/"
BUCKET = "dtc-data-lake-bucket"
# Specify the folder name where you want to store the data
BUCKET_FOLDER = "module04_csv"  # You can change this to any folder name you want


def upload_to_gcs(bucket, object_name, local_file):
    """
    Upload a file to Google Cloud Storage in a specific folder
    """
    client = storage.Client(credentials=credentials)
    bucket = client.bucket(bucket)

    # Prepend the folder name to the object path
    object_path = f"{BUCKET_FOLDER}/{object_name}"
    blob = bucket.blob(object_path)
    blob.upload_from_filename(local_file)


# services = ['fhv','green','yellow']
def web_to_gcs(year, service):
    for i in range(12):
        # sets the month part of the file_name string
        month = "0" + str(i + 1)
        month = month[-2:]

        # csv file_name
        file_name = f"{service}_tripdata_{year}-{month}.csv.gz"

        # download it using requests via a pandas df
        request_url = f"{init_url}{service}/{file_name}"
        r = requests.get(request_url)
        open(file_name, "wb").write(r.content)
        print(f"Local: {file_name}")

        # Upload the csv.gz file directly to GCS
        upload_to_gcs(BUCKET, f"{service}/{file_name}", file_name)
        print(f"GCS: {BUCKET_FOLDER}/{service}/{file_name}")

        # Clean up local file
        os.remove(file_name)  # Remove the downloaded .csv.gz file


# Example usage
# web_to_gcs("2019", "green")
# web_to_gcs("2020", "green")
# web_to_gcs('2019', 'yellow')
# web_to_gcs('2020', 'yellow')
web_to_gcs('2019', 'fhv')
