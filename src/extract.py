import requests
import os
from google.cloud import storage

# kaggle credentials
USERNAME = "mayankjain9797"
KEY = "9020d4ca4dfc915031630790995c05af"

# GCS details to store the extract files
BUCKET_NAME = "bucket"
GCS_FOLDER = "spotify_raw"  # where in the bucket to store

# harccoded Dataset and file name
dataset = "joebeachcapital/spotify-dataset-2023"
files = [
    "spotify-albums_data_2023.csv",
    "spotify_tracks_data_2023.csv"
]

# temp dir
download_dir = "/tmp/spotify"
os.makedirs(download_dir, exist_ok=True)

# Auth and headers
auth = (KAGGLE_USERNAME, KAGGLE_KEY)
response = requests.get(kaggle_url, auth=auth) # use stream=True if files are large


# GCS client
client = storage.Client()
bucket = client.bucket(BUCKET_NAME)

# Download and upload loop
for file in files:
    kaggle_file_url = f"https://www.kaggle.com/api/v1/datasets/download/{dataset}/files/{file}"

    if response.status_code == 200:
        local_path = os.path.join(download_dir, file)
        with open(local_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=16 * 1024 * 1024): # taking chunk size as 16 mb
                f.write(chunk)
        print(f"Downloaded: {file}")

        # Upload to GCS
        blob = bucket.blob(f"{GCS_FOLDER}/{file}")
        blob.upload_from_filename(local_path)
        print(f"Uploaded to GCS: gs://{BUCKET_NAME}/{GCS_FOLDER}/{file}")
    else:
        print(f"Failed to download {file}: Status code {response.status_code}")
