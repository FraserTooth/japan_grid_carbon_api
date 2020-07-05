from tepco_scraper import generateTEPCOJSON, generateTEPCOCsv
from google.cloud import storage


def tepco_scraper(request):
    csvText = generateTEPCOCsv()
    request_json = request.get_json()
    BUCKET_NAME = 'scraper_data'
    BLOB_NAME = 'tepco_historical_data.csv'

    _upload_blob(BUCKET_NAME, csvText, BLOB_NAME)
    return f'Success!'


def _upload_blob(bucket_name, blob_text, destination_blob_name):
    """Uploads a file to the bucket."""
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_string(blob_text)

    print('Scraped Data Uploaded to {}.'.format(
        destination_blob_name))
