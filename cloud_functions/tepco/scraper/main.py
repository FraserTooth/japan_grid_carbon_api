import tepco_scraper as ts
import json
from google.cloud import storage
from google.cloud import bigquery
from google.api_core import retry

BQ = bigquery.Client()
BQ_DATASET = 'japan-grid-carbon-api'
BQ_TABLE = 'tepco_data'

CS = storage.Client()
BUCKET_NAME = 'scraper_data'
BLOB_NAME = 'tepco_historical_data.csv'


def tepco_scraper(request):
    request_json = request.get_json()

    print("Scraping:")
    df = ts.get_tepco_as_dataframe()
    print(" - Got Data")
    print("Converting:")

    print("Sending:")
    _upload_blob_to_storage(BUCKET_NAME, df, BLOB_NAME)
    print(" - Sent to Cloud Storage")

    _insert_into_bigquery(df)
    print(" - Sent to BigQuery")
    return f'Success!'


def _upload_blob_to_storage(bucket_name, df, destination_blob_name):
    """Uploads a file to the bucket."""
    bucket = CS.get_bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_string(ts.convert_tepco_df_to_csv(df))

    print('Scraped Data Uploaded to {}.'.format(
        destination_blob_name))


def _insert_into_bigquery(df):
    table = BQ.dataset(BQ_DATASET).table(BQ_TABLE)
    errors = BQ.insert_rows_from_dataframe(table,
                                           json_rows=ts.convert_tepco_df_to_json(
                                               df),
                                           retry=retry.Retry(deadline=30))
    if errors != []:
        raise BigQueryError(errors)


class BigQueryError(Exception):
    '''Exception raised whenever a BigQuery error happened'''

    def __init__(self, errors):
        super().__init__(self._format(errors))
        self.errors = errors

    def _format(self, errors):
        err = []
        for error in errors:
            err.extend(error['errors'])
        return json.dumps(err)
