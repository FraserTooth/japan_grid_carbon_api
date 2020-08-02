import tohokuden_scraper as ts
import json
from google.cloud import storage
from google.cloud import bigquery
from google.api_core import retry


def tohokuden_scraper(request):
    request_json = request.get_json(silent=True)

    print("Scraping:")
    df = ts.get_tohokuden_as_dataframe()
    print(" - Got Data")
    print("Converting:")

    print("Sending:")
    _upload_blob_to_storage(df)
    print(" - Sent to Cloud Storage")

    _insert_into_bigquery(df)
    print(" - Sent to BigQuery")
    return f'Success!'


def _upload_blob_to_storage(df):
    CS = storage.Client()
    BUCKET_NAME = 'scraper_data'
    BLOB_NAME = 'tohoku_historical_data.csv'

    """Uploads a file to the bucket."""
    bucket = CS.get_bucket(BUCKET_NAME)
    blob = bucket.blob(BLOB_NAME)

    blob.upload_from_string(ts.convert_tohokuden_df_to_csv(df))

    print('Scraped Data Uploaded to {}.'.format(BLOB_NAME))


def _insert_into_bigquery(df):
    BQ_DATASET = 'tepco'
    BQ_TABLE = 'historical_data_by_generation_type'

    table_id = BQ_DATASET + "." + BQ_TABLE

    df.to_gbq(table_id, if_exists="replace")


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
