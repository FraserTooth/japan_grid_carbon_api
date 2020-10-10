import json
import os
from datetime import datetime
from google.cloud import storage
from google.cloud import bigquery
from google.api_core import retry
from pydoc import locate

stage = os.environ['STAGE']

from scrapers.area_data.utilities.tepco.TepcoAreaScraper import TepcoAreaScraper
from scrapers.area_data.utilities.kepco.KepcoAreaScraper import KepcoAreaScraper
from scrapers.area_data.utilities.tohokuden.TohokudenAreaScraper import TohokudenAreaScraper
from scrapers.area_data.utilities.chuden.ChudenAreaScraper import ChudenAreaScraper
from scrapers.area_data.utilities.hepco.HepcoAreaScraper import HepcoAreaScraper
from scrapers.area_data.utilities.rikuden.RikudenAreaScraper import RikudenAreaScraper
from scrapers.area_data.utilities.cepco.CepcoAreaScraper import CepcoAreaScraper
from scrapers.area_data.utilities.yonden.YondenAreaScraper import YondenAreaScraper
from scrapers.area_data.utilities.kyuden.KyudenAreaScraper import KyudenAreaScraper
from scrapers.area_data.utilities.okiden.OkidenAreaScraper import OkidenAreaScraper


def selectUtility(utility):
    utilities = {
        "tepco": TepcoAreaScraper(),
        "tohokuden": TohokudenAreaScraper(),
        "kepco": KepcoAreaScraper(),
        "chuden": ChudenAreaScraper(),
        "hepco": HepcoAreaScraper(),
        "rikuden": RikudenAreaScraper(),
        "cepco": CepcoAreaScraper(),
        "yonden": YondenAreaScraper(),
        "kyuden": KyudenAreaScraper(),
        "okiden": OkidenAreaScraper(),
    }
    return utilities.get(utility, None)


class AreaDataScraper:
    def __init__(self, utility):
        self.utility = utility
        self.scraper = selectUtility(utility)

    def scrape(self):
        print("Scraping Area Data for {}:".format(self.utility))
        df = self.scraper.get_dataframe()
        numRows = len(df.index)
        print(" - Got {} rows of Data".format(numRows))

        print("Sending:")
        self._upload_blob_to_storage(df)
        print(" - Sent to Cloud Storage")

        self._insert_into_bigquery(df)
        print(" - Sent to BigQuery")

        print("Creating Regression Model")
        self._create_linear_regression_model()
        print(" - Regression Created")
        return numRows

    def _upload_blob_to_storage(self, df):
        CS = storage.Client()
        dateString = datetime.today().strftime('%Y-%m-%d')
        BUCKET_NAME = 'scraper_data_' + stage
        BLOB_NAME = '{utility}_historical_data_{date}.csv'.format(
            utility=self.utility,
            date=dateString
        )

        """Uploads a file to the bucket."""
        bucket = CS.get_bucket(BUCKET_NAME)
        blob = bucket.blob(BLOB_NAME)

        blob.upload_from_string(self.scraper.convert_df_to_csv(df))

        print('Scraped Data Uploaded to {}.'.format(BLOB_NAME))

    def _insert_into_bigquery(self, df):
        BQ_DATASET = self.utility
        BQ_TABLE = 'historical_data_by_generation_type'

        table_id = BQ_DATASET + "." + BQ_TABLE

        df.to_gbq(table_id, if_exists="replace")

    def _create_linear_regression_model(self):

        # Dynamically Pull in the API code
        #   this function runs once upon scraping
        #   but all functional data - re: Carbon is in API
        #   this should maybe be changed
        utilityCaps = self.utility.title() + "API"
        UtilityAPI = locate(
            'api.utilities.{utility}.{utilityCaps}.{utilityCaps}'.format(
                utility=self.utility,
                utilityCaps=utilityCaps
            )
        )

        api = UtilityAPI()
        return api.create_linear_regression_model()


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
