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
        print("Full Scrape and Model of Area Data for {}:".format(self.utility))
        numRows, startDate, endDate = self.get_data()
        self.create_timeseries_model()
        return numRows, startDate, endDate

    def get_data(self):
        print("Scraping Area Data for {}:".format(self.utility))
        df = self.scraper.get_dataframe()
        numRows = len(df.index)
        startDate = df['datetime'].iloc[0]
        endDate = df['datetime'].iloc[-1]
        print(" - Got {} rows of Data".format(numRows))
        print("   - Starting from {}".format(startDate))
        print("   - Ending at {}".format(endDate))

        print("Sending:")
        self._upload_blob_to_storage(df)
        print(" - Sent to Cloud Storage")

        self._insert_into_bigquery(
            df, 'historical_data_by_generation_type', 'replace')
        print(" - Sent to BigQuery")

        return numRows, startDate, endDate

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

    def _insert_into_bigquery(self, df, table_name, insertiontype):
        BQ_DATASET = self.utility

        table_id = BQ_DATASET + "." + table_name

        df.to_gbq(table_id, if_exists=insertiontype)

    def create_timeseries_model(self):
        print("Creating Timeseries Model")
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
        result = api.create_timeseries_model()

        print("Getting ARIMA Timeseries Forecast")

        forecast_df = api._query_timeseries_model()
        forecast_df['date_created'] = datetime.now()

        print(" - Timeseries Model Created")
        print("Sending Forecast To BigQuery")

        self._insert_into_bigquery(forecast_df, 'intensity_forecast', 'append')


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
