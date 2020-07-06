import json
import logging
import pandas as pd
from google.cloud import storage
from google.cloud import bigquery
from google.api_core import retry

import tepco.analysis.tepco_carbon_intensity as tci


def _extract_daily_info_from_big_query(utility):

    query = """
      SELECT * 
      FROM `japan-grid-carbon-api.{utility}.historical_data_by_generation_type` 
      ORDER BY datetime desc
    """.format(utility=utility)

    return pd.read_gbq(query)


def _tepco_daily_intensity():
    df = _extract_daily_info_from_big_query("tepco")


def daily_carbon_intensity(request):
    request_json = request.get_json(silent=True)
    request_args = request.args

    if request_json and 'utility' in request_json:
        utility = request_json['utility']
    elif request_args and 'utility' in request_args:
        utility = request_args['utility']
    else:
        return f'No utility specified', 400

    if utility == "tepco":
        return _extract_daily_info_from_big_query()
