import json
import logging
import pandas as pd
from google.cloud import storage
from google.cloud import bigquery
from google.api_core import retry

import tepco.analysis.tepco_carbon_intensity as tci


def _extract_daily_info_from_big_query(utility):

    query = """
    SELECT
        EXTRACT(HOUR FROM datetime) AS hour,
        AVG(kWh_demand) as kWh_demand_hour,
        AVG(kWh_nuclear) as kWh_nuclear_hour,
        AVG(kWh_fossil) as kWh_fossil_hour,
        AVG(kWh_hydro) as kWh_hydro_hour,
        AVG(kWh_geothermal) as kWh_geothermal_hour,
        AVG(kWh_biomass) as kWh_biomass_hour,
        AVG(kWh_solar_output) as kWh_solar_output_hour,
        AVG(kWh_solar_throttling) as kWh_solar_throttling_hour,
        AVG(kWh_wind_output) as kWh_wind_output_hour,
        AVG(kWh_wind_throttling) as kWh_wind_throttling_hour,
        AVG(kWh_pumped_storage) as kWh_pumped_storage_hour,
        AVG(kWh_interconnectors) as kWh_interconnectors_hour,
        AVG(kWh_total) as kWh_total_hour
    FROM `japan-grid-carbon-api.{utility}.historical_data_by_generation_type`
    GROUP BY hour
    ORDER BY hour asc
    """.format(utility=utility)

    return pd.read_gbq(query)


def _tepco_daily_intensity():
    df = tci.addCarbonIntensityFactors(
        _extract_daily_info_from_big_query("tepco"), "_hour")

    df.reset_index(inplace=True)
    return df.to_json(orient='index', date_format="iso")


def daily_carbon_intensity(request):

    # For more information about CORS and CORS preflight requests, see
    # https://developer.mozilla.org/en-US/docs/Glossary/Preflight_request
    # for more information.

    # Set CORS headers for the preflight request
    if request.method == 'OPTIONS':
        # Allows GET requests from any origin with the Content-Type
        # header and caches preflight response for an 3600s
        headers = {
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'GET',
            'Access-Control-Allow-Headers': 'Content-Type',
            'Access-Control-Max-Age': '3600'
        }

        return ('', 204, headers)

    # Set CORS headers for the main request
    headers = {
        'Access-Control-Allow-Origin': '*'
    }

    request_json = request.get_json(silent=True)
    request_args = request.args

    if request_json and 'utility' in request_json:
        utility = request_json['utility']
    elif request_args and 'utility' in request_args:
        utility = request_args['utility']
    else:
        return f'No utility specified', 400, headers

    if utility == "tepco":
        return _tepco_daily_intensity(), 200, headers
