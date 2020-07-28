import json
import copy
import logging
import pandas as pd
from google.cloud import storage
from google.cloud import bigquery
from google.api_core import retry

import tepco.analysis.tepco_carbon_intensity as tci

cache = {}


def _extract_daily_carbon_intensity_from_big_query(utility):

    carbonIntensity = tci.getCarbonIntensityFactors()

    query = """
    SELECT
    EXTRACT(HOUR FROM datetime) AS hour,
    AVG((
        (kWh_nuclear * {intensity_nuclear}) + 
        (kWh_fossil * {intensity_fossil}) + 
        (kWh_hydro * {intensity_hydro}) + 
        (kWh_geothermal * {intensity_geothermal}) + 
        (kWh_biomass * {intensity_biomass}) +
        (kWh_solar_output * {intensity_solar_output}) +
        (kWh_wind_output * {intensity_wind_output}) +
        (kWh_pumped_storage * {intensity_pumped_storage}) +
        (kWh_interconnectors * {intensity_interconnectors}) 
        ) / kWh_total
        ) as carbon_intensity
    FROM japan-grid-carbon-api.{utility}.historical_data_by_generation_type
    GROUP BY hour
    order by hour asc
    """.format(
        utility=utility,
        intensity_nuclear=carbonIntensity["kWh_nuclear"],
        intensity_fossil=carbonIntensity["kWh_fossil"],
        intensity_hydro=carbonIntensity["kWh_hydro"],
        intensity_geothermal=carbonIntensity["kWh_geothermal"],
        intensity_biomass=carbonIntensity["kWh_biomass"],
        intensity_solar_output=carbonIntensity["kWh_solar_output"],
        intensity_wind_output=carbonIntensity["kWh_wind_output"],
        intensity_pumped_storage=carbonIntensity["kWh_pumped_storage"],
        intensity_interconnectors=carbonIntensity["kWh_interconnectors"]
    )

    return pd.read_gbq(query)


def _extract_daily_carbon_intensity_by_month_from_big_query(utility):

    carbonIntensity = tci.getCarbonIntensityFactors()

    query = """
    SELECT
    EXTRACT(MONTH FROM datetime) AS month,
    EXTRACT(HOUR FROM datetime) AS hour,
    AVG((
        (kWh_nuclear * {intensity_nuclear}) + 
        (kWh_fossil * {intensity_fossil}) + 
        (kWh_hydro * {intensity_hydro}) + 
        (kWh_geothermal * {intensity_geothermal}) + 
        (kWh_biomass * {intensity_biomass}) +
        (kWh_solar_output * {intensity_solar_output}) +
        (kWh_wind_output * {intensity_wind_output}) +
        (kWh_pumped_storage * {intensity_pumped_storage}) +
        (kWh_interconnectors * {intensity_interconnectors}) 
        ) / kWh_total
        ) as carbon_intensity
    FROM japan-grid-carbon-api.{utility}.historical_data_by_generation_type
    GROUP BY month, hour
    order by month, hour asc
    """.format(
        utility=utility,
        intensity_nuclear=carbonIntensity["kWh_nuclear"],
        intensity_fossil=carbonIntensity["kWh_fossil"],
        intensity_hydro=carbonIntensity["kWh_hydro"],
        intensity_geothermal=carbonIntensity["kWh_geothermal"],
        intensity_biomass=carbonIntensity["kWh_biomass"],
        intensity_solar_output=carbonIntensity["kWh_solar_output"],
        intensity_wind_output=carbonIntensity["kWh_wind_output"],
        intensity_pumped_storage=carbonIntensity["kWh_pumped_storage"],
        intensity_interconnectors=carbonIntensity["kWh_interconnectors"]
    )

    return pd.read_gbq(query)


def _extract_daily_carbon_intensity_by_month_and_weekday_from_big_query(utility):

    carbonIntensity = tci.getCarbonIntensityFactors()

    query = """
    SELECT
    EXTRACT(MONTH FROM datetime) AS month,
    EXTRACT(DAYOFWEEK FROM datetime) AS dayofweek,
    EXTRACT(HOUR FROM datetime) AS hour,
    AVG((
        (kWh_nuclear * {intensity_nuclear}) + 
        (kWh_fossil * {intensity_fossil}) + 
        (kWh_hydro * {intensity_hydro}) + 
        (kWh_geothermal * {intensity_geothermal}) + 
        (kWh_biomass * {intensity_biomass}) +
        (kWh_solar_output * {intensity_solar_output}) +
        (kWh_wind_output * {intensity_wind_output}) +
        (kWh_pumped_storage * {intensity_pumped_storage}) +
        (kWh_interconnectors * {intensity_interconnectors}) 
        ) / kWh_total
        ) as carbon_intensity
    FROM japan-grid-carbon-api.{utility}.historical_data_by_generation_type
    GROUP BY month, dayofweek, hour
    order by month, dayofweek, hour asc
    """.format(
        utility=utility,
        intensity_nuclear=carbonIntensity["kWh_nuclear"],
        intensity_fossil=carbonIntensity["kWh_fossil"],
        intensity_hydro=carbonIntensity["kWh_hydro"],
        intensity_geothermal=carbonIntensity["kWh_geothermal"],
        intensity_biomass=carbonIntensity["kWh_biomass"],
        intensity_solar_output=carbonIntensity["kWh_solar_output"],
        intensity_wind_output=carbonIntensity["kWh_wind_output"],
        intensity_pumped_storage=carbonIntensity["kWh_pumped_storage"],
        intensity_interconnectors=carbonIntensity["kWh_interconnectors"]
    )

    return pd.read_gbq(query)


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


def _extract_daily_info_by_month_from_big_query(utility):

    query = """
    SELECT
        EXTRACT(MONTH FROM datetime) AS month,
        EXTRACT(HOUR FROM datetime) AS hour,
        AVG(kWh_demand) as kWh_demand_avg,
        AVG(kWh_nuclear) as kWh_nuclear_avg,
        AVG(kWh_fossil) as kWh_fossil_avg,
        AVG(kWh_hydro) as kWh_hydro_avg,
        AVG(kWh_geothermal) as kWh_geothermal_avg,
        AVG(kWh_biomass) as kWh_biomass_avg,
        AVG(kWh_solar_output) as kWh_solar_output_avg,
        AVG(kWh_solar_throttling) as kWh_solar_throttling_avg,
        AVG(kWh_wind_output) as kWh_wind_output_avg,
        AVG(kWh_wind_throttling) as kWh_wind_throttling_avg,
        AVG(kWh_pumped_storage) as kWh_pumped_storage_avg,
        AVG(kWh_interconnectors) as kWh_interconnectors_avg,
        AVG(kWh_total) as kWh_total_avg
    FROM `japan-grid-carbon-api.{utility}.historical_data_by_generation_type`
    GROUP BY month, hour
    order by month, hour asc
    """.format(utility=utility)

    return pd.read_gbq(query)


def _tepco_daily_intensity():
    # Check Cache
    if "_tepco_daily_intensity" in cache:
        print("Returning cache._tepco_daily_intensity:")
        return cache["_tepco_daily_intensity"]

    df = _extract_daily_carbon_intensity_from_big_query("tepco")

    df.reset_index(inplace=True)

    output = {"carbon_intensity_by_hour": df[[
        'hour', 'carbon_intensity']].to_dict("records"),
        "fromCache": True}

    # Populate Cache
    cache['_tepco_daily_intensity'] = copy.deepcopy(output)
    output["fromCache"] = False
    return output


def _tepco_daily_intensity_by_month():
    # Check Cache
    if "_tepco_daily_intensity_by_month" in cache:
        print("Returning cache._tepco_daily_intensity_by_month:")
        return cache["_tepco_daily_intensity_by_month"]

    df = _extract_daily_carbon_intensity_by_month_from_big_query("tepco")

    df.reset_index(inplace=True)

    output = {
        "carbon_intensity_by_month": df.groupby('month')
        .apply(
            lambda month: month[['hour', 'carbon_intensity']]
            .to_dict(orient='records')
        ).to_dict(),
        "fromCache": True
    }

    # Populate Cache
    cache['_tepco_daily_intensity_by_month'] = copy.deepcopy(output)
    output["fromCache"] = False
    return output


def _tepco_daily_intensity_by_month_and_weekday():
    # Check Cache
    if "_tepco_daily_intensity_by_month_and_weekday" in cache:
        print("Returning cache._tepco_daily_intensity_by_month_and_weekday:")
        return cache["_tepco_daily_intensity_by_month_and_weekday"]

    df = _extract_daily_carbon_intensity_by_month_and_weekday_from_big_query(
        "tepco")

    df.reset_index(inplace=True)

    print(df.head())

    output = {
        "carbon_intensity_by_month_and_weekday": df.groupby('month')
        .apply(
            lambda month: month.groupby('dayofweek').apply(
                lambda day: day[['hour', 'carbon_intensity']]
                .to_dict(orient='records')
            ).to_dict()
        ).to_dict(),
        "fromCache": True
    }

    # Populate Cache
    cache['_tepco_daily_intensity_by_month_and_weekday'] = copy.deepcopy(
        output)
    output["fromCache"] = False
    return output


def daily_carbon_intensity(request):
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

    response = {}

    if request_json and 'utility' in request_json:
        utility = request_json['utility']
    elif request_args and 'utility' in request_args:
        utility = request_args['utility']
    else:
        return f'No utility specified', 400, headers

    if utility == "tepco":
        df = _tepco_daily_intensity()
        response['data'] = df
        return json.dumps(response), 200, headers


def daily_carbon_intensity_by_month(request):
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

    response = {}

    if request_json and 'utility' in request_json:
        utility = request_json['utility']
    elif request_args and 'utility' in request_args:
        utility = request_args['utility']
    else:
        return f'No utility specified', 400, headers

    if utility == "tepco":
        carbon_intensity_by_month = _tepco_daily_intensity_by_month()
        response['data'] = carbon_intensity_by_month
        return response, 200, headers


def daily_carbon_intensity_by_month_and_weekday(request):
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

    response = {}

    if request_json and 'utility' in request_json:
        utility = request_json['utility']
    elif request_args and 'utility' in request_args:
        utility = request_args['utility']
    else:
        return f'No utility specified', 400, headers

    if utility == "tepco":
        carbon_intensity_by_month_and_weekday = _tepco_daily_intensity_by_month_and_weekday()
        response['data'] = carbon_intensity_by_month_and_weekday
        return response, 200, headers
