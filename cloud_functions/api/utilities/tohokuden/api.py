import copy
import logging
import pandas as pd
from google.cloud import storage
from google.cloud import bigquery
from google.api_core import retry

import utilities.tohokuden.analysis.tohokuden_carbon_intensity as tci

cache = {}


def _get_intensity_query_string(utility):
    carbonIntensity = tci.getCarbonIntensityFactors()

    return """
    SELECT
    EXTRACT(HOUR FROM datetime) AS hour,
    AVG((
        (MWh_nuclear * {intensity_nuclear}) + 
        (MWh_fossil * {intensity_fossil}) + 
        (MWh_hydro * {intensity_hydro}) + 
        (MWh_geothermal * {intensity_geothermal}) + 
        (MWh_biomass * {intensity_biomass}) +
        (MWh_solar_output * {intensity_solar_output}) +
        (MWh_wind_output * {intensity_wind_output}) +
        (MWh_pumped_storage * {intensity_pumped_storage})
        ) / (MWh_area_demand + MWh_interconnectors)
        ) as carbon_intensity
    FROM japan-grid-carbon-api.{utility}.historical_data_by_generation_type
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


def _extract_daily_carbon_intensity_from_big_query(utility):

    query = _get_intensity_query_string(utility) + """
    GROUP BY hour
    order by hour asc
    """

    return pd.read_gbq(query)


def _extract_daily_carbon_intensity_by_month_from_big_query(utility):

    query = _get_intensity_query_string(utility) + """
    GROUP BY month, hour
    order by month, hour asc
    """

    return pd.read_gbq(query)


def _extract_daily_carbon_intensity_by_month_and_weekday_from_big_query(utility):

    query = _get_intensity_query_string(utility) + """
    GROUP BY month, dayofweek, hour
    order by month, dayofweek, hour asc
    """

    return pd.read_gbq(query)


def daily_intensity():
    # Check Cache
    if "_tohokuden_daily_intensity" in cache:
        print("Returning cache._tohokuden_daily_intensity:")
        return cache["_tohokuden_daily_intensity"]

    df = _extract_daily_carbon_intensity_from_big_query("tohokuden")

    df.reset_index(inplace=True)

    output = {"carbon_intensity_by_hour": df[[
        'hour', 'carbon_intensity']].to_dict("records"),
        "fromCache": True}

    # Populate Cache
    cache['_tohokuden_daily_intensity'] = copy.deepcopy(output)
    output["fromCache"] = False
    return output


def daily_intensity_by_month():
    # Check Cache
    if "_tohokuden_daily_intensity_by_month" in cache:
        print("Returning cache._tohokuden_daily_intensity_by_month:")
        return cache["_tohokuden_daily_intensity_by_month"]

    df = _extract_daily_carbon_intensity_by_month_from_big_query("tohokuden")

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
    cache['_tohokuden_daily_intensity_by_month'] = copy.deepcopy(output)
    output["fromCache"] = False
    return output


def daily_intensity_by_month_and_weekday():
    # Check Cache
    if "_tohokuden_daily_intensity_by_month_and_weekday" in cache:
        print("Returning cache._tohokuden_daily_intensity_by_month_and_weekday:")
        return cache["_tohokuden_daily_intensity_by_month_and_weekday"]

    df = _extract_daily_carbon_intensity_by_month_and_weekday_from_big_query(
        "tohokuden")

    df.reset_index(inplace=True)

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
    cache['_tohokuden_daily_intensity_by_month_and_weekday'] = copy.deepcopy(
        output)
    output["fromCache"] = False
    return output
