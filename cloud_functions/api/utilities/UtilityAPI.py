import pandas as pd
import requests
import json
from google.cloud import bigquery
import os
stage = os.environ['STAGE']


class UtilityAPI:
    def __init__(self, utility):
        self.utility = utility
        self.bqStageName = "" if stage == "production" else "-staging"

    # Likely to be Overwritten
    def _get_intensity_query_string(self):
        ci = self.get_carbon_intensity_factors()

        return """
        AVG((
            (kWh_nuclear * {intensity_nuclear}) +
            (kWh_fossil * {intensity_fossil}) +
            (kWh_hydro * {intensity_hydro}) +
            (kWh_geothermal * {intensity_geothermal}) +
            (kWh_biomass * {intensity_biomass}) +
            (kWh_solar_output * {intensity_solar_output}) +
            (kWh_wind_output * {intensity_wind_output}) +
            (kWh_pumped_storage * {intensity_pumped_storage}) +
            (if(kWh_interconnectors > 0,kWh_interconnectors, 0) * {intensity_interconnectors})
            ) / kWh_total
            ) as carbon_intensity
        FROM `japan-grid-carbon-api{bqStageName}.{utility}.historical_data_by_generation_type`
        """.format(
            bqStageName=self.bqStageName,
            utility=self.utility,
            intensity_nuclear=ci["kWh_nuclear"],
            intensity_fossil=ci["kWh_fossil"],
            intensity_hydro=ci["kWh_hydro"],
            intensity_geothermal=ci["kWh_geothermal"],
            intensity_biomass=ci["kWh_biomass"],
            intensity_solar_output=ci["kWh_solar_output"],
            intensity_wind_output=ci["kWh_wind_output"],
            intensity_pumped_storage=ci["kWh_pumped_storage"],
            intensity_interconnectors=ci["kWh_interconnectors"]
        )

    def _extract_daily_carbon_intensity_from_big_query(self):

        query = """
        SELECT
        EXTRACT(HOUR FROM datetime) AS hour,
        """ + self._get_intensity_query_string() + """
        GROUP BY hour
        order by hour asc
        """

        return pd.read_gbq(query)

    def _extract_daily_carbon_intensity_by_year_from_big_query(self):

        query = """
        SELECT
        EXTRACT(YEAR from datetime) as year,
        EXTRACT(HOUR FROM datetime) AS hour,
        """ + self._get_intensity_query_string() + """
        GROUP BY year, hour
        order by year, hour asc
        """

        return pd.read_gbq(query)

    def _extract_daily_carbon_intensity_by_month_from_big_query(self):

        query = """
        SELECT
        EXTRACT(MONTH FROM datetime) AS month,
        EXTRACT(HOUR FROM datetime) AS hour,
        """ + self._get_intensity_query_string() + """
        GROUP BY month, hour
        order by month, hour asc
        """

        return pd.read_gbq(query)

    def _extract_daily_carbon_intensity_by_month_and_year_from_big_query(self):

        query = """
        SELECT
        EXTRACT(MONTH FROM datetime) AS month,
        EXTRACT(YEAR FROM datetime) AS year,
        EXTRACT(HOUR FROM datetime) AS hour,
        """ + self._get_intensity_query_string() + """
        GROUP BY year, month, hour
        order by year, month, hour asc
        """

        return pd.read_gbq(query)

    def _extract_daily_carbon_intensity_by_month_and_weekday_from_big_query(self):

        query = """
        SELECT
        EXTRACT(MONTH FROM datetime) AS month,
        EXTRACT(DAYOFWEEK FROM datetime) AS dayofweek,
        EXTRACT(HOUR FROM datetime) AS hour,
        """ + self._get_intensity_query_string() + """
        GROUP BY month, dayofweek, hour
        order by month, dayofweek, hour asc
        """

        return pd.read_gbq(query)

    def _extract_daily_carbon_intensity_by_month_and_year_from_big_query(self):

        query = """
        SELECT
        EXTRACT(MONTH FROM datetime) AS month,
        EXTRACT(YEAR FROM datetime) AS year,
        EXTRACT(DAYOFWEEK FROM datetime) AS dayofweek,
        EXTRACT(HOUR FROM datetime) AS hour,
        """ + self._get_intensity_query_string() + """
        GROUP BY year, month, dayofweek, hour
        order by year, month, dayofweek, hour asc
        """

        return pd.read_gbq(query)

    def _extract_prediction_from_big_query_by_weekday_month_and_year(self, year):

        query = """
        SELECT
        predicted_carbon_intensity, year, dayofweek, month, hour
        FROM
        ML.PREDICT(MODEL `japan-grid-carbon-api{bqStageName}.{utility}.year_month_dayofweek_model`,
            (
            SELECT
                {year} AS year,
                EXTRACT(DAYOFWEEK FROM datetime) AS dayofweek,
                EXTRACT(MONTH FROM datetime) AS month,
                EXTRACT(HOUR FROM datetime) AS hour,
            FROM japan-grid-carbon-api{bqStageName}.{utility}.historical_data_by_generation_type
            GROUP BY month, dayofweek, hour
            )
        )
        order by month, dayofweek, hour asc
        """.format(
                bqStageName=self.bqStageName,
                utility=self.utility,
                year=year
        )

        return pd.read_gbq(query)

    def daily_intensity(self):

        df = self._extract_daily_carbon_intensity_from_big_query()

        df.reset_index(inplace=True)

        output = {"carbon_intensity_by_hour": df[[
            'hour', 'carbon_intensity']].to_dict("records")
        }

        return output

    def daily_intensity_by_year(self):

        df = self._extract_daily_carbon_intensity_by_year_from_big_query()

        df.reset_index(inplace=True)

        output = {
            "carbon_intensity_by_year": df.groupby('year')
            .apply(
                lambda year: year[['hour', 'carbon_intensity']]
                .to_dict(orient='records')
            ).to_dict()
        }

        return output

    def daily_intensity_by_month(self):

        df = self._extract_daily_carbon_intensity_by_month_from_big_query()

        df.reset_index(inplace=True)

        output = {
            "carbon_intensity_by_month": df.groupby('month')
            .apply(
                lambda month: month[['hour', 'carbon_intensity']]
                .to_dict(orient='records')
            ).to_dict()
        }

        return output

    def daily_intensity_by_month_and_year(self):

        df = self._extract_daily_carbon_intensity_by_month_and_year_from_big_query()

        df.reset_index(inplace=True)

        output = {
            "carbon_intensity_by_month_and_year": df.groupby('year')
            .apply(
                lambda year: year.groupby('month').apply(
                    lambda month: month[['hour', 'carbon_intensity']]
                    .to_dict(orient='records')
                ).to_dict()
            ).to_dict()
        }

        return output

    def daily_intensity_by_month_and_weekday(self):

        df = self._extract_daily_carbon_intensity_by_month_and_weekday_from_big_query()

        df.reset_index(inplace=True)

        output = {
            "carbon_intensity_by_month_and_weekday": df.groupby('month')
            .apply(
                lambda month: month.groupby('dayofweek').apply(
                    lambda day: day[['hour', 'carbon_intensity']]
                    .to_dict(orient='records')
                ).to_dict()
            ).to_dict()
        }

        return output

    def daily_intensity_by_year_month_and_weekday(self):

        df = self._extract_daily_carbon_intensity_by_month_and_year_from_big_query()

        df.reset_index(inplace=True)

        output = {
            "carbon_intensity_by_year_month_and_weekday": df.groupby('year')
            .apply(
                lambda year: year.groupby('month').apply(
                    lambda month: month.groupby('dayofweek').apply(
                        lambda day: day[['hour', 'carbon_intensity']]
                        .to_dict(orient='records')
                    ).to_dict()
                ).to_dict()
            ).to_dict()
        }

        return output

    def daily_intensity_prediction_for_year_by_month_and_weekday(self, year):
        df = self._extract_prediction_from_big_query_by_weekday_month_and_year(
            year)

        df.reset_index(inplace=True)

        output = {
            "prediction_year": year,
            "carbon_intensity_by_month_and_weekday": df.groupby('month')
            .apply(
                lambda month: month.groupby('dayofweek').apply(
                    lambda day: day[['hour', 'predicted_carbon_intensity']]
                    .to_dict(orient='records')
                ).to_dict()
            ).to_dict()
        }

        return output

    def query_timeseries_model(self):
        query = """
        SELECT
        *
        FROM
        ML.FORECAST(
            MODEL `japan-grid-carbon-api{bqStageName}.{utility}.model_intensity_timeseries`,
            STRUCT(2232 AS horizon)
        )
        """.format(
            bqStageName=self.bqStageName,
            utility=self.utility,
        )

        return pd.read_gbq(query)

    def create_timeseries_model(self):
        client = bigquery.Client()

        query = """
        CREATE OR REPLACE MODEL `japan-grid-carbon-api{bqStageName}.{utility}.model_intensity_timeseries`
        OPTIONS(
        MODEL_TYPE='ARIMA',
        TIME_SERIES_TIMESTAMP_COL="datetime",
        TIME_SERIES_DATA_COL ="carbon_intensity",
        AUTO_ARIMA= TRUE,
        DATA_FREQUENCY ='HOURLY',
        HOLIDAY_REGION = 'JP',
        HORIZON = 2232
        ) AS
            SELECT
            datetime,
                {intensity_calc}
                as carbon_intensity
            FROM (
                {from_string}
            )
        order by datetime
        """.format(
            bqStageName=self.bqStageName,
            utility=self.utility,
            from_string=self._pumped_storage_calc_query_string(),
            intensity_calc=self._carbon_intensity_query_string()
        )
        print("Creating ARIMA Timeseries model for " + self.utility)

        queryJob = client.query(query)
        result = queryJob.result()

        return "Success"

    # Likely to be Overwritten

    def get_carbon_intensity_factors(self):
        # Get And Calculate Carbon Intensity
        print("Grabbing Intensities")
        response = requests.get(
            "https://api.carbonintensity.org.uk/intensity/factors")

        # Thermal Data: https://www7.tepco.co.jp/fp/thermal-power/list-e.html
        fossilFuelStations = {
            "lng": 4.38 + 3.6 + 3.6 + 5.16 + 3.42 + 3.541 + 1.15 + 2 + 1.14,
            "oil": 5.66 + 1.05 + 4.40,
            "coal": 2
        }
        totalFossil = fossilFuelStations["lng"] + \
            fossilFuelStations["oil"] + fossilFuelStations["coal"]

        json = response.json()
        factors = json["data"][0]

        print("Resolving Intensities for " + self.utility)

        return {
            "kWh_nuclear": factors["Nuclear"],
            "kWh_fossil": (factors["Coal"] * fossilFuelStations["coal"] + factors["Oil"] * fossilFuelStations["oil"] + factors["Gas (Open Cycle)"] * fossilFuelStations["lng"]) / totalFossil,
            "kWh_hydro": factors["Hydro"],
            "kWh_geothermal": 0,  # Probably
            "kWh_biomass": factors["Biomass"],
            "kWh_solar_output": factors["Solar"],
            "kWh_wind_output": factors["Wind"],
            "kWh_pumped_storage": factors["Pumped Storage"],
            # TODO: Replace this with a rolling calculation of the average of other parts of Japan's carbon intensity, probably around 850 though
            "kWh_interconnectors": 500
        }
