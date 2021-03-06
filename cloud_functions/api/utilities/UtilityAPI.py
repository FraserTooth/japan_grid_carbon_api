import pandas as pd
import requests
import json
from google.cloud import bigquery
import os
stage = os.environ['STAGE']

HORIZON = 2500
# Bit more than than 3 months of hours (24h * 31d * 3m = 2322)

# https://criepi.denken.or.jp/jp/kenkikaku/report/detail/Y06.html
national_lifecycle_carbon_intensities_by_source = {
    "coal": 943,
    "oil": 738,
    "lng": 474,
    "nuclear": 19,
    "hydro": 11,
    "geothermal": 13,
    "solar": 59,
    "wind": 26,
    "biomass": 120  # Still use the UK factor
}


class UtilityAPI:
    def __init__(self, utility, config):
        self.utility = utility
        self.bqStageName = "" if stage == "production" else "-staging"
        self.config = config

    def _get_intensity_query_string(self):
        query_string = """
        AVG(
            {intensity_calc}
        ) as carbon_intensity
        FROM (
            {from_string}
        )
        """.format(
            from_string=self._pumped_storage_calc_query_string(),
            intensity_calc=self._carbon_intensity_query_string()
        )
        return query_string

    def _pumped_storage_calc_query_string(self):
        return """
            SELECT *,
            (daMWh_nuclear + daMWh_fossil + daMWh_hydro + daMWh_geothermal + daMWh_biomass + daMWh_solar_output + daMWh_wind_output + daMWh_pumped_storage_contribution + daMWh_interconnector_contribution) as daMWh_total_generation
            FROM (
                SELECT *,
                if(daMWh_interconnectors > 0,daMWh_interconnectors, 0) as daMWh_interconnector_contribution,
                if(daMWh_pumped_storage > 0,daMWh_pumped_storage, 0) as daMWh_pumped_storage_contribution,
                FROM `japan-grid-carbon-api{bqStageName}.{utility}.historical_data_by_generation_type`
            )
        """.format(
            bqStageName=self.bqStageName,
            utility=self.utility
        )

    def _carbon_intensity_query_string(self):
        ci = self.get_carbon_intensity_factors()

        return """
        (
            (daMWh_nuclear * {intensity_nuclear}) +
            (daMWh_fossil * {intensity_fossil}) +
            (daMWh_hydro * {intensity_hydro}) +
            (daMWh_geothermal * {intensity_geothermal}) +
            (daMWh_biomass * {intensity_biomass}) +
            (daMWh_solar_output * {intensity_solar_output}) +
            (daMWh_wind_output * {intensity_wind_output}) +
            (daMWh_pumped_storage_contribution * {intensity_pumped_storage}) +
            (daMWh_interconnector_contribution * {intensity_interconnectors})
        ) / daMWh_total_generation
        """.format(
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

    def _query_timeseries_model(self):
        query = """
        SELECT
        *
        FROM
        ML.FORECAST(
            MODEL `japan-grid-carbon-api{bqStageName}.{utility}.model_intensity_timeseries`,
            STRUCT({horizon_size} AS horizon)
        )
        """.format(
            bqStageName=self.bqStageName,
            utility=self.utility,
            horizon_size=HORIZON
        )

        return pd.read_gbq(query)

    def _query_intensity_forecast(self, from_date, to_date):
        # Self Join on Table to return the most recently dated intensity forecast
        query = """
        SELECT 
            a.*
        FROM `japan-grid-carbon-api{bqStageName}.{utility}.intensity_forecast` as a
        INNER JOIN (
            SELECT
            forecast_timestamp,
            MAX(date_created) as most_recent_forecast_date
            FROM `japan-grid-carbon-api{bqStageName}.{utility}.intensity_forecast`
            GROUP BY forecast_timestamp
        ) b ON a.forecast_timestamp = b.forecast_timestamp AND a.date_created = b.most_recent_forecast_date
        WHERE EXTRACT(DATE from a.forecast_timestamp) BETWEEN DATE("{from_date}") and DATE("{to_date}")
        order by a.forecast_timestamp
        """.format(
            bqStageName=self.bqStageName,
            utility=self.utility,
            from_date=from_date,
            to_date=to_date
        )

        return pd.read_gbq(query)

    def historic_intensity(self, from_date, to_date):
        query = """
        SELECT
        datetime as timestamp,
        {intensity_calc}
        as carbon_intensity
        FROM (
            {from_string}
        )
        WHERE EXTRACT(DATE from datetime) BETWEEN DATE("{from_date}") and DATE("{to_date}")
        order by datetime
        """.format(
            from_string=self._pumped_storage_calc_query_string(),
            intensity_calc=self._carbon_intensity_query_string(),
            from_date=from_date,
            to_date=to_date
        )

        df = pd.read_gbq(query)
        df['timestamp'] = df['timestamp'].astype(str)

        output = {"historic":
                  df.to_dict(orient='records')
                  }

        return output

    def daily_intensity(self):

        df = self._extract_daily_carbon_intensity_from_big_query()

        df.reset_index(inplace=True)

        output = {"carbon_intensity_average": {
            "breakdown": "hour",
            "data": df[['hour', 'carbon_intensity']].to_dict("records")}
        }

        return output

    def daily_intensity_by_year(self):

        df = self._extract_daily_carbon_intensity_by_year_from_big_query()

        data = df.groupby('year').apply(
            lambda year: year[['hour', 'carbon_intensity']].to_dict(
                orient='records')
        )

        remap = list(map(lambda year, data: {
            "year": year, "data": data}, data.index, data.array))

        output = {
            "carbon_intensity_average": {
                "breakdown": 'year',
                "data": remap
            }
        }

        return output

    def daily_intensity_by_month(self):

        df = self._extract_daily_carbon_intensity_by_month_from_big_query()

        data = df.groupby('month').apply(
            lambda year: year[['hour', 'carbon_intensity']].to_dict(
                orient='records')
        )

        remap = list(map(lambda month, data: {
            "month": month, "data": data}, data.index, data.array))

        output = {
            "carbon_intensity_average": {
                "breakdown": 'month',
                "data": remap
            }
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

    def timeseries_prediction(self, from_date, to_date):
        df = self._query_intensity_forecast(from_date, to_date)

        df['forecast_timestamp'] = df['forecast_timestamp'].astype(str)
        df['date_created'] = df['date_created'].astype(str)

        output = {
            'forecast': df.to_dict(orient='records')
        }

        return output

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
        HORIZON = {horizon_size}
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
            intensity_calc=self._carbon_intensity_query_string(),
            horizon_size=HORIZON
        )
        print("Creating ARIMA Timeseries model for " + self.utility)

        queryJob = client.query(query)
        result = queryJob.result()

        return "Success"

    def get_carbon_intensity_factors(self):
        stations = self.config["fuel_type_totals"]
        totalFossil = stations["lng"] + \
            stations["oil"] + stations["coal"]

        factors = national_lifecycle_carbon_intensities_by_source

        return {
            "kWh_nuclear": factors["nuclear"],
            "kWh_fossil": (factors["coal"] * stations["coal"] + factors["oil"] * stations["oil"] + factors["lng"] * stations["lng"]) / totalFossil,
            "kWh_hydro": factors["hydro"],
            "kWh_geothermal": factors["geothermal"],
            "kWh_biomass": factors["biomass"],
            "kWh_solar_output": factors["solar"],
            "kWh_wind_output": factors["wind"],
            # Not always charged when renewables available, average of this
            "kWh_pumped_storage": self.config["pumped_storage_factor"],
            # TODO: Replace this with a rolling calculation of the average of other parts of Japan's carbon intensity, probably around 850 though
            "kWh_interconnectors": 500
        }
