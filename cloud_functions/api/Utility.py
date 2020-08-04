class Utility:
    def __init__(self, utility):
        self.utility = utility
        self.carbonIntensity = self.get_carbon_intensity_factors()

    def _get_intensity_query_string(self):
        ci = self.carbonIntensity

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
        FROM japan-grid-carbon-api.{utility}.historical_data_by_generation_type
        """.format(
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

    def daily_intensity(self):

        df = _extract_daily_carbon_intensity_from_big_query(self.utility)

        df.reset_index(inplace=True)

        output = {"carbon_intensity_by_hour": df[[
            'hour', 'carbon_intensity']].to_dict("records")
        }

        return output

    def daily_intensity_by_month(self):

        df = _extract_daily_carbon_intensity_by_month_from_big_query(
            self.utility)

        df.reset_index(inplace=True)

        output = {
            "carbon_intensity_by_month": df.groupby('month')
            .apply(
                lambda month: month[['hour', 'carbon_intensity']]
                .to_dict(orient='records')
            ).to_dict()
        }

        return output

    def daily_intensity_by_month_and_weekday(self):

        df = _extract_daily_carbon_intensity_by_month_and_weekday_from_big_query(
            self.utility)

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
