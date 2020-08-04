import utilities.UtilityAPI as UtilityAPI


class TepcoAPI(UtilityAPI):
    def __init__(self):
        super().__init__("tepco")

    def _get_intensity_query_string(self):
        ci = self.carbonIntensity

        return """
        AVG((
            (daMWh_nuclear * {intensity_nuclear}) +
            (daMWh_fossil * {intensity_fossil}) +
            (daMWh_hydro * {intensity_hydro}) +
            (daMWh_geothermal * {intensity_geothermal}) +
            (daMWh_biomass * {intensity_biomass}) +
            (daMWh_solar_output * {intensity_solar_output}) +
            (daMWh_wind_output * {intensity_wind_output}) +
            (daMWh_pumped_storage * {intensity_pumped_storage}) +
            (if(daMWh_interconnectors > 0,daMWh_interconnectors, 0) * {intensity_interconnectors})
            ) / daMWh_total
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

        print("POOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOO")
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
