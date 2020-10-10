import requests
from ..UtilityAPI import UtilityAPI


class YondenAPI(UtilityAPI):
    def __init__(self):
        super().__init__("yonden")

    def _get_intensity_query_string(self):
        ci = self.get_carbon_intensity_factors()

        return """
        AVG((
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
            ) as carbon_intensity
        FROM (
            SELECT *,
            (daMWh_nuclear + daMWh_fossil + daMWh_hydro + daMWh_geothermal + daMWh_biomass + daMWh_solar_output + daMWh_wind_output + daMWh_pumped_storage_contribution + daMWh_interconnector_contribution) as daMWh_total_generation
            FROM (
                SELECT *,
                if(daMWh_interconnectors > 0,daMWh_interconnectors, 0) as daMWh_interconnector_contribution,
                if(daMWh_pumped_storage > 0,daMWh_pumped_storage, 0) as daMWh_pumped_storage_contribution,
                FROM `japan-grid-carbon-api{bqStageName}.{utility}.historical_data_by_generation_type`
            )
        )
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

    def get_carbon_intensity_factors(self):
        # Get And Calculate Carbon Intensity
        print("Grabbing Intensities")
        response = requests.get(
            "https://api.carbonintensity.org.uk/intensity/factors")

        # Thermal Data: https://www.yonden.co.jp/english/assets/pdf/ir/tools/ann_r/annual_e_2019.pdf
        fossilFuelStations = {
            "lng": 0.296 + 0.289 + 0.35,
            "oil": 0.45 + 0.45 + 0.45,
            "coal": 0.156 + 0.25 + 0.7
        }
        totalFossil = fossilFuelStations["lng"] + \
            fossilFuelStations["oil"] + fossilFuelStations["coal"]

        json = response.json()
        factors = json["data"][0]

        print("Resolving Intensities for Yonden")

        return {
            "kWh_nuclear": factors["Nuclear"],
            "kWh_fossil": (factors["Coal"] * fossilFuelStations["coal"] + factors["Oil"] * fossilFuelStations["oil"] + factors["Gas (Open Cycle)"] * fossilFuelStations["lng"]) / totalFossil,
            "kWh_hydro": factors["Hydro"],
            "kWh_geothermal": 0,  # Probably
            "kWh_biomass": factors["Biomass"],
            "kWh_solar_output": factors["Solar"],
            "kWh_wind_output": factors["Wind"],
            # Not always charged when renewables available, average of this
            "kWh_pumped_storage": 8.57,
            # TODO: Replace this with a rolling calculation of the average of other parts of Japan's carbon intensity, probably around 850 though
            "kWh_interconnectors": 500
        }
