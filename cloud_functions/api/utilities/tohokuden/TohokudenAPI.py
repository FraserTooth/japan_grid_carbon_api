import requests
from ..UtilityAPI import UtilityAPI


class TohokudenAPI(UtilityAPI):
    def __init__(self):
        super().__init__("tohokuden")

    def _get_intensity_query_string(self):
        ci = self.get_carbon_intensity_factors()

        return """
        AVG((
            (MWh_nuclear * {intensity_nuclear}) + 
            (MWh_fossil * {intensity_fossil}) + 
            (MWh_hydro * {intensity_hydro}) + 
            (MWh_geothermal * {intensity_geothermal}) + 
            (MWh_biomass * {intensity_biomass}) +
            (MWh_solar_output * {intensity_solar_output}) +
            (MWh_wind_output * {intensity_wind_output}) +
            (MWh_pumped_storage_contribution * {intensity_pumped_storage}) +
            (MWh_interconnector_contribution * {intensity_interconnectors}) 
            ) / (MWh_total_generation)
            ) as carbon_intensity
        FROM (
            SELECT *,
            (MWh_nuclear + MWh_fossil + MWh_hydro + MWh_geothermal + MWh_biomass + MWh_solar_output + MWh_wind_output + MWh_pumped_storage_contribution + MWh_interconnector_contribution) as MWh_total_generation
            FROM (
                SELECT *,
                if(MWh_interconnectors > 0,MWh_interconnectors, 0) as MWh_interconnector_contribution,
                if(MWh_pumped_storage > 0,MWh_pumped_storage, 0) as MWh_pumped_storage_contribution,
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

        # Thermal Energy Percentages: https://www.tohoku-epco.co.jp/ir/report/factbook/pdf/fact01.pdf
        # Numbers represent the proportions of energy use
        fossilFuelStations = {
            "lng": 24,
            "oil": 2,
            "coal": 23
        }
        totalFossil = fossilFuelStations["lng"] + \
            fossilFuelStations["oil"] + fossilFuelStations["coal"]

        json = response.json()
        factors = json["data"][0]

        print("Resolving Intensities for Tohoku")

        return {
            "kWh_nuclear": factors["Nuclear"],
            "kWh_fossil": (factors["Coal"] * fossilFuelStations["coal"] + factors["Oil"] * fossilFuelStations["oil"] + factors["Gas (Open Cycle)"] * fossilFuelStations["lng"]) / totalFossil,
            "kWh_hydro": factors["Hydro"],
            "kWh_geothermal": 0,  # Probably
            "kWh_biomass": factors["Biomass"],
            "kWh_solar_output": factors["Solar"],
            "kWh_wind_output": factors["Wind"],
            # Appears to always get charge when renewables available
            "kWh_pumped_storage": 0,
            # TODO: Replace this with a rolling calculation of the average of other parts of Japan's carbon intensity, probably around 850 though
            "kWh_interconnectors": 500
        }
