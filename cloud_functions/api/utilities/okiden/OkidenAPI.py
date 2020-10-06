import requests
from ..UtilityAPI import UtilityAPI


# Okiden has no inteconnectors, nuclear or pumped storage

class OkidenAPI(UtilityAPI):
    def __init__(self):
        super().__init__("okiden")

    def _get_intensity_query_string(self):
        ci = self.get_carbon_intensity_factors()

        return """
        AVG((
            (MWh_fossil * {intensity_fossil}) + 
            (MWh_hydro * {intensity_hydro}) + 
            (MWh_biomass * {intensity_biomass}) +
            (MWh_solar_output * {intensity_solar_output}) +
            (MWh_wind_output * {intensity_wind_output})
            ) / (MWh_total_generation)
            ) as carbon_intensity
        FROM (
            SELECT *,
            (MWh_fossil + MWh_hydro + MWh_biomass + MWh_solar_output + MWh_wind_output) as MWh_total_generation
            FROM `japan-grid-carbon-api.{utility}.historical_data_by_generation_type`
        )
        """.format(
            utility=self.utility,
            intensity_fossil=ci["kWh_fossil"],
            intensity_hydro=ci["kWh_hydro"],
            intensity_biomass=ci["kWh_biomass"],
            intensity_solar_output=ci["kWh_solar_output"],
            intensity_wind_output=ci["kWh_wind_output"],
        )

    def get_carbon_intensity_factors(self):

        # Thermal Energy Percentages: https://www.okiden.co.jp/shared/pdf/ir/ar/ar2017/180516_02.pdf
        # Numbers represent the proportions of energy use
        fossilFuelStations = {
            "lng": 21,
            "oil": 13,
            "coal": 61
        }
        totalFossil = fossilFuelStations["lng"] + \
            fossilFuelStations["oil"] + fossilFuelStations["coal"]

        # CEPCO Has factors in its info - "For Japan" - Page 9
        factors = {
            "Nuclear": 19,
            "Hydro": 11,
            "Wind": 26,
            "Solar": 38,
            "Gas (Open Cycle)": 599,
            "Gas (Combined Cycle)": 474,
            "Oil": 738,
            "Coal": 943,
            "Geothermal": 13,
            # From UK - Not in PDF
            "Biomass": 120,
        }

        print("Resolving Intensities for Okiden")

        return {
            "kWh_fossil": (factors["Coal"] * fossilFuelStations["coal"] + factors["Oil"] * fossilFuelStations["oil"] + factors["Gas (Open Cycle)"] * fossilFuelStations["lng"]) / totalFossil,
            "kWh_hydro": factors["Hydro"],
            "kWh_geothermal": factors["Geothermal"],
            "kWh_biomass": factors["Biomass"],
            "kWh_solar_output": factors["Solar"],
            "kWh_wind_output": factors["Wind"],
            # No interconnectors, nuclear or pumped storage
        }
