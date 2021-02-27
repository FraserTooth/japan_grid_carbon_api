import requests
from ..UtilityAPI import UtilityAPI


config_okiden = {
    "pumped_storage_factor": 0,

    # Thermal Energy Percentages: https://www.okiden.co.jp/shared/pdf/ir/ar/ar2017/180516_02.pdf
    "fuel_type_totals": {
        "lng": 21,
        "oil": 13,
        "coal": 61
    }
}

# Okiden has no inteconnectors, nuclear or pumped storage


class OkidenAPI(UtilityAPI):
    def __init__(self):
        super().__init__("okiden", config_okiden)

    def _pumped_storage_calc_query_string(self):
        return """
            SELECT *,
            (MWh_fossil + MWh_hydro + MWh_biomass + MWh_solar_output + MWh_wind_output) as MWh_total_generation
            FROM `japan-grid-carbon-api{bqStageName}.{utility}.historical_data_by_generation_type`
        """.format(
            bqStageName=self.bqStageName,
            utility=self.utility
        )

    def _carbon_intensity_query_string(self):
        ci = self.get_carbon_intensity_factors()

        return """
        (
            (MWh_fossil * {intensity_fossil}) + 
            (MWh_hydro * {intensity_hydro}) + 
            (MWh_biomass * {intensity_biomass}) +
            (MWh_solar_output * {intensity_solar_output}) +
            (MWh_wind_output * {intensity_wind_output})
        ) / (MWh_total_generation)
        """.format(
            intensity_fossil=ci["kWh_fossil"],
            intensity_hydro=ci["kWh_hydro"],
            intensity_biomass=ci["kWh_biomass"],
            intensity_solar_output=ci["kWh_solar_output"],
            intensity_wind_output=ci["kWh_wind_output"],
        )
