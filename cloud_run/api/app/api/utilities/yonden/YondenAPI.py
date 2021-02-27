import requests
from ..UtilityAPI import UtilityAPI

config_yonden = {
    "pumped_storage_factor": 8.57,

    # Thermal Data: https://www.yonden.co.jp/english/assets/pdf/ir/tools/ann_r/annual_e_2019.pdf
    "fuel_type_totals": {
        "lng": 0.296 + 0.289 + 0.35,
        "oil": 0.45 + 0.45 + 0.45,
        "coal": 0.156 + 0.25 + 0.7
    }
}


class YondenAPI(UtilityAPI):
    def __init__(self):
        super().__init__("yonden", config_yonden)

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
