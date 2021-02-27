import requests
from ..UtilityAPI import UtilityAPI

config_kyuden = {
    "pumped_storage_factor": 8.57,

    # Thermal Data: https://www.kyuden.co.jp/var/rev0/0220/1476/c57wp8gc_16.pdf
    "fuel_type_totals": {
        "lng": 1.8 + 2.825,
        "oil": 1 + 0.875 + 1,
        "coal": 0.36 + 0.7 + 1.4
    }
}


class KyudenAPI(UtilityAPI):
    def __init__(self):
        super().__init__("kyuden", config_kyuden)

    def _pumped_storage_calc_query_string(self):
        return """
            SELECT *,
            (MWh_nuclear + MWh_fossil + MWh_hydro + MWh_geothermal + MWh_biomass + MWh_solar_output + MWh_wind_output + MWh_pumped_storage_contribution + MWh_interconnector_contribution) as MWh_total_generation
            FROM (
                SELECT *,
                if(MWh_interconnectors > 0,MWh_interconnectors, 0) as MWh_interconnector_contribution,
                if(MWh_pumped_storage > 0,MWh_pumped_storage, 0) as MWh_pumped_storage_contribution,
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
