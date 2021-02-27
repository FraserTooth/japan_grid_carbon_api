import requests
from ..UtilityAPI import UtilityAPI


config_rikuden = {
    "pumped_storage_factor": 80.07,

    # Thermal Energy Percentages:http://www.rikuden.co.jp/eng_ir/attach/integratedreport2019-1.pdf
    "fuel_type_totals": {
        "lng": 0.4247,
        "oil": 0.25 + 0.5 + 0.5,
        "coal": 0.5 + 0.7 + 0.25 + 0.5 + 0.7 + 0.25 + 0.25
    }
}


class RikudenAPI(UtilityAPI):
    def __init__(self):
        super().__init__("rikuden", config_rikuden)

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
