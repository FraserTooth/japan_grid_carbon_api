import pytest
import requests
import requests_mock
import json
import gc
from utilities.UtilityAPI import UtilityAPI

uk_carbon_intensity_response = json.dumps({
    "data": [
        {
            "Biomass": 120,
            "Coal": 937,
            "Dutch Imports": 474,
            "French Imports": 53,
            "Gas (Combined Cycle)": 394,
            "Gas (Open Cycle)": 651,
            "Hydro": 0,
            "Irish Imports": 458,
            "Nuclear": 0,
            "Oil": 935,
            "Other": 300,
            "Pumped Storage": 0,
            "Solar": 0,
            "Wind": 0
        }
    ]
})


def test_get_carbon_intensity_factors(requests_mock):

    api = UtilityAPI('tepco')

    requests_mock.get(
        "https://api.carbonintensity.org.uk/intensity/factors", text=uk_carbon_intensity_response)

    expected = {
        "kWh_nuclear": 0,
        "kWh_fossil": 741.68489817766,
        "kWh_hydro": 0,
        "kWh_geothermal": 0,
        "kWh_biomass": 120,
        "kWh_solar_output": 0,
        "kWh_wind_output": 0,
        "kWh_pumped_storage": 0,
        # TODO: Replace this with a rolling calculation of the average of other parts of Japan's carbon intensity, probably around 850 though
        "kWh_interconnectors": 500
    }

    assert expected == api.get_carbon_intensity_factors()
