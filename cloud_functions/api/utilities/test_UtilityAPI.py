import pytest
import requests
import requests_mock
import json
import gc
import pandas as pd
from .UtilityAPI import UtilityAPI

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


@pytest.fixture(autouse=True)
def before_each(requests_mock):
    requests_mock.get(
        "https://api.carbonintensity.org.uk/intensity/factors", text=uk_carbon_intensity_response)


def test_get_carbon_intensity_factors():

    api = UtilityAPI('tepco')

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


def test_gbq_query_string():
    api = UtilityAPI('tepco')

    expected = """
        AVG((
            (kWh_nuclear * 0) + 
            (kWh_fossil * 741.68489817766) + 
            (kWh_hydro * 0) + 
            (kWh_geothermal * 0) + 
            (kWh_biomass * 120) +
            (kWh_solar_output * 0) +
            (kWh_wind_output * 0) +
            (kWh_pumped_storage * 0) +
            (if(kWh_interconnectors > 0,kWh_interconnectors, 0) * 500) 
            ) / kWh_total
            ) as carbon_intensity
        FROM japan-grid-carbon-api.tepco.historical_data_by_generation_type
        """

    assert expected == api._get_intensity_query_string()


def test_daily_intensity(mocker):
    api = UtilityAPI('tepco')

    def mock_daily_intensity(self):
        d = {'hour': [1, 2], 'carbon_intensity': [500, 550]}
        return pd.DataFrame(data=d)

    mocker.patch(
        'pandas.read_gbq',
        mock_daily_intensity
    )

    expected = {
        "carbon_intensity_by_hour": [
            {
                "hour": 1,
                "carbon_intensity": 500,
            },
            {
                "hour": 2,
                "carbon_intensity": 550,
            }
        ]
    }

    assert expected == api.daily_intensity()


def test_daily_intensity_by_month(mocker):
    api = UtilityAPI('tepco')

    def mock_daily_intensity_by_month(self):
        d = {
            'hour': [1, 2, 1, 2],
            'carbon_intensity': [500, 550, 600, 650],
            'month': [1, 1, 2, 2]
        }
        return pd.DataFrame(data=d)

    mocker.patch(
        'pandas.read_gbq',
        mock_daily_intensity_by_month
    )

    expected = {
        "carbon_intensity_by_month": {
            1: [
                {
                    "hour": 1,
                    "carbon_intensity": 500,
                },
                {
                    "hour": 2,
                    "carbon_intensity": 550,
                }
            ],
            2: [
                {
                    "hour": 1,
                    "carbon_intensity": 600,
                },
                {
                    "hour": 2,
                    "carbon_intensity": 650,
                }
            ]
        }
    }

    assert expected == api.daily_intensity_by_month()


def test_daily_intensity_by_month_and_weekday(mocker):
    api = UtilityAPI('tepco')

    def mock_daily_intensity_by_month_and_weekday(self):
        d = {
            'hour': [1, 2, 3, 4],
            'carbon_intensity': [500, 550, 600, 650],
            'month': [1, 1, 2, 2],
            'dayofweek': [1, 2, 1, 2]
        }
        return pd.DataFrame(data=d)

    mocker.patch(
        'pandas.read_gbq',
        mock_daily_intensity_by_month_and_weekday
    )

    expected = {
        "carbon_intensity_by_month_and_weekday": {
            1: {
                1: [
                    {
                        "hour": 1,
                        "carbon_intensity": 500,
                    }
                ],
                2: [
                    {
                        "hour": 2,
                        "carbon_intensity": 550,
                    }
                ]
            },
            2: {
                1: [
                    {
                        "hour": 3,
                        "carbon_intensity": 600,
                    }
                ],
                2: [
                    {
                        "hour": 4,
                        "carbon_intensity": 650,
                    }
                ]
            },
        }
    }

    assert expected == api.daily_intensity_by_month_and_weekday()
