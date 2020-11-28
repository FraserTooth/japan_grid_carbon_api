import pytest
import requests
import requests_mock
import json
import gc
import pandas as pd
from .UtilityAPI import UtilityAPI


def test_get_carbon_intensity_factors():

    api = UtilityAPI('tepco')

    expected = {
        "kWh_nuclear": 19,
        "kWh_fossil": 718.3333333333334,
        "kWh_hydro": 11,
        "kWh_geothermal": 13,
        "kWh_biomass": 120,
        "kWh_solar_output": 59,
        "kWh_wind_output": 26,
        "kWh_pumped_storage": 80.07,
        # TODO: Replace this with a rolling calculation of the average of other parts of Japan's carbon intensity, probably around 850 though
        "kWh_interconnectors": 500
    }

    assert expected == api.get_carbon_intensity_factors()


def test_gbq_query_string():
    api = UtilityAPI('tepco')

    expected = """
        AVG(
            
        (
            (daMWh_nuclear * 19) +
            (daMWh_fossil * 718.3333333333334) +
            (daMWh_hydro * 11) +
            (daMWh_geothermal * 13) +
            (daMWh_biomass * 120) +
            (daMWh_solar_output * 59) +
            (daMWh_wind_output * 26) +
            (daMWh_pumped_storage_contribution * 80.07) +
            (daMWh_interconnector_contribution * 500)
        ) / daMWh_total_generation
        
        ) as carbon_intensity
        FROM (
            
            SELECT *,
            (daMWh_nuclear + daMWh_fossil + daMWh_hydro + daMWh_geothermal + daMWh_biomass + daMWh_solar_output + daMWh_wind_output + daMWh_pumped_storage_contribution + daMWh_interconnector_contribution) as daMWh_total_generation
            FROM (
                SELECT *,
                if(daMWh_interconnectors > 0,daMWh_interconnectors, 0) as daMWh_interconnector_contribution,
                if(daMWh_pumped_storage > 0,daMWh_pumped_storage, 0) as daMWh_pumped_storage_contribution,
                FROM `japan-grid-carbon-api-staging.tepco.historical_data_by_generation_type`
            )
        
        )
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
        "carbon_intensity_average": {
            "breakdown": "hour",
            "data": [
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
        "carbon_intensity_average": {
            "breakdown": "month",
            "data": [
                {
                    "month": 1,
                    "data": [
                        {
                            "hour": 1,
                            "carbon_intensity": 500,
                        },
                        {
                            "hour": 2,
                            "carbon_intensity": 550,
                        }
                    ]
                },
                {
                    "month": 2,
                    "data": [
                        {
                            "hour": 1,
                            "carbon_intensity": 600,
                        },
                        {
                            "hour": 2,
                            "carbon_intensity": 650,
                        }
                    ]
                },
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


def test_daily_intensity_by_year_month_and_weekday(mocker):
    api = UtilityAPI('tepco')

    def test_daily_intensity_by_year_month_and_weekday(self):
        d = {
            'hour': [1, 2, 3, 4],
            'year': [2016, 2016, 2017, 2017],
            'carbon_intensity': [500, 550, 600, 650],
            'month': [1, 1, 2, 2],
            'dayofweek': [1, 2, 1, 2]
        }
        return pd.DataFrame(data=d)

    mocker.patch(
        'pandas.read_gbq',
        test_daily_intensity_by_year_month_and_weekday
    )

    expected = {
        "carbon_intensity_by_year_month_and_weekday": {
            2016: {
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
            },
            2017: {
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
    }

    assert expected == api.daily_intensity_by_year_month_and_weekday()


def test_daily_intensity_prediction_for_year_by_month_and_weekday(mocker):
    api = UtilityAPI('tepco')

    def mock_daily_intensity_prediction_for_year_by_month_and_weekday(self):
        d = {
            'hour': [1, 2, 3, 4],
            'predicted_carbon_intensity': [500, 550, 600, 650],
            'year': [2030, 2030, 2030, 2030],
            'month': [1, 1, 2, 2],
            'dayofweek': [1, 2, 1, 2]
        }
        return pd.DataFrame(data=d)

    mocker.patch(
        'pandas.read_gbq',
        mock_daily_intensity_prediction_for_year_by_month_and_weekday
    )

    expected = {
        "prediction_year": 2030,
        "carbon_intensity_by_month_and_weekday": {
            1: {
                1: [
                    {
                        "hour": 1,
                        "predicted_carbon_intensity": 500,
                    }
                ],
                2: [
                    {
                        "hour": 2,
                        "predicted_carbon_intensity": 550,
                    }
                ]
            },
            2: {
                1: [
                    {
                        "hour": 3,
                        "predicted_carbon_intensity": 600,
                    }
                ],
                2: [
                    {
                        "hour": 4,
                        "predicted_carbon_intensity": 650,
                    }
                ]
            },
        }
    }

    assert expected == api.daily_intensity_prediction_for_year_by_month_and_weekday(
        2030)


def test_daily_intensity_by_year(mocker):
    api = UtilityAPI('tepco')

    def test_daily_intensity_by_year(self):
        d = {
            'hour': [1, 2, 3, 4],
            'year': [2016, 2016, 2017, 2017],
            'carbon_intensity': [500, 550, 600, 650],
        }
        return pd.DataFrame(data=d)

    mocker.patch(
        'pandas.read_gbq',
        test_daily_intensity_by_year
    )

    expected = {
        "carbon_intensity_average": {
            "breakdown": "year",
            "data": [
                {
                    "year": 2016,
                    "data": [
                        {
                            "hour": 1,
                            "carbon_intensity": 500,
                        },
                        {
                            "hour": 2,
                            "carbon_intensity": 550,
                        }
                    ]
                },
                {
                    "year": 2017,
                    "data": [
                        {
                            "hour": 3,
                            "carbon_intensity": 600,
                        },
                        {
                            "hour": 4,
                            "carbon_intensity": 650,
                        }
                    ]
                },
            ]


        }
    }

    assert expected == api.daily_intensity_by_year()


def test_historic_data(mocker):
    api = UtilityAPI('tepco')

    def test_historic_data(self):
        d = {
            'timestamp': [
                "2020-11-01 00:00:00+00:00",
                "2020-11-01 01:00:00+00:00",
                "2020-11-01 02:00:00+00:00",
                "2020-11-01 03:00:00+00:00"
            ],
            'carbon_intensity': [500, 550, 600, 650],
        }
        return pd.DataFrame(data=d)

    mocker.patch(
        'pandas.read_gbq',
        test_historic_data
    )

    expected = {
        "historic": [
            {
                "timestamp": "2020-11-01 00:00:00+00:00",
                "carbon_intensity": 500,
            },
            {
                "timestamp": "2020-11-01 01:00:00+00:00",
                "carbon_intensity": 550,
            },
            {
                "timestamp": "2020-11-01 02:00:00+00:00",
                "carbon_intensity": 600,
            },
            {
                "timestamp": "2020-11-01 03:00:00+00:00",
                "carbon_intensity": 650,
            },
        ]
    }

    assert expected == api.historic_intensity('2020-11-01', '2020-11-02')
