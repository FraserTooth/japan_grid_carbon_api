import pytest
import json
import gc
import os
os.environ["STAGE"] = "staging"

from .main import (daily_carbon_intensity,
                   daily_carbon_intensity_with_breakdown,
                   daily_carbon_intensity_prediction,
                   carbon_intensity_timeseries_prediction,
                   clearCache)


# Before All
@pytest.fixture(autouse=True)
def before_each():
    clearCache('tepco')
    gc.collect()


# Daily Carbon Intensity

def test_daily_carbon_intensity_bad_utility():
    message, code, cors = daily_carbon_intensity(
        "fish")
    assert message == 'Invalid Utility Specified'
    assert code == 400


def test_daily_carbon_intensity_response(mocker):

    mocker.patch(
        'cloud_functions.api.utilities.tepco.TepcoAPI.TepcoAPI.daily_intensity',
        return_value='xyz'
    )

    body, code, cors = daily_carbon_intensity("tepco")

    expectedData = {
        "data": "xyz",
        "fromCache": False
    }

    assert body == json.dumps(expectedData)
    assert code == 200


def test_daily_carbon_intensity_cache(mocker):

    mocker.patch(
        'cloud_functions.api.utilities.tepco.TepcoAPI.TepcoAPI.daily_intensity',
        return_value='xyz'
    )

    body1, code1, cors1 = daily_carbon_intensity("tepco")
    body2, code2, cors2 = daily_carbon_intensity("tepco")

    expectedData1 = {
        "data": "xyz",
        "fromCache": False
    }

    expectedData2 = {
        "data": "xyz",
        "fromCache": True
    }

    assert body1 == json.dumps(expectedData1)
    assert body2 == json.dumps(expectedData2)


# Daily Carbon Intensity by Breakdown


def test_daily_carbon_intensity_with_breakdown_bad_utility():
    message, code, cors = daily_carbon_intensity_with_breakdown(
        "fish", "breakdown")
    assert message == 'Invalid Utility Specified'
    assert code == 400


def test_daily_carbon_intensity_with_breakdown_bad_breakdown():
    message, code, cors = daily_carbon_intensity_with_breakdown(
        "tepco", "fish")
    assert message == 'Invalid Breakdown Specified'
    assert code == 400


def test_daily_carbon_intensity_with_breakdown_response(mocker):

    mocker.patch(
        'cloud_functions.api.utilities.tepco.TepcoAPI.TepcoAPI.daily_intensity_by_month',
        return_value='xyz'
    )

    body, code, cors = daily_carbon_intensity_with_breakdown(
        "tepco", "month")

    expectedData = {
        "data": "xyz",
        "fromCache": False
    }

    assert body == json.dumps(expectedData)
    assert code == 200


def test_daily_carbon_intensity_with_breakdown_cache(mocker):

    mocker.patch(
        'cloud_functions.api.utilities.tepco.TepcoAPI.TepcoAPI.daily_intensity_by_month',
        return_value='xyz'
    )

    body1, code1, cors1 = daily_carbon_intensity_with_breakdown(
        "tepco", "month")
    body2, code2, cors2 = daily_carbon_intensity_with_breakdown(
        "tepco", "month")

    expectedData1 = {
        "data": "xyz",
        "fromCache": False
    }

    expectedData2 = {
        "data": "xyz",
        "fromCache": True
    }

    assert body1 == json.dumps(expectedData1)
    assert body2 == json.dumps(expectedData2)


# Daily Carbon Intensity Predictions


def test_daily_carbon_intensity_predictions_bad_utility():
    message, code, cors = daily_carbon_intensity_prediction(
        "fish", 2030)
    assert message == 'Invalid Utility Specified'
    assert code == 400


def test_daily_carbon_intensity_predictions_bad_year():
    message, code, cors = daily_carbon_intensity_prediction(
        "tepco", 2000)
    assert message == 'Invalid Year Specified - must be between this year and 50 from now'
    assert code == 400


def test_daily_carbon_intensity_predictions_response(mocker):

    mocker.patch(
        'cloud_functions.api.utilities.tepco.TepcoAPI.TepcoAPI.daily_intensity_prediction_for_year_by_month_and_weekday',
        return_value='xyz'
    )

    body, code, cors = daily_carbon_intensity_prediction(
        "tepco", 2030)

    expectedData = {
        "data": "xyz",
        "fromCache": False
    }

    assert body == json.dumps(expectedData)
    assert code == 200


def test_daily_carbon_intensity_predictions_cache(mocker):

    mocker.patch(
        'cloud_functions.api.utilities.tepco.TepcoAPI.TepcoAPI.daily_intensity_prediction_for_year_by_month_and_weekday',
        return_value='xyz'
    )

    body1, code1, cors1 = daily_carbon_intensity_prediction(
        "tepco", 2030)
    body2, code2, cors2 = daily_carbon_intensity_prediction(
        "tepco", 2030)

    expectedData1 = {
        "data": "xyz",
        "fromCache": False
    }

    expectedData2 = {
        "data": "xyz",
        "fromCache": True
    }

    assert body1 == json.dumps(expectedData1)
    assert body2 == json.dumps(expectedData2)

# Carbon Intensity Timeseries Predictions


def test_carbon_intensity_timeseries_prediction_bad_utility():
    message, code, cors = carbon_intensity_timeseries_prediction(
        "fish")
    assert message == 'Invalid Utility Specified'
    assert code == 400


def test_carbon_intensity_timeseries_prediction_response(mocker):

    mocker.patch(
        'cloud_functions.api.utilities.tepco.TepcoAPI.TepcoAPI.timeseries_prediction',
        return_value='xyz'
    )

    body, code, cors = carbon_intensity_timeseries_prediction(
        "tepco")

    expectedData = {
        "data": "xyz",
        "fromCache": False
    }

    assert body == json.dumps(expectedData)
    assert code == 200


def test_carbon_intensity_timeseries_prediction_cache(mocker):

    mocker.patch(
        'cloud_functions.api.utilities.tepco.TepcoAPI.TepcoAPI.timeseries_prediction',
        return_value='xyz'
    )

    body1, code1, cors1 = carbon_intensity_timeseries_prediction(
        "tepco")
    body2, code2, cors2 = carbon_intensity_timeseries_prediction(
        "tepco")

    expectedData1 = {
        "data": "xyz",
        "fromCache": False
    }

    expectedData2 = {
        "data": "xyz",
        "fromCache": True
    }

    assert body1 == json.dumps(expectedData1)
    assert body2 == json.dumps(expectedData2)
