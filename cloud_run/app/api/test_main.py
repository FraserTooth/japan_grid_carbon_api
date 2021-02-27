import pytest
from fastapi import HTTPException
import json
import gc
import os
os.environ["STAGE"] = "staging"

from .main import (
    daily_carbon_intensity,
    daily_carbon_intensity_with_breakdown,
    daily_carbon_intensity_prediction,
    carbon_intensity_timeseries_prediction,
    clearCache,
    generate_standard_error_model,
    historical_intensity
)

# client = TestClient(app)

# Before All


@pytest.fixture(autouse=True)
def before_each():
    clearCache('tepco')
    gc.collect()


# Daily Carbon Intensity

def test_daily_carbon_intensity_bad_utility():
    with pytest.raises(HTTPException) as error:
        daily_carbon_intensity("fish")
    assert error.value.detail == generate_standard_error_model(
        'Invalid Utility Specified', 400)


def test_daily_carbon_intensity_response(mocker):

    mocker.patch(
        'app.api.utilities.tepco.TepcoAPI.TepcoAPI.daily_intensity',
        return_value='xyz'
    )

    body = daily_carbon_intensity("tepco")

    expectedData = {
        "data": "xyz",
        "fromCache": False
    }

    assert body == expectedData


def test_daily_carbon_intensity_cache(mocker):

    mocker.patch(
        'app.api.utilities.tepco.TepcoAPI.TepcoAPI.daily_intensity',
        return_value='xyz'
    )

    body1 = daily_carbon_intensity("tepco")
    body2 = daily_carbon_intensity("tepco")

    expectedData1 = {
        "data": "xyz",
        "fromCache": False
    }

    expectedData2 = {
        "data": "xyz",
        "fromCache": True
    }

    assert body1 == expectedData1
    assert body2 == expectedData2


# Daily Carbon Intensity by Breakdown


def test_daily_carbon_intensity_with_breakdown_bad_utility():
    with pytest.raises(HTTPException) as error:
        daily_carbon_intensity_with_breakdown(
            "fish", "breakdown")
    assert error.value.detail == generate_standard_error_model(
        'Invalid Utility Specified', 400)


def test_daily_carbon_intensity_with_breakdown_bad_breakdown():
    with pytest.raises(HTTPException) as error:
        daily_carbon_intensity_with_breakdown(
            "tepco", "fish")
    assert error.value.detail == generate_standard_error_model(
        'Invalid Breakdown Specified', 400)


def test_daily_carbon_intensity_with_breakdown_response(mocker):

    mocker.patch(
        'app.api.utilities.tepco.TepcoAPI.TepcoAPI.daily_intensity_by_month',
        return_value='xyz'
    )

    body = daily_carbon_intensity_with_breakdown(
        "tepco", "month")

    expectedData = {
        "data": "xyz",
        "fromCache": False
    }

    assert body == expectedData


def test_daily_carbon_intensity_with_breakdown_cache(mocker):

    mocker.patch(
        'app.api.utilities.tepco.TepcoAPI.TepcoAPI.daily_intensity_by_month',
        return_value='xyz'
    )

    body1 = daily_carbon_intensity_with_breakdown(
        "tepco", "month")
    body2 = daily_carbon_intensity_with_breakdown(
        "tepco", "month")

    expectedData1 = {
        "data": "xyz",
        "fromCache": False
    }

    expectedData2 = {
        "data": "xyz",
        "fromCache": True
    }

    assert body1 == expectedData1
    assert body2 == expectedData2


# Daily Carbon Intensity Predictions


def test_daily_carbon_intensity_predictions_bad_utility():
    with pytest.raises(HTTPException) as error:
        daily_carbon_intensity_prediction(
            "fish", 2030)
    assert error.value.detail == generate_standard_error_model(
        'Invalid Utility Specified', 400)


def test_daily_carbon_intensity_predictions_bad_year():
    with pytest.raises(HTTPException) as error:
        daily_carbon_intensity_prediction(
            "tepco", 2000)
    assert error.value.detail == generate_standard_error_model(
        'Invalid Year Specified - must be between this year and 50 from now', 400)


def test_daily_carbon_intensity_predictions_response(mocker):

    mocker.patch(
        'app.api.utilities.tepco.TepcoAPI.TepcoAPI.daily_intensity_prediction_for_year_by_month_and_weekday',
        return_value='xyz'
    )

    body = daily_carbon_intensity_prediction(
        "tepco", 2030)

    expectedData = {
        "data": "xyz",
        "fromCache": False
    }

    assert body == expectedData


def test_daily_carbon_intensity_predictions_cache(mocker):

    mocker.patch(
        'app.api.utilities.tepco.TepcoAPI.TepcoAPI.daily_intensity_prediction_for_year_by_month_and_weekday',
        return_value='xyz'
    )

    body1 = daily_carbon_intensity_prediction(
        "tepco", 2030)
    body2 = daily_carbon_intensity_prediction(
        "tepco", 2030)

    expectedData1 = {
        "data": "xyz",
        "fromCache": False
    }

    expectedData2 = {
        "data": "xyz",
        "fromCache": True
    }

    assert body1 == expectedData1
    assert body2 == expectedData2

# Carbon Intensity Timeseries Predictions


def test_carbon_intensity_timeseries_prediction_bad_utility():
    with pytest.raises(HTTPException) as error:
        carbon_intensity_timeseries_prediction(
            "fish", "2020-01-01")
    assert error.value.detail == generate_standard_error_model(
        'Invalid Utility Specified', 400)


def test_carbon_intensity_timeseries_prediction_bad_date_from():
    with pytest.raises(HTTPException) as error:
        carbon_intensity_timeseries_prediction(
            "tepco", "fish")
    assert error.value.detail == generate_standard_error_model(
        'Invalid FROM Date Provided', 400)


def test_carbon_intensity_timeseries_prediction_bad_date_to():
    with pytest.raises(HTTPException) as error:
        carbon_intensity_timeseries_prediction(
            "tepco", "2020-01-02", "fish")
    assert error.value.detail == generate_standard_error_model(
        'Invalid TO Date Provided', 400)


def test_carbon_intensity_timeseries_prediction_to_before_from():
    with pytest.raises(HTTPException) as error:
        carbon_intensity_timeseries_prediction(
            "tepco", "2020-01-02", "2020-01-01")
    assert error.value.detail == generate_standard_error_model(
        'Invalid Query - TO Date before FROM Date', 400)


def test_carbon_intensity_timeseries_prediction_response(mocker):

    mocker.patch(
        'app.api.utilities.tepco.TepcoAPI.TepcoAPI.timeseries_prediction',
        return_value='xyz'
    )

    body = carbon_intensity_timeseries_prediction(
        "tepco", "2020-01-01")

    expectedData = {
        "data": "xyz",
        "fromCache": False
    }

    assert body == expectedData


def test_carbon_intensity_timeseries_prediction_cache(mocker):

    mocker.patch(
        'app.api.utilities.tepco.TepcoAPI.TepcoAPI.timeseries_prediction',
        return_value='xyz'
    )

    body1 = carbon_intensity_timeseries_prediction(
        "tepco", "2020-01-01")
    body2 = carbon_intensity_timeseries_prediction(
        "tepco", "2020-01-01")

    expectedData1 = {
        "data": "xyz",
        "fromCache": False
    }

    expectedData2 = {
        "data": "xyz",
        "fromCache": True
    }

    assert body1 == expectedData1
    assert body2 == expectedData2


# Carbon Intensity Historic Intensities


def test_carbon_intensity_historical_bad_utility():
    with pytest.raises(HTTPException) as error:
        historical_intensity(
            "fish", "2020-01-02", "2020-02-02")
    assert error.value.detail == generate_standard_error_model(
        'Invalid Utility Specified', 400)


def test_carbon_intensity_historical_bad_date_from():
    with pytest.raises(HTTPException) as error:
        historical_intensity(
            "tepco", "fish")
    assert error.value.detail == generate_standard_error_model(
        'Invalid FROM Date Provided', 400)


def test_carbon_intensity_historical_bad_date_to():
    with pytest.raises(HTTPException) as error:
        historical_intensity(
            "tepco", "2020-01-02", "fish")
    assert error.value.detail == generate_standard_error_model(
        'Invalid TO Date Provided', 400)


def test_carbon_intensity_historical_response(mocker):

    mocker.patch(
        'app.api.utilities.tepco.TepcoAPI.TepcoAPI.historic_intensity',
        return_value='xyz'
    )

    body = historical_intensity(
        "tepco", "2020-01-02", "2020-02-02")

    expectedData = {
        "data": "xyz",
        "fromCache": False
    }

    assert body == expectedData


def test_carbon_intensity_historical_cache(mocker):

    mocker.patch(
        'app.api.utilities.tepco.TepcoAPI.TepcoAPI.historic_intensity',
        return_value='xyz'
    )

    body1 = historical_intensity(
        "tepco", "2020-01-02", "2020-02-02")
    body2 = historical_intensity(
        "tepco", "2020-01-02", "2020-02-02")

    expectedData1 = {
        "data": "xyz",
        "fromCache": False
    }

    expectedData2 = {
        "data": "xyz",
        "fromCache": True
    }

    assert body1 == expectedData1
    assert body2 == expectedData2
