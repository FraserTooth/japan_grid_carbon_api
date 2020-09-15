import pytest
import json
import gc
from .main import daily_carbon_intensity, daily_carbon_intensity_with_breakdown, clearCache


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
