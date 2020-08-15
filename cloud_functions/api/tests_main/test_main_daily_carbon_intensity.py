import pytest
import json
import gc
from main import daily_carbon_intensity, clearCache


@pytest.fixture(autouse=True)
def before_each():
    clearCache('tepco')
    gc.collect()


def test_bad_utility():
    message, code, cors = daily_carbon_intensity(
        "fish")
    assert message == 'Invalid Utility Specified'
    assert code == 400


def test_response(mocker):

    def mock_daily_intensity(self):
        return 'xyz'

    mocker.patch(
        'utilities.tepco.TepcoAPI.TepcoAPI.daily_intensity',
        mock_daily_intensity
    )

    body, code, cors = daily_carbon_intensity("tepco")

    expectedData = {
        "data": "xyz",
        "fromCache": False
    }

    assert body == json.dumps(expectedData)
    assert code == 200


def test_cache(mocker):

    def mock_daily_intensity(self):
        return 'xyz'

    mocker.patch(
        'utilities.tepco.TepcoAPI.TepcoAPI.daily_intensity',
        mock_daily_intensity
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
