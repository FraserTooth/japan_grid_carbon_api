import pytest
import json
from main import daily_carbon_intensity


def test_bad_utility():
    message, code, cors = daily_carbon_intensity(
        "fish")
    assert message == 'Invalid utility specified'
    assert code == 400


def test_response(mocker):

    def mock_daily_intensity(self):
        return 'xyz'

    mocker.patch(
        'utilities.tepco.TepcoAPI.TepcoAPI.daily_intensity',
        mock_daily_intensity
    )

    body, code, cors = daily_carbon_intensity(
        "tepco")

    expectedData = {
        "data": "xyz",
        "fromCache": False
    }

    assert body == json.dumps(expectedData)
    assert code == 200
