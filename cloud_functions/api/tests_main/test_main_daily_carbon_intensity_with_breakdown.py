import pytest
import json
from main import daily_carbon_intensity_with_breakdown


def test_bad_utility():
    message, code, cors = daily_carbon_intensity_with_breakdown(
        "fish", "breakdown")
    assert message == 'Invalid Utility Specified'
    assert code == 400


def test_bad_breakdown():
    message, code, cors = daily_carbon_intensity_with_breakdown(
        "tepco", "fish")
    assert message == 'Invalid Breakdown Specified'
    assert code == 400


def test_response(mocker):

    def mock_daily_intensity_by_month(self):
        return 'xyz'

    mocker.patch(
        'utilities.tepco.TepcoAPI.TepcoAPI.daily_intensity_by_month',
        mock_daily_intensity_by_month
    )

    body, code, cors = daily_carbon_intensity_with_breakdown(
        "tepco", "month")

    expectedData = {
        "data": "xyz",
        "fromCache": False
    }

    assert body == json.dumps(expectedData)
    assert code == 200
