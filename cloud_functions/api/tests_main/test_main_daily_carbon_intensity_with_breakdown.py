import pytest
from main import daily_carbon_intensity_with_breakdown


def test_daily_carbon_intensity_breakdown_bad_utility():
    message, code, cors = daily_carbon_intensity_with_breakdown(
        "fish", "breakdown")
    assert message == 'Invalid utility specified'
    assert code == 400
