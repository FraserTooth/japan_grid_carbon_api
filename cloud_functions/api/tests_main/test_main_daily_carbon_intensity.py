import pytest
from main import daily_carbon_intensity


def test_bad_utility():
    message, code, cors = daily_carbon_intensity(
        "fish")
    assert message == 'Invalid utility specified'
    assert code == 400
