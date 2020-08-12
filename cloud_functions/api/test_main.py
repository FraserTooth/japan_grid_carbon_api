from main import daily_carbon_intensity


def test_daily_carbon_intensity():
    assert daily_carbon_intensity("fish") == "Fish"
