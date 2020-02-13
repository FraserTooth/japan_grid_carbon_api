import csv
import requests
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

CSV_URL = 'http://www.tepco.co.jp/forecast/html/images/area-2019.csv'


# Get And Calculate Carbon Intensity
response = requests.get(
    "https://api.carbonintensity.org.uk/intensity/factors")

# Thermal Data: https://www7.tepco.co.jp/fp/thermal-power/list-e.html
fossilFuelStations = {
    "lng": 4.38+3.6+3.6+5.16+3.42+3.541+1.15+2+1.14,
    "oil": 5.66+1.05+4.40,
    "coal": 2
}
totalFossil = fossilFuelStations["lng"] + \
    fossilFuelStations["oil"]+fossilFuelStations["coal"]

json = response.json()
factors = json["data"][0]
print(factors)
carbonIntensity = {
    "kWh_nuclear": factors["Nuclear"],
    "kWh_fossil": (factors["Coal"]*fossilFuelStations["coal"] + factors["Oil"]*fossilFuelStations["oil"] + factors["Gas (Open Cycle)"]*fossilFuelStations["lng"])/totalFossil,
    "kWh_hydro": factors["Hydro"],
    "kWh_geothermal": 0,  # Probably
    "kWh_biomass": factors["Biomass"],
    "kWh_solar_output": factors["Solar"],
    "kWh_wind_output": factors["Wind"],
    "kWh_pumped_storage": factors["Pumped Storage"],
    # TODO: Replace this with a rolling calculation of the average of other parts of Japan's carbon intensity, probably around 850 though
    "kWh_interconnectors": 850
}
# print(carbonIntensity)


def carbonCalculation(row):
    # Reference: https://github.com/carbon-intensity/methodology/blob/master/Carbon%20Intensity%20Forecast%20Methodology.pdf
    nuclearIntensity = row["kWh_nuclear"] * carbonIntensity["kWh_nuclear"]
    fossilIntensity = row["kWh_fossil"] * carbonIntensity["kWh_fossil"]
    hydroIntensity = row["kWh_hydro"] * carbonIntensity["kWh_hydro"]
    geothermalIntensity = row["kWh_geothermal"] * \
        carbonIntensity["kWh_geothermal"]
    biomassIntensity = row["kWh_biomass"] * carbonIntensity["kWh_biomass"]
    solarIntensity = row["kWh_solar_output"] * \
        carbonIntensity["kWh_solar_output"]
    windIntensity = row["kWh_wind_output"] * carbonIntensity["kWh_wind_output"]
    pumpedStorageIntensity = row["kWh_pumped_storage"] * \
        carbonIntensity["kWh_pumped_storage"]
    interconnectorIntensity = row["kWh_interconnectors"] * \
        carbonIntensity["kWh_interconnectors"]

    return (nuclearIntensity + fossilIntensity + hydroIntensity + geothermalIntensity + biomassIntensity + solarIntensity + windIntensity + pumpedStorageIntensity + interconnectorIntensity) / row["kWh_total"]


def renameHeader(header):

    translations = {
        "Unnamed: 0": "date",
        "Unnamed: 1": "time",
        "Unnamed: 0_Unnamed: 1": "datetime",
        "Unnamed: 2": "kWh_demand",
        "原子力": "kWh_nuclear",
        "火力": "kWh_fossil",
        "水力": "kWh_hydro",
        "地熱": "kWh_geothermal",
        "バイオマス": "kWh_biomass",
        "太陽光発電実績": "kWh_solar_output",
        "太陽光出力制御量": "kWh_solar_throttling",
        "風力発電実績": "kWh_wind_output",
        "風力出力制御量": "kWh_wind_throttling",
        "揚水": "kWh_pumped_storage",
        "連系線": "kWh_interconnectors",
        "合計": "kWh_total"
    }

    if header in translations:
        return translations[header]
    return header


print("Reading CSV")
df = pd.read_csv(CSV_URL, skiprows=2, encoding="cp932",
                 parse_dates=[[0, 1]])

print("Renaming Columns")
df = df.rename(columns=lambda x: renameHeader(x), errors="raise")
# headers = renameHeaders(energyData[2])

print("Calculating Carbon Intensity")
df["carbon_intensity"] = df.apply(lambda row: carbonCalculation(row), axis=1)
# print(df)

# Create a Daily Average of Carbon Intensity Against the Time of Day
print("Creating Daily Averages")
times = pd.DatetimeIndex(df.datetime)
# print(times.hour)
dailyAverage = df.groupby([times.hour]).mean()
# print(dailyAverage)

# Plot Year's Carbon Intensity
plot = df.plot.line(x="datetime", y="carbon_intensity")
fig = plot.get_figure()
fig.savefig("scraper/plots/test.png")

# Plot Daily Carbon Intensity
dailyPlot = dailyAverage.plot.line(y="carbon_intensity")
dailyfig = dailyPlot.get_figure()
dailyfig.savefig("scraper/plots/dailytest.png")
