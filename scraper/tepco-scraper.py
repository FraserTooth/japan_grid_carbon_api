import csv
import requests
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

CSV_URL_2019 = 'http://www.tepco.co.jp/forecast/html/images/area-2019.csv'
CSV_URL_2018 = 'http://www.tepco.co.jp/forecast/html/images/area-2018.csv'
CSV_URL_2017 = 'http://www.tepco.co.jp/forecast/html/images/area-2017.csv'
CSV_URL_2016 = 'http://www.tepco.co.jp/forecast/html/images/area-2016.csv'


# Get And Calculate Carbon Intensity
print("Grabbing Intensities")
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

print("Resolving Intensities for Tokyo")
# print(factors)
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


dtypes = {
    "Unnamed: 2": int,
    "火力": int,
    "水力": int,
    "地熱": int,
    "バイオマス": int,
    "太陽光発電実績": int,
    "太陽光出力制御量": int,
    "風力発電実績": int,
    "風力出力制御量": int,
    "揚水": int,
    "連系線": int,
    "合計": int
}

print("Reading CSV")
print("---2019")
df2019 = pd.read_csv(CSV_URL_2019, skiprows=2, encoding="cp932",
                     parse_dates=[[0, 1]], dtype=dtypes, thousands=",")
df2018 = pd.read_csv(CSV_URL_2018, skiprows=2, encoding="cp932",
                     parse_dates=[[0, 1]], dtype=dtypes, thousands=",")
print("---2018")
df2017 = pd.read_csv(CSV_URL_2017, skiprows=2, encoding="cp932",
                     parse_dates=[[0, 1]], dtype=dtypes, thousands=",")
print("---2017")
df2016 = pd.read_csv(CSV_URL_2016, skiprows=2, encoding="cp932",
                     parse_dates=[[0, 1]], dtype=dtypes, thousands=",")
print("---2016")

df = pd.concat([df2016, df2017, df2018, df2019])

# Translate Column Headers
print("Renaming Columns")
df = df.rename(columns=lambda x: renameHeader(x), errors="raise")

print("Calculating Carbon Intensity")
df["carbon_intensity"] = df.apply(lambda row: carbonCalculation(row), axis=1)

# Grouping Functions
print("Creating Grouped Data")
# Allow Timebased Breakdowns against date facts
times = pd.DatetimeIndex(df.datetime)

# Create a Daily Average of Carbon Intensity Against the Time of Day for all days and years
dailyAverage = df.groupby([times.hour]).mean()

# Create a average of the Carbon Intensity for all times in a given month
monthlyAverage = df.groupby([times.month]).mean()

# Create a average of the Carbon Intensity for all times in a given month
dailyAverageByMonth = pd.pivot_table(df, index=[times.hour], columns=[
    times.month], values="carbon_intensity", aggfunc=np.mean)
dailyAverageByMonth.columns.name = "month"
dailyAverageByMonth.index.name = "hour"

print("Creating Plots")
# Plot Year's Carbon Intensity
plot = df.plot.line(x="datetime", y="carbon_intensity")
fig = plot.get_figure()
fig.savefig("scraper/plots/test.png")

# Plot Daily Carbon Intensity
dailyPlot = dailyAverage.plot.line(y="carbon_intensity")
dailyfig = dailyPlot.get_figure()
dailyfig.savefig("scraper/plots/dailytest.png")

# Plot Average Carbon Intensity In Month
monthlyPlot = monthlyAverage.plot.bar(y="carbon_intensity")
monthlyPlot.set_ylim(
    monthlyAverage["carbon_intensity"].min()*0.9, monthlyAverage["carbon_intensity"].max()*1.1)
monthlyfig = monthlyPlot.get_figure()
monthlyfig.savefig("scraper/plots/monthlytest.png")


# Plot Daily Carbon Intensity for Each Month
# NICE FORMATTING STUFF
# These are the "Tableau 20" colors as RGB.
tableau20 = [(31, 119, 180), (174, 199, 232), (255, 127, 14), (255, 187, 120),
             (44, 160, 44), (152, 223, 138), (214, 39, 40), (255, 152, 150),
             (148, 103, 189), (197, 176, 213), (140, 86, 75), (196, 156, 148),
             (227, 119, 194), (247, 182, 210), (127, 127, 127), (199, 199, 199),
             (188, 189, 34), (219, 219, 141), (23, 190, 207), (158, 218, 229)]

# Scale the RGB values to the [0, 1] range, which is the format matplotlib accepts.
for i in range(len(tableau20)):
    r, g, b = tableau20[i]
    tableau20[i] = (r / 255., g / 255., b / 255.)

dailyMonthPlot = dailyAverageByMonth.plot.line(
    color=tableau20, title='Carbon Intensity over a Given Day, By Month (2016-Now)')

dailyMonthPlot.set_xlabel('Hour')
dailyMonthPlot.set_ylabel('CO2ge/kWh')
dailyMonthfig = dailyMonthPlot.get_figure()
dailyMonthfig.savefig("scraper/plots/dailyMonthTest.png")
