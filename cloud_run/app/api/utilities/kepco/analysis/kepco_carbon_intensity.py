import pandas as pd
import numpy as np
import requests
import os


def getCarbonIntensityFactors():
    # Get And Calculate Carbon Intensity
    print("Grabbing Intensities")
    response = requests.get(
        "https://api.carbonintensity.org.uk/intensity/factors")

    # Thermal Data: https://www7.tepco.co.jp/fp/thermal-power/list-e.html
    fossilFuelStations = {
        "lng": 4.38 + 3.6 + 3.6 + 5.16 + 3.42 + 3.541 + 1.15 + 2 + 1.14,
        "oil": 5.66 + 1.05 + 4.40,
        "coal": 2
    }
    totalFossil = fossilFuelStations["lng"] + \
        fossilFuelStations["oil"] + fossilFuelStations["coal"]

    json = response.json()
    factors = json["data"][0]

    print("Resolving Intensities for Tokyo")
    # print(factors)
    carbonIntensity = {
        "kWh_nuclear": factors["Nuclear"],
        "kWh_fossil": (factors["Coal"] * fossilFuelStations["coal"] + factors["Oil"] * fossilFuelStations["oil"] + factors["Gas (Open Cycle)"] * fossilFuelStations["lng"]) / totalFossil,
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
    return carbonIntensity


def addCarbonIntensityFactors(df, row_suffix=""):
    carbonIntensity = getCarbonIntensityFactors()

    # Add Carbon Intensity
    print("Calculating Carbon Intensity")
    df["carbon_intensity"] = df.apply(
        lambda row: carbonCalculation(row, carbonIntensity, row_suffix), axis=1)

    return df


def carbonCalculation(row, carbonIntensity, suffix=""):

    # Reference: https://github.com/carbon-intensity/methodology/blob/master/Carbon%20Intensity%20Forecast%20Methodology.pdf
    nuclearIntensity = row["kWh_nuclear{suffix}".format(
        suffix=suffix)] * carbonIntensity["kWh_nuclear"]
    fossilIntensity = row["kWh_fossil{suffix}".format(
        suffix=suffix)] * carbonIntensity["kWh_fossil"]
    hydroIntensity = row["kWh_hydro{suffix}".format(
        suffix=suffix)] * carbonIntensity["kWh_hydro"]
    geothermalIntensity = row["kWh_geothermal{suffix}".format(suffix=suffix)] * \
        carbonIntensity["kWh_geothermal"]
    biomassIntensity = row["kWh_biomass{suffix}".format(
        suffix=suffix)] * carbonIntensity["kWh_biomass"]
    solarIntensity = row["kWh_solar_output{suffix}".format(suffix=suffix)] * \
        carbonIntensity["kWh_solar_output"]
    windIntensity = row["kWh_wind_output{suffix}".format(
        suffix=suffix)] * carbonIntensity["kWh_wind_output"]
    pumpedStorageIntensity = row["kWh_pumped_storage{suffix}".format(suffix=suffix)] * \
        carbonIntensity["kWh_pumped_storage"]
    interconnectorIntensity = row["kWh_interconnectors{suffix}".format(suffix=suffix)] * \
        carbonIntensity["kWh_interconnectors"]

    return (nuclearIntensity + fossilIntensity + hydroIntensity + geothermalIntensity + biomassIntensity + solarIntensity + windIntensity + pumpedStorageIntensity + interconnectorIntensity) / row["kWh_total{suffix}".format(suffix=suffix)]


def createDailyAndMonthlyAverageGroup(df, times):
    # Grouping Functions
    print("Creating Grouped Data")

    # Create a Daily Average of Carbon Intensity Against the Time of Day for all days and years
    dailyAverage = df.groupby([times.hour]).mean()

    # Create a average of the Carbon Intensity for all times in a given month
    monthlyAverage = df.groupby([times.month]).mean()

    return dailyAverage, monthlyAverage


def createDailyAveragePerMonth(df, times):
    # Create a average of the Carbon Intensity for all times in a given month
    dailyAverageByMonth = pd.pivot_table(df, index=[times.hour], columns=[
        times.month], values="carbon_intensity", aggfunc=np.mean)
    dailyAverageByMonth.columns.name = "month"
    dailyAverageByMonth.index.name = "hour"
    return dailyAverageByMonth
