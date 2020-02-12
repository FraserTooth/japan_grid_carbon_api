import csv
import requests
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

CSV_URL = 'http://www.tepco.co.jp/forecast/html/images/area-2019.csv'


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
        "太陽光発電実績": "kWh_solar_record",
        "太陽光出力制御量": "kWh_solar_output",
        "風力発電実績": "kWh_wind_record",
        "風力出力制御量": "kWh_wind_output",
        "揚水": "kWh_wind_output",
        "風力出力制御量": "kWh_pumped_storage",
        "連系線": "kWh_interconnectors",
        "合計": "kWh_total"
    }

    if header in translations:
        return translations[header]
    return header


df = pd.read_csv(CSV_URL, skiprows=2, encoding="cp932", parse_dates=[[0, 1]])

df = df.rename(columns=lambda x: renameHeader(x), errors="raise")
# headers = renameHeaders(energyData[2])

plot = df.plot.line(x="datetime")
fig = plot.get_figure()
fig.savefig("scraper/plots/test.png")
