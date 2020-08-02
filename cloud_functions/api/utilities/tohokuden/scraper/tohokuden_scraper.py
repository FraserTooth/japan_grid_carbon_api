import csv
import requests
import numpy as np
import pandas as pd
import datetime


def _renameHeader(header):

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


def _parseTohokudenCsvs():
    CSV_URL_DAILY = 'https://www.tepco.co.jp/forecast/html/images/juyo-d-j.csv'

    CSV_URLS = [
        'http://www.tepco.co.jp/forecast/html/images/area-2016.csv',
        'http://www.tepco.co.jp/forecast/html/images/area-2017.csv',
        'http://www.tepco.co.jp/forecast/html/images/area-2018.csv',
        'http://www.tepco.co.jp/forecast/html/images/area-2019.csv',
        'http://www.tepco.co.jp/forecast/html/images/area-2020.csv'
    ]

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

    def _getTEPCOCSV(url):
        print("  -- getting:", url)
        return pd.read_csv(url, skiprows=2, encoding="cp932",
                           parse_dates=[[0, 1]], dtype=dtypes, thousands=",")

    print("Reading CSVs")

    dataList = map(_getTEPCOCSV, CSV_URLS)

    df = pd.concat(dataList)

    # Translate Column Headers
    print("Renaming Columns")
    df = df.rename(columns=lambda x: _renameHeader(x), errors="raise")

    return df


def get_tohokuden_as_json():
    df = _parseTepcoCsvs()
    return df.to_json(orient='index', date_format="iso")


def convert_tohokuden_df_to_json(df):
    df.reset_index(inplace=True)
    return df.to_json(orient='index', date_format="iso")


def get_tohokuden_as_csv():
    df = _parseTepcoCsvs()
    return df.to_csv(index=False)


def convert_tohokuden_df_to_csv(df):
    return df.to_csv(index=False)


def get_tohokuden_as_dataframe():
    return _parseTepcoCsvs()
