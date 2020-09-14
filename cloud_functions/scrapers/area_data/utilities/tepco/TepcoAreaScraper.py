import csv
import requests
import numpy as np
import pandas as pd
import datetime


class TepcoAreaScraper:

    def _parseTepcoCsvs(self):
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

        def _renameHeader(header):
            translations = {
                "Unnamed: 0": "date",
                "Unnamed: 1": "time",
                "Unnamed: 0_Unnamed: 1": "datetime",
                "Unnamed: 2": "daMWh_demand",
                "原子力": "daMWh_nuclear",
                "火力": "daMWh_fossil",
                "水力": "daMWh_hydro",
                "地熱": "daMWh_geothermal",
                "バイオマス": "daMWh_biomass",
                "太陽光発電実績": "daMWh_solar_output",
                "太陽光出力制御量": "daMWh_solar_throttling",
                "風力発電実績": "daMWh_wind_output",
                "風力出力制御量": "daMWh_wind_throttling",
                "揚水": "daMWh_pumped_storage",
                "連系線": "daMWh_interconnectors",
                "合計": "daMWh_total"
            }

            if header in translations:
                return translations[header]
            return header

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

    def get_json(self):
        df = self._parseTepcoCsvs()
        return df.to_json(orient='index', date_format="iso")

    def convert_df_to_json(self, df):
        df.reset_index(inplace=True)
        return df.to_json(orient='index', date_format="iso")

    def get_csv(self):
        df = self._parseTepcoCsvs()
        return df.to_csv(index=False)

    def convert_df_to_csv(self, df):
        return df.to_csv(index=False)

    def get_dataframe(self):
        return self._parseTepcoCsvs()
