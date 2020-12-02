import csv
import requests
import numpy as np
import pandas as pd
import datetime


class ChudenAreaScraper:

    def _parseCsvs(self):
        CSV_URLS = [
            'https://powergrid.chuden.co.jp/denki_yoho_content_data/2016_areabalance_current_term.csv',
            'https://powergrid.chuden.co.jp/denki_yoho_content_data/2017_areabalance_current_term.csv',
            'https://powergrid.chuden.co.jp/denki_yoho_content_data/2018_areabalance_current_term.csv',
            'https://powergrid.chuden.co.jp/denki_yoho_content_data/2019_areabalance_current_term.csv',
            'https://powergrid.chuden.co.jp/denki_yoho_content_data/2020_areabalance_current_term.csv',
        ]

        dtypes = {
            "エリア需要": int,
            "水力": int,
            "火力": int,
            "原子力": int,
            "太陽光（実績）": int,
            "太陽光（出力制御量）": int,
            "風力（実績）": int,
            "風力（出力制御量）": int,
            "地熱": int,
            "バイオマス": int,
            "揚水": int,
            "連系線": int
        }

        def _renameHeader(header):
            translations = {
                "DATE": "date",
                "TIME": "time",
                "DATE_TIME": "datetime",
                "エリア需要": "MWh_area_demand",
                "水力": "MWh_hydro",
                "火力": "MWh_fossil",
                "原子力": "MWh_nuclear",
                "太陽光（実績）": "MWh_solar_output",
                "太陽光（出力制御量）": "MWh_solar_throttling",
                "風力（実績）": "MWh_wind_output",
                "風力（出力制御量）": "MWh_wind_throttling",
                "地熱": "MWh_geothermal",
                "バイオマス": "MWh_biomass",
                "揚水": "MWh_pumped_storage",
                "連系線": "MWh_interconnectors",
            }

            if header in translations:
                return translations[header]
            return header

            if header in translations:
                return translations[header]
            return header

        def _getCSV(url):
            print("  -- getting:", url)
            return pd.read_csv(url, skiprows=4, encoding="cp932",
                               parse_dates=[[0, 1]], dtype=dtypes)

        print("Reading CSVs")

        dataList = map(_getCSV, CSV_URLS)

        df = pd.concat(dataList)

        # Translate Column Headers
        print("Renaming Columns")
        df = df.rename(columns=lambda x: _renameHeader(x), errors="raise")

        return df

    def get_json(self):
        df = self._parseCsvs()
        return df.to_json(orient='index', date_format="iso")

    def convert_df_to_json(self, df):
        df.reset_index(inplace=True)
        return df.to_json(orient='index', date_format="iso")

    def get_csv(self):
        df = self._parseCsvs()
        return df.to_csv(index=False)

    def convert_df_to_csv(self, df):
        return df.to_csv(index=False)
