import csv
import requests
import numpy as np
import pandas as pd
import datetime
import io


class CepcoAreaScraper:
    def _parseCsvs(self):
        CSV_URLS = [
            'https://www.energia.co.jp/nw/service/retailer/eriajyukyu/sys/eria_jyukyu.csv'
        ]

        # DATE
        # TIME
        # 需要
        # 原子力
        # 火力
        # 水力
        # 地熱
        # バイオマス
        # 太陽光(実績)
        # 太陽光(抑制量)
        # 風力(実績)
        # 風力(抑制量)
        # 揚水
        # 連系線潮流

        dtypes = {
            "需要": int,
            "原子力": int,
            "火力": int,
            "水力": int,
            "地熱": int,
            "バイオマス": int,
            "太陽光(実績)": int,
            # "太陽光(抑制量)": int, Just a hypen
            "風力(実績)": int,
            # "風力(抑制量)": int, Just a hypen
            "揚水": int,
            "連系線潮流": int
        }

        def _renameHeader(header):
            translations = {
                "DATE_TIME": "datetime",
                "需要": "MWh_area_demand",
                "原子力": "MWh_nuclear",
                "火力": "MWh_fossil",
                "水力": "MWh_hydro",
                "地熱": "MWh_geothermal",
                "バイオマス": "MWh_biomass",
                "太陽光(実績)": "MWh_solar_output",
                "太陽光(抑制量)": "MWh_solar_throttling",
                "風力(実績)": "MWh_wind_output",
                "風力(抑制量)": "MWh_wind_throttling",
                "揚水": "MWh_pumped_storage",
                "連系線潮流": "MWh_interconnectors",
            }

            if header in translations:
                return translations[header]
            return header

        def _getKepcoCSV(url):
            print("  -- getting:", url)
            try:
                data = pd.read_csv(url,
                                   skiprows=2,
                                   encoding="cp932",
                                   parse_dates=[[0, 1]],
                                   dtype=dtypes
                                   )
            except Exception as e:
                print("Caught error \"{error}\" at {url}".format(
                    error=e, url=url))
                return None
            return data

        print("Reading CSVs")

        dataList = map(_getKepcoCSV, CSV_URLS)

        df = pd.concat(dataList)

        # Fix the throttled columns
        df["太陽光(抑制量)"].replace({"－": pd.to_numeric(0)}, inplace=True)
        df["風力(抑制量)"].replace({"－": pd.to_numeric(0)}, inplace=True)

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

    def get_dataframe(self):
        return self._parseCsvs()
