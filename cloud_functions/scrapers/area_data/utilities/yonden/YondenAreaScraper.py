import csv
import requests
import numpy as np
import pandas as pd
import datetime
import io


class YondenAreaScraper:
    def _parseCsvs(self):
        CSV_URLS = [
            'https://www.yonden.co.jp/nw/assets/renewable_energy/data/supply_demand/jukyu2016.xlsx',
            'https://www.yonden.co.jp/nw/assets/renewable_energy/data/supply_demand/jukyu2017.xlsx',
            'https://www.yonden.co.jp/nw/assets/renewable_energy/data/supply_demand/jukyu2018.xlsx',
            'https://www.yonden.co.jp/nw/assets/renewable_energy/data/supply_demand/jukyu2019.xlsx',
            'https://www.yonden.co.jp/nw/assets/renewable_energy/data/supply_demand/jukyu2020.xlsx',
        ]

        # ---- Order of Columns ----
        # DATE
        # TIME
        # エリア需要
        # 原子力
        # 火力
        # 水力
        # 地熱
        # バイオマス
        # 太陽光 実績
        # 太陽光 制御量
        # 風力 実績
        # 風力 制御量
        # 揚水
        # 連系線
        # 合計

        headersList = [
            "date",
            "time",
            "daMWh_area_demand",
            "daMWh_nuclear",
            "daMWh_fossil",
            "daMWh_hydro",
            "daMWh_geothermal",
            "daMWh_biomass",
            "daMWh_solar_output",
            "daMWh_solar_throttling",
            "daMWh_wind_output",
            "daMWh_wind_throttling",
            "daMWh_pumped_storage",
            "daMWh_interconnectors",
            "daMWh_total_supply"
        ]

        dtypes = {
            "date": str,
            "time": str,

        }

        def _getExcel(url):
            print("  -- getting:", url)
            try:
                data = pd.read_excel(url,
                                     skiprows=9,
                                     skipfooter=1,
                                     names=headersList,
                                     header=None,
                                     usecols=range(len(headersList)),
                                     dtype=dtypes
                                     )
            except Exception as e:
                print("Caught error \"{error}\" at {url}".format(
                    error=e, url=url))
                return None
            return data.dropna(thresh=8)

        print("Reading Excel Files")

        dataList = map(_getExcel, CSV_URLS)

        df = pd.concat(dataList)
        df["daMWh_geothermal"].replace({"－": pd.to_numeric(0)}, inplace=True)
        df = df.reset_index(drop=True)

        # # Create Date Column and Tidy
        df.insert(loc=0, column='datetime',
                  value=pd.to_datetime(df['date'] + ' ' + df['time']))
        df = df.drop(columns=['date', 'time'])

        # Set Dtypes
        df = df.apply(
            lambda x: pd.to_numeric(x, errors='coerce', downcast="integer") if x.name != "datetime" else x)

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
