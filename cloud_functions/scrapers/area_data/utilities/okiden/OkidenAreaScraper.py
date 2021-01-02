import csv
import requests
import numpy as np
import pandas as pd
import datetime
import io


class OkidenAreaScraper:
    def _parseCsvs(self):
        CSV_URLS = [
            'https://www.okiden.co.jp/business-support/service/supply-and-demand/jukyu/csv/2016.csv',
            'https://www.okiden.co.jp/business-support/service/supply-and-demand/jukyu/csv/2017.csv',
            'https://www.okiden.co.jp/business-support/service/supply-and-demand/jukyu/csv/2018.csv',
            'https://www.okiden.co.jp/business-support/service/supply-and-demand/jukyu/csv/2019.csv',
            'https://www.okiden.co.jp/business-support/service/supply-and-demand/jukyu/csv/2020.csv',
        ]

        # DATE,
        # TIME,
        # エリアの 需要実績,
        # ----- EMPTY,
        # 火力,
        # 水力,
        # バイオマス,
        # 太陽光,
        # 太陽光 出力制御量,
        # 風力,
        # 風力 出力制御量,
        # 合計

        headersList = [
            "date",
            "time",
            "MWh_area_demand",
            "emptyRow",  # Empty row for visual formatting I guess
            "MWh_fossil",
            "MWh_hydro",
            "MWh_biomass",
            "MWh_solar_output",
            "MWh_solar_throttling",
            "MWh_wind_output",
            "MWh_wind_throttling",
        ]

        dtypes = {
            "エリア需要〔MWh〕": int,
            "水力〔MWh〕": int,
            "火力〔MWh〕": int,
            "原子力〔MWh〕": int,
            "太陽光実績〔MWh〕": int,
            "太陽光抑制量〔MWh〕": int,
            "風力実績〔MWh〕": int,
            "風力抑制量〔MWh〕": int,
            "地熱〔MWh〕": int,
            "バイオマス〔MWh〕": int,
            "揚水〔MWh〕": int,
            "連系線〔MWh〕": int
        }

        def _renameHeader(header):
            translations = {
                0: "date",
                1: "time",
                2: "MWh_area_demand",
                4: "MWh_fossil",
                5: "MWh_hydro",
                6: "MWh_biomass",
                7: "MWh_solar_output",
                8: "MWh_solar_throttling",
                9: "MWh_wind_output",
                10: "MWh_wind_throttling",
                11: "MWh_total_supply",
            }

            if header in translations:
                return translations[header]
            return header

        def _getCSV(url):
            print("  -- getting:", url)
            try:
                data = pd.read_csv(
                    url,
                    skiprows=7,
                    header=None,
                    encoding="cp932",
                    # Skip Empty Row
                    usecols=[0, 1, 2, 4, 5, 6, 7, 8, 9, 10, 11],
                )
            except Exception as e:
                print("Caught error \"{error}\" at {url}".format(
                    error=e, url=url))
                return None
            # Year Column from URL for badly formatted Japanese date column
            year = url[-8:-4]
            data["year"] = year
            return data.dropna(thresh=8)

        def fix_japanese_dates(row):
            if "日" in row['date']:
                row['date'] = row["year"] + "/" + \
                    row['date'].replace("日", "/").replace("月", "/")
            return row

        print("Reading CSVs")

        dataList = map(_getCSV, CSV_URLS)

        df = pd.concat(dataList)

        # Translate Column Headers
        print("Renaming Columns")
        df = df.rename(columns=lambda x: _renameHeader(x), errors="raise")

        # Fix Date Column with Japanese dates
        print("Fixing Japanese Dates")
        df = df.apply(
            fix_japanese_dates,
            axis=1,
        )

        # Create Time Column
        print("Creating Time Column")
        df.insert(loc=0,
                  column='datetime',
                  value=pd.to_datetime(df['date'] + ' ' + df['time']))
        df = df.drop(columns=['date', 'time', 'year'])

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
