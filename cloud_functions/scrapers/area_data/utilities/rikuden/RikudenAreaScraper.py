import csv
import requests
import numpy as np
import pandas as pd
import datetime


class RikudenAreaScraper:

    def _parseCsvs(self):
        # CSVs change format halfway thru
        # http://www.rikuden.co.jp/nw_jyukyudata/attach/area_jisseki_rikuden201604_06.csv

        CSV_URLS = [
            # All the 3 month CSVs
            "http://www.rikuden.co.jp/nw_jyukyudata/attach/area_jisseki_rikuden201604_06.csv",
            "http://www.rikuden.co.jp/nw_jyukyudata/attach/area_jisseki_rikuden201607_09.csv",
            "http://www.rikuden.co.jp/nw_jyukyudata/attach/area_jisseki_rikuden201610_12.csv",
            "http://www.rikuden.co.jp/nw_jyukyudata/attach/area_jisseki_rikuden201701_03.csv",
            "http://www.rikuden.co.jp/nw_jyukyudata/attach/area_jisseki_rikuden201704_06.csv",
            "http://www.rikuden.co.jp/nw_jyukyudata/attach/area_jisseki_rikuden201707_09.csv",
            "http://www.rikuden.co.jp/nw_jyukyudata/attach/area_jisseki_rikuden201710_12.csv",
            "http://www.rikuden.co.jp/nw_jyukyudata/attach/area_jisseki_rikuden201801_03.csv",
            "http://www.rikuden.co.jp/nw_jyukyudata/attach/area_jisseki_rikuden201804_06.csv",
            "http://www.rikuden.co.jp/nw_jyukyudata/attach/area_jisseki_rikuden201807_09.csv",
            # The first 3 singles, to get the months back to 1
            "http://www.rikuden.co.jp/nw_jyukyudata/attach/area_jisseki_rikuden201810.csv",
            "http://www.rikuden.co.jp/nw_jyukyudata/attach/area_jisseki_rikuden201811.csv",
            "http://www.rikuden.co.jp/nw_jyukyudata/attach/area_jisseki_rikuden201812.csv",
        ]

        for year in range(2019, 2021):
            for month in range(1, 13):

                if year == 2019 and month == 10:
                    # Incorrectly Formatted Month
                    CSV_URLS.append(
                        "http://www.rikuden.co.jp/nw_jyukyudata/attach/area_jisseki_rikuden201910_01.csv")
                    continue

                url = "http://www.rikuden.co.jp/nw_jyukyudata/attach/area_jisseki_rikuden{year}{month}.csv".format(
                    year=year,
                    month="{:02d}".format(month)
                )
                CSV_URLS.append(url)

        headersList = [
            "date",
            "time",
            "MWh_area_demand",
            "MWh_nuclear",
            "MWh_fossil",
            "MWh_hydro",
            "MWh_geothermal",
            "MWh_biomass",
            "MWh_solar_output",
            "MWh_solar_throttling",
            "MWh_wind_output",
            "MWh_wind_throttling",
            "MWh_pumped_storage",
            "MWh_interconnectors",
        ]

        def _getCSV(url):
            print("  -- getting:", url)

            # Badly Formatted File
            if url == "http://www.rikuden.co.jp/nw_jyukyudata/attach/area_jisseki_rikuden201610_12.csv":
                skipRows = 7
            else:
                skipRows = 6

            try:
                data = pd.read_csv(
                    url,
                    skiprows=skipRows,
                    encoding="cp932",
                    names=headersList,
                    header=None,
                    usecols=range(len(headersList))
                )
            except Exception as e:
                print("Caught error \"{error}\" at {url}".format(
                    error=e, url=url))
                return None
            return data.dropna(thresh=8)

        print("Reading CSVs")

        dataList = map(_getCSV, CSV_URLS)

        df = pd.concat(dataList)
        df = df.fillna(0)
        df = df.reset_index(drop=True)

        # # Create Date Column and Tidy
        df.insert(loc=0, column='datetime',
                  value=pd.to_datetime(df['date'] + ' ' + df['time']))
        df = df.drop(columns=['date', 'time'])

        # Set Dtypes
        df = df.apply(
            lambda x: pd.to_numeric(x, errors='coerce', downcast="integer") if x.name != "datetime" else x)

        # Translate Column Headers

        def _renameHeader(header):
            translations = {
                "date_time": "datetime",
            }

            if header in translations:
                return translations[header]
            return header

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
