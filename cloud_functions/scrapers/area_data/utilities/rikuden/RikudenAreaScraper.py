
import pandas as pd
from ..UtilityAreaScraper import UtilityAreaScraper


class RikudenAreaScraper(UtilityAreaScraper):

    def _parseCsvs(self):
        CSV_URLS = list(self.get_data_urls_from_page(
            "http://www.rikuden.co.jp/nw_jyukyudata/area_jisseki.html",
            "\/nw_jyukyudata\/attach\/area_jisseki_rikuden\S+\.csv",
            "http://www.rikuden.co.jp"
        ))

        # Page is in a funny order
        CSV_URLS.sort()

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

    def get_dataframe(self):
        return self._parseCsvs()
