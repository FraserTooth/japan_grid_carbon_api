import requests
import pandas as pd
import io
import re
from ..UtilityAreaScraper import UtilityAreaScraper

class KepcoAreaScraper(UtilityAreaScraper):
    def _parseCsvs(self):
        # CSV list is loaded into the HTML after page load, so basic HTML scraping wont work...
        # its just one CSV per year tho, so this list isn't so bad.
        CSV_URLS = [
            'https://www.kansai-td.co.jp/denkiyoho/csv/area_jyukyu_jisseki_2016.csv',
            'https://www.kansai-td.co.jp/denkiyoho/csv/area_jyukyu_jisseki_2017.csv',
            'https://www.kansai-td.co.jp/denkiyoho/csv/area_jyukyu_jisseki_2018.csv',
            'https://www.kansai-td.co.jp/denkiyoho/csv/area_jyukyu_jisseki_2019.csv',
            'https://www.kansai-td.co.jp/denkiyoho/csv/area_jyukyu_jisseki_2020.csv',
            'https://www.kansai-td.co.jp/denkiyoho/csv/area_jyukyu_jisseki_2021.csv',
        ]

        # DATE_TIME,
        # エリア需要〔MWh〕,
        # 水力〔MWh〕,
        # 火力〔MWh〕,
        # 原子力〔MWh〕,
        # 太陽光実績〔MWh〕,
        # 太陽光抑制量〔MWh〕,
        # 風力実績〔MWh〕,
        # 風力抑制量〔MWh〕,
        # 地熱〔MWh〕,
        # バイオマス〔MWh〕,
        # 揚水〔MWh〕,
        # 連系線〔MWh〕

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
                "DATE_TIME": "datetime",
                "エリア需要〔MWh〕": "MWh_area_demand",
                "原子力〔MWh〕": "MWh_nuclear",
                "火力〔MWh〕": "MWh_fossil",
                "水力〔MWh〕": "MWh_hydro",
                "地熱〔MWh〕": "MWh_geothermal",
                "バイオマス〔MWh〕": "MWh_biomass",
                "実績〔MWh〕": "MWh_solar_output",
                "抑制量〔MWh〕": "MWh_solar_throttling",
                "実績〔MWh〕.1": "MWh_wind_output",
                "抑制量〔MWh〕.1": "MWh_wind_throttling",
                "揚水〔MWh〕": "MWh_pumped_storage",
                "連系線〔MWh〕": "MWh_interconnectors",
            }

            if header in translations:
                return translations[header]
            return header

        def _getKepcoCSV(url):
            print("  -- getting:", url)
            try:
                response = requests.get(url)

                response_content = response.content.decode(
                    'cp932')

                # Replace the stubborn empty line that pandas wouldn't pick up
                response_content = response_content.replace(',,,,,,,,,,,,\r\n', '')

                # Fix the occasional "10,000" formatted numbers
                pattern = r'"(\d+?),(\d+?)"'
                replacement = r'\1\2'
                response_content = re.sub(pattern, replacement, response_content)

                data = pd.read_csv(io.StringIO(response_content),
                                   skiprows=1,
                                   skip_blank_lines=True,
                                   dtype=dtypes)
            except Exception as e:
                print("Caught error \"{error}\" at {url}".format(
                    error=e, url=url))
                return None
            return data

        print("Reading CSVs")

        dataList = map(_getKepcoCSV, CSV_URLS)

        df = pd.concat(dataList)

        # Translate Column Headers
        print("Renaming Columns")
        df = df.rename(columns=lambda x: _renameHeader(x), errors="raise")

        df['datetime'] = pd.to_datetime(df['datetime'], infer_datetime_format=True).dt.tz_localize(
            'Asia/Tokyo')

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
