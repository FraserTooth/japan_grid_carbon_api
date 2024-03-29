import pandas as pd
from ..UtilityAreaScraper import UtilityAreaScraper


class HepcoAreaScraper(UtilityAreaScraper):

    def _parseCsvs(self):

        CSV_URLS = list(self.get_data_urls_from_page(
            "https://www.hepco.co.jp/network/renewable_energy/fixedprice_purchase/supply_demand_results.html",
            "csv/sup_dem_results_\d{4}_\dq.csv",
            "https://www.hepco.co.jp/network/renewable_energy/fixedprice_purchase/"
        ))

        # Random Excel File in the Mix, has a comment about an earthquake in 2018
        EXCEL_URLS = list(self.get_data_urls_from_page(
            "https://www.hepco.co.jp/network/renewable_energy/fixedprice_purchase/supply_demand_results.html",
            "xls/sup_dem_results_\d{4}_\dq.xls",
            "https://www.hepco.co.jp/network/renewable_energy/fixedprice_purchase/"
        ))

        dtypes = {
            "エリア需要": "int",
            "水力": "int",
            "火力": "int",
            "原子力": "int",
            "太陽光実績": "int",
            "太陽光抑制量": "int",
            "風力実績": "int",
            "風力抑制量": "int",
            "地熱": "int",
            "バイオマス": "int",
            "揚水": "int",
            "連系線": "int",
            "供給力合計": "int",
        }

        converters = {
            "時刻": lambda x: (x.replace('時', ':00'))
        }

        excel_converters = {
            # Don't interpret time as timestamp due to the need to fill forward
            "月日": str,
            "時刻": lambda x: (x.replace('時', ':00'))
        }

        def _renameHeader(header):
            translations = {
                "月日": "date",
                "時刻": "time",
                "月日_時刻": "datetime",
                "エリア需要": "MWh_area_demand",
                "水力": "MWh_hydro",
                "火力": "MWh_fossil",
                "原子力": "MWh_nuclear",
                "太陽光実績": "MWh_solar_output",
                "太陽光抑制量": "MWh_solar_throttling",
                "風力実績": "MWh_wind_output",
                "風力抑制量": "MWh_wind_throttling",
                "地熱": "MWh_geothermal",
                "バイオマス": "MWh_biomass",
                "揚水": "MWh_pumped_storage",
                "連系線": "MWh_interconnectors",
                "供給力合計": "MWh_total_supply"
            }

            if header in translations:
                return translations[header]
            return header

        def _getCSV(url):

            print("  -- getting:", url)
            try:
                data = pd.read_csv(url, skiprows=2, encoding="cp932",
                                   converters=converters,
                                   skip_blank_lines=True)
            except Exception as e:
                print("Caught error \"{error}\" at {url}".format(
                    error=e, url=url))
                return None
            return data

        def _getExcel(url):
            print("  -- getting:", url)
            try:
                data = pd.read_excel(url,
                                     skiprows=2,
                                     encoding='cp932',
                                     converters=excel_converters,
                                     skip_blank_lines=True
                                     )
            except Exception as e:
                print("Caught error \"{error}\" at {url}".format(
                    error=e, url=url))
                return None
            return data

        print("Reading CSVs")

        dataListExcel = map(_getExcel, EXCEL_URLS)
        dfExcel = pd.concat(dataListExcel)

        dataList = map(_getCSV, CSV_URLS)
        dfCSV = pd.concat(dataList)

        df = pd.concat([dfExcel, dfCSV])

        print("Manual Conversions...")

        # Fill Forward Dates
        df["月日"] = df["月日"].fillna(method='ffill')

        # Remove First Blank Line
        df = df.dropna()

        # Create Date Column and Tidy
        df.insert(loc=0, column='datetime',
                  value=pd.to_datetime(df['月日'] + ' ' + df['時刻']))
        df = df.drop(columns=['月日', '時刻'])

        # Set Dtypes
        df = df.apply(
            lambda x: pd.to_numeric(x, errors='coerce', downcast="integer") if x.name != "datetime" else x)

        # Translate Column Headers
        print("Renaming Columns")
        df = df.rename(columns=lambda x: _renameHeader(x), errors="raise")

        df = df.sort_values(by=['datetime']).reset_index(drop=True)

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
