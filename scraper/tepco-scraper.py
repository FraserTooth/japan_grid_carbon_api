import csv
import requests
import numpy as np
import pandas as pd

CSV_URL = 'http://www.tepco.co.jp/forecast/html/images/area-2019.csv'


def renameHeaders(headerList):

    headerList[0] = "date"

    headerList[1] = "time"
    headerList[2] = "kWh_demand"

    translations = {
        "原子力": "kWh_nuclear",
        "火力": "kWh_fossil",
        "水力": "kWh_hydro",
        "地熱": "kWh_geothermal",
        "バイオマス": "kWh_biomass",
        "太陽光発電実績": "kWh_solar_record",
        "太陽光出力制御量": "kWh_solar_output",
        "風力発電実績": "kWh_wind_record",
        "風力出力制御量": "kWh_wind_output",
        "揚水": "kWh_wind_output",
        "風力出力制御量": "kWh_pumped_storage",
        "連系線": "kWh_interconnectors",
        "合計": "kWh_total"
    }

    def translate(x):
        if x in translations:
            return translations[x]
        return x

    return list(map(translate, headerList))


with requests.Session() as s:
    download = s.get(CSV_URL)

    decoded_content = download.content.decode('cp932')
    # decoded_content = download.content

    # print(decoded_content[0:100])

    cr = csv.reader(decoded_content.splitlines(), delimiter=',')
    energyData = list(cr)
    headers = renameHeaders(energyData[2])

    print(headers)
    data = energyData[3:]
    df = pd.DataFrame(data, columns=headers)
    print(df)

    # for row in energyData[3:20]:
    #     print(row)
