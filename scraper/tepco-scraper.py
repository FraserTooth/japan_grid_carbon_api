import csv
import requests

CSV_URL = 'http://www.tepco.co.jp/forecast/html/images/area-2019.csv'

with requests.Session() as s:
    download = s.get(CSV_URL)

    decoded_content = download.content.decode('cp932')
    # decoded_content = download.content

    # print(decoded_content[0:100])

    cr = csv.reader(decoded_content.splitlines(), delimiter=',')
    energyData = list(cr)
    headers = energyData[2]
    print(headers)
    for row in energyData[3:20]:
        print(row)
