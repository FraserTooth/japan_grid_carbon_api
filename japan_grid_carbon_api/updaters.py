from django.db import models
import japan_grid_carbon_api.models
import dateutil.parser as parser
import pandas as pd

import scraper.tepco_scraper as tepcoScraper


def updateTepcoData():
    # Update the TEPCO Energy Data
    tepco = japan_grid_carbon_api.models.TEPCOEnergyData
    latestData = tepco.objects.latest('datetime')
    latestDataDate = pd.to_datetime(
        latestData.datetime.isoformat()).tz_convert(None)

    tepcoDataframe = tepcoScraper.getTEPCODataframewithCarbonIntensity()

    newData = tepcoDataframe[tepcoDataframe.datetime > latestDataDate]
    print(newData)

# import japan_grid_carbon_api.updaters as updater
# updater.updateTepcoData()
