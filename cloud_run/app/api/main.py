from typing import List, Union, Optional, Any
from pydantic import BaseModel
import copy
from fastapi import APIRouter, HTTPException, Response, status
from datetime import datetime
now = datetime.now()

router = APIRouter(
    prefix="/api/v1/carbon_intensity",
    # tags=["items"],
    # dependencies=[Depends(get_token_header)],
    responses={404: {"description": "Not found"}},
)

from pprint import pprint

from .utilities.tepco.TepcoAPI import TepcoAPI
from .utilities.tohokuden.TohokudenAPI import TohokudenAPI
from .utilities.kepco.KepcoAPI import KepcoAPI
from .utilities.chuden.ChudenAPI import ChudenAPI
from .utilities.hepco.HepcoAPI import HepcoAPI
from .utilities.rikuden.RikudenAPI import RikudenAPI
from .utilities.cepco.CepcoAPI import CepcoAPI
from .utilities.yonden.YondenAPI import YondenAPI
from .utilities.kyuden.KyudenAPI import KyudenAPI
from .utilities.okiden.OkidenAPI import OkidenAPI


def generate_standard_error_model(message, code):
    return {
        "message": message,
        "code": code
    }


# Standard Response Messages for Errors
BAD_UTILITY = generate_standard_error_model('Invalid Utility Specified', 400)
BAD_BREAKDOWN = generate_standard_error_model(
    'Invalid Breakdown Specified', 400)
BAD_YEAR = generate_standard_error_model(
    'Invalid Year Specified - must be between this year and 50 from now', 400)


cache = {
    "tepco": {},
    "tohokuden": {},
    "kepco": {},
    "chuden": {},
    "hepco": {},
    "rikuden": {},
    "cepco": {},
    "yonden": {},
    "kyuden": {},
    "okiden": {},
}


class HourRecord(BaseModel):
    hour: str
    carbon_intensity: float


class DataFormat(BaseModel):
    breakdown: str
    data: List[HourRecord]


class CarbonIntensityAverage(BaseModel):
    fromCache: bool
    carbon_intensity_average: DataFormat


class TimestampRecord(BaseModel):
    timestamp: datetime
    carbon_intensity: float


class HistoricDataFormat(BaseModel):
    historic: List[TimestampRecord]


class HistoricData(BaseModel):
    fromCache: bool
    data: HistoricDataFormat


def clearCache(utility):
    # Mainly Used for the Test Framework
    cache[utility] = {}


def selectUtility(utility):
    utilities = {
        "tepco": TepcoAPI(),
        "tohokuden": TohokudenAPI(),
        "kepco": KepcoAPI(),
        "chuden": ChudenAPI(),
        "hepco": HepcoAPI(),
        "rikuden": RikudenAPI(),
        "cepco": CepcoAPI(),
        "yonden": YondenAPI(),
        "kyuden": KyudenAPI(),
        "okiden": OkidenAPI(),
    }
    return utilities.get(utility, None)


def validateDates(fromDate, toDate):
    BAD_FROM_DATE_FORMAT = {
        "valid": False,
        "response": generate_standard_error_model(
            'Invalid FROM Date Provided', 400)
    }
    BAD_TO_DATE_FORMAT = {
        "valid": False,
        "response": generate_standard_error_model(
            'Invalid TO Date Provided', 400)
    }
    TO_BEFORE_FROM = {
        "valid": False,
        "response": generate_standard_error_model(
            'Invalid Query - TO Date before FROM Date', 400
        )
    }

    # Check fromDate
    try:
        datetimeFrom = datetime.strptime(fromDate, '%Y-%m-%d')
    except Exception as e:
        return BAD_FROM_DATE_FORMAT

    # Check toDate (optional)
    if (toDate != None):
        try:
            datetimeTo = datetime.strptime(toDate, '%Y-%m-%d')
        except Exception as e:
            return BAD_TO_DATE_FORMAT

        # Check Order of Dates
        if(datetimeTo < datetimeFrom):
            return TO_BEFORE_FROM
    return {
        "valid": True
    }


@ router.get('/historic/{utility}/{fromDate}', response_model=HistoricData)
@ router.get('/historic/{utility}/{fromDate}/{toDate}', response_model=HistoricData)
def historical_intensity(utility: str, fromDate: str, toDate: Optional[str] = None):
    response = {}

    # Check Utility
    utilityClass = selectUtility(utility)
    if utilityClass == None:
        raise HTTPException(status_code=400, detail=BAD_UTILITY)

    datesValid = validateDates(fromDate, toDate)
    if(datesValid["valid"] == False):
        raise HTTPException(status_code=400, detail=datesValid["response"])

    if(toDate == None):
        toDate = fromDate

    # Check Cache
    try:
        if toDate in cache[utility]["historical_intensity"][fromDate]:
            print("Returning cache. " + utility +
                  " historical_intensity " + fromDate + "-" + toDate + ":")
            return cache[utility]["historical_intensity"][fromDate][toDate]
    except KeyError:
        print("Not in Cache: " + utility +
              " historical_intensity " + fromDate + "-" + toDate)

    response['data'] = utilityClass.historic_intensity(fromDate, toDate)
    response['fromCache'] = True

    # Populate Cache
    if not "historical_intensity" in cache[utility]:
        cache[utility]["historical_intensity"] = {}
    if not fromDate in cache[utility]["historical_intensity"]:
        cache[utility]["historical_intensity"][fromDate] = {}
    if not toDate in cache[utility]["historical_intensity"][fromDate]:
        cache[utility]["historical_intensity"][fromDate][toDate] = {}

    cache[utility]["historical_intensity"][fromDate][toDate] = copy.deepcopy(
        response)
    response["fromCache"] = False

    return response


@ router.get('/average/{utility}', response_model=CarbonIntensityAverage)
def daily_carbon_intensity(utility):
    response = {}

    utilityClass = selectUtility(utility)

    if utilityClass == None:
        raise HTTPException(status_code=400, detail=BAD_UTILITY)

    # Check Cache
    if "daily_intensity" in cache[utility]:
        print("Returning cache. " + utility + " daily_intensity:")
        return cache[utility]["daily_intensity"]

    response['data'] = utilityClass.daily_intensity()
    response['fromCache'] = True

    # Populate Cache
    cache[utility]["daily_intensity"] = copy.deepcopy(response)
    response["fromCache"] = False

    return response


@ router.get('/average/{breakdown}/{utility}')
def daily_carbon_intensity_with_breakdown(utility, breakdown):
    response = {}

    utilityClass = selectUtility(utility)

    # Sense Check Utiltity
    if utilityClass == None:
        raise HTTPException(status_code=400, detail=BAD_UTILITY)

    # Check Breakdown Type
    breakdowns = {
        "year": utilityClass.daily_intensity_by_year,
        "month": utilityClass.daily_intensity_by_month,
        "month_and_year": utilityClass.daily_intensity_by_month_and_year,
        "month_and_weekday": utilityClass.daily_intensity_by_month_and_weekday
    }
    dataSource = breakdowns.get(breakdown, None)
    if dataSource == None:
        raise HTTPException(status_code=400, detail=BAD_BREAKDOWN)

    # Check Cache
    if breakdown in cache[utility]:
        print("Returning cache. " + utility +
              " daily_intensity_by_" + breakdown + ":")
        return cache[utility][breakdown]

    response['data'] = dataSource()
    response['fromCache'] = True

    # Populate Cache
    cache[utility][breakdown] = copy.deepcopy(response)
    response["fromCache"] = False

    return response


@ router.get('/forecast/average/{year}/{utility}')
def daily_carbon_intensity_prediction(utility, year):
    response = {}

    utilityClass = selectUtility(utility)

    # Sense Check Utiltity
    if utilityClass == None:
        raise HTTPException(status_code=400, detail=BAD_UTILITY)

    # Check Breakdown Type
    if int(year) < now.year or int(year) > (now.year + 50):
        raise HTTPException(status_code=400, detail=BAD_YEAR)

    print("Fetching Prediction - " + utility +
          " predicted intensity for " + str(year) + ":")

    # Check Cache
    if year in cache[utility]:
        print("Returning cache...")
        return cache[utility][year]

    response['data'] = utilityClass.daily_intensity_prediction_for_year_by_month_and_weekday(
        year)
    response['fromCache'] = True

    # Populate Cache
    cache[utility][year] = copy.deepcopy(response)
    response["fromCache"] = False

    return response


@ router.get('/forecast/{utility}/{fromDate}')
@ router.get('/forecast/{utility}/{fromDate}/{toDate}')
def carbon_intensity_timeseries_prediction(utility, fromDate, toDate=None):
    response = {}

    # Check Utility
    utilityClass = selectUtility(utility)
    if utilityClass == None:
        raise HTTPException(status_code=400, detail=BAD_UTILITY)

    datesValid = validateDates(fromDate, toDate)
    if(datesValid["valid"] == False):
        raise HTTPException(status_code=400, detail=datesValid["response"])

    if(toDate == None):
        toDate = fromDate

    print("Fetching Prediction - " + utility + ":")

    # Check Cache
    try:
        if toDate in cache[utility]["prediction"][fromDate]:
            print("Returning cache. " + utility +
                  " prediction " + fromDate + "-" + toDate + ":")
            return cache[utility]["prediction"][fromDate][toDate]
    except KeyError:
        print("Not in Cache: " + utility +
              " prediction " + fromDate + "-" + toDate)

    response['data'] = utilityClass.timeseries_prediction(fromDate, toDate)
    response['fromCache'] = True

    # Populate Cache
    if not "prediction" in cache[utility]:
        cache[utility]["prediction"] = {}
    if not fromDate in cache[utility]["prediction"]:
        cache[utility]["prediction"][fromDate] = {}
    if not toDate in cache[utility]["prediction"][fromDate]:
        cache[utility]["prediction"][fromDate][toDate] = {}

    cache[utility]["prediction"][fromDate][toDate] = copy.deepcopy(
        response)
    response["fromCache"] = False

    return response
