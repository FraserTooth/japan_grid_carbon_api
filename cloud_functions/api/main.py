import json
import copy
import werkzeug.datastructures
from flask import Flask
import datetime
import re
now = datetime.datetime.now()
app = Flask(__name__)

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

# Standard Response Messages for Errors
BAD_UTILITY = 'Invalid Utility Specified'
BAD_DATE = 'Invalid Date Provided'

# Add CORS to All Requests
headers = {
    'Access-Control-Allow-Origin': '*',
    'mimetype': 'application/json'
}


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


def isValidDate(dateString):
    # YYYY-MM-DD, potentially valid date
    regex = r"^20[12]\d-[01]\d-[0-3]\d$"
    isValidPattern = bool(re.match(regex, dateString))
    return isValidPattern


def clearCache(utility):
    cache[utility] = {}


def api(request):
    with app.app_context():
        headers = werkzeug.datastructures.Headers()
        for key, value in request.headers.items():
            headers.add(key, value)
        with app.test_request_context(method=request.method, base_url=request.base_url, path=request.path, query_string=request.query_string, headers=headers, data=request.data):
            try:
                rv = app.preprocess_request()
                if rv is None:
                    rv = app.dispatch_request()
            except Exception as e:
                rv = app.handle_user_exception(e)
            response = app.make_response(rv)
            return app.process_response(response)


@app.route('/v1/carbon_intensity/historic/<utility>/<fromDate>', defaults={'toDate': None})
@app.route('/v1/carbon_intensity/historic/<utility>/<fromDate>/<toDate>')
def historical_intensity(utility, fromDate, toDate=None):
    response = {}

    # Check Utility
    utilityClass = selectUtility(utility)
    if utilityClass == None:
        return BAD_UTILITY, 400, headers

    # Check fromDate
    if not isValidDate(fromDate):
        return BAD_DATE, 400, headers

    # Check toDate (optional)
    if (toDate != None):
        if not isValidDate(toDate):
            return BAD_DATE, 400, headers
    else:
        # Set to fromDate if needed
        toDate = fromDate

    # Check Cache
    try:
        if toDate in cache[utility]["historical_intensity"][fromDate]:
            print("Returning cache. " + utility +
                  " historical_intensity " + fromDate + "-" + toDate + ":")
            return json.dumps(cache[utility]["historical_intensity"][fromDate][toDate]), 200, headers
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

    return json.dumps(response), 200, headers


@app.route('/v1/carbon_intensity/average/<utility>')
def daily_carbon_intensity(utility):
    response = {}

    utilityClass = selectUtility(utility)

    if utilityClass == None:
        return BAD_UTILITY, 400, headers

    # Check Cache
    if "daily_intensity" in cache[utility]:
        print("Returning cache. " + utility + " daily_intensity:")
        return json.dumps(cache[utility]["daily_intensity"]), 200, headers

    response['data'] = utilityClass.daily_intensity()
    response['fromCache'] = True

    # Populate Cache
    cache[utility]["daily_intensity"] = copy.deepcopy(response)
    response["fromCache"] = False

    return json.dumps(response), 200, headers


@app.route('/v1/carbon_intensity/average/<breakdown>/<utility>')
def daily_carbon_intensity_with_breakdown(utility, breakdown):
    response = {}

    utilityClass = selectUtility(utility)

    # Sense Check Utiltity
    if utilityClass == None:
        return BAD_UTILITY, 400, headers

    # Check Breakdown Type
    breakdowns = {
        "year": utilityClass.daily_intensity_by_year,
        "month": utilityClass.daily_intensity_by_month,
        "month_and_year": utilityClass.daily_intensity_by_month_and_year,
        "month_and_weekday": utilityClass.daily_intensity_by_month_and_weekday
    }
    dataSource = breakdowns.get(breakdown, None)
    if dataSource == None:
        return f'Invalid Breakdown Specified', 400, headers

    # Check Cache
    if breakdown in cache[utility]:
        print("Returning cache. " + utility +
              " daily_intensity_by_" + breakdown + ":")
        return json.dumps(cache[utility][breakdown]), 200, headers

    response['data'] = dataSource()
    response['fromCache'] = True

    # Populate Cache
    cache[utility][breakdown] = copy.deepcopy(response)
    response["fromCache"] = False

    return json.dumps(response), 200, headers


@app.route('/v1/carbon_intensity/forecast/average/<year>/<utility>')
def daily_carbon_intensity_prediction(utility, year):
    response = {}

    utilityClass = selectUtility(utility)

    # Sense Check Utiltity
    if utilityClass == None:
        return BAD_UTILITY, 400, headers

    # Check Breakdown Type
    if int(year) < now.year or int(year) > (now.year + 50):
        return f'Invalid Year Specified - must be between this year and 50 from now', 400, headers

    print("Fetching Prediction - " + utility +
          " predicted intensity for " + str(year) + ":")

    # Check Cache
    if year in cache[utility]:
        print("Returning cache...")
        return json.dumps(cache[utility][year]), 200, headers

    response['data'] = utilityClass.daily_intensity_prediction_for_year_by_month_and_weekday(
        year)
    response['fromCache'] = True

    # Populate Cache
    cache[utility][year] = copy.deepcopy(response)
    response["fromCache"] = False

    return json.dumps(response), 200, headers


@app.route('/v1/carbon_intensity/forecast/<utility>')
def carbon_intensity_timeseries_prediction(utility):
    response = {}

    utilityClass = selectUtility(utility)

    # Sense Check Utiltity
    if utilityClass == None:
        return BAD_UTILITY, 400, headers

    print("Fetching Prediction - " + utility + ":")

    # Check Cache
    if 'prediction' in cache[utility]:
        print("Returning cache...")
        return json.dumps(cache[utility]['prediction']), 200, headers

    response['data'] = utilityClass.timeseries_prediction()
    response['fromCache'] = True

    # Populate Cache
    cache[utility]['prediction'] = copy.deepcopy(response)
    response["fromCache"] = False

    return json.dumps(response), 200, headers
