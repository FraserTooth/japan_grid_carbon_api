import json
import copy
import werkzeug.datastructures
from flask import Flask
app = Flask(__name__)

from pprint import pprint

from utilities.tepco.TepcoAPI import TepcoAPI
from utilities.tohokuden.TohokudenAPI import TohokudenAPI

# Standard Response Messages for Errors
BAD_UTILITY = 'Invalid Utility Specified'

# Add CORS to All Requests
headers = {
    'Access-Control-Allow-Origin': '*'
}


cache = {
    "tepco": {},
    "tohokuden": {}
}


def selectUtility(utility):
    utilities = {
        "tepco": TepcoAPI(),
        "tohokuden": TohokudenAPI()
    }
    return utilities.get(utility, None)


def main(request):
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


@app.route('/daily_carbon_intensity/<utility>')
def daily_carbon_intensity(utility):
    response = {}

    utilityClass = selectUtility(utility)

    if utilityClass == None:
        return BAD_UTILITY, 400, headers

    # Check Cache
    if "daily_intensity" in cache[utility]:
        print("Returning cache." + utility + "daily_intensity:")
        return json.dumps(cache[utility]["daily_intensity"]), 200, headers

    response['data'] = utilityClass.daily_intensity()
    response['fromCache'] = True

    # Populate Cache
    cache[utility]["daily_intensity"] = copy.deepcopy(response)
    response["fromCache"] = False

    return json.dumps(response), 200, headers


@app.route('/daily_carbon_intensity/<utility>/<breakdown>')
def daily_carbon_intensity_with_breakdown(utility, breakdown):
    response = {}

    utilityClass = selectUtility(utility)

    # Sense Check Utiltity
    if utilityClass == None:
        return BAD_UTILITY, 400, headers

    # Check Breakdown Type
    breakdowns = {
        "month": utilityClass.daily_intensity_by_month,
        "month_and_weekday": utilityClass.daily_intensity_by_month_and_weekday
    }
    dataSource = breakdowns.get(breakdown, None)
    if dataSource == None:
        return f'Invalid Breakdown Specified', 400, headers

    # Check Cache
    if breakdown in cache[utility]:
        print("Returning cache." + utility +
              "daily_intensity_by_" + breakdown + ":")
        return json.dumps(cache[utility][breakdown]), 200, headers

    response['data'] = dataSource()
    response['fromCache'] = True

    # Populate Cache
    cache[utility][breakdown] = copy.deepcopy(response)
    response["fromCache"] = False

    return json.dumps(response), 200, headers
