import json


import tepco.api as tepco


def dailyCarbon(request):
    # Set CORS headers for the preflight request
    if request.method == 'OPTIONS':
        # Allows GET requests from any origin with the Content-Type
        # header and caches preflight response for an 3600s
        headers = {
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'GET',
            'Access-Control-Allow-Headers': 'Content-Type',
            'Access-Control-Max-Age': '3600'
        }

        return ('', 204, headers)

    # Set CORS headers for the main request
    headers = {
        'Access-Control-Allow-Origin': '*'
    }

    request_json = request.get_json(silent=True)
    request_args = request.args

    response = {}

    if request_json and 'utility' in request_json:
        utility = request_json['utility']
    elif request_args and 'utility' in request_args:
        utility = request_args['utility']
    else:
        return f'No utility specified', 400, headers

    if utility == "tepco":
        response['data'] = tepco.daily_intensity()
        return json.dumps(response), 200, headers


def daily_carbon_intensity_by_month(request):
    # Set CORS headers for the preflight request
    if request.method == 'OPTIONS':
        # Allows GET requests from any origin with the Content-Type
        # header and caches preflight response for an 3600s
        headers = {
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'GET',
            'Access-Control-Allow-Headers': 'Content-Type',
            'Access-Control-Max-Age': '3600'
        }

        return ('', 204, headers)

    # Set CORS headers for the main request
    headers = {
        'Access-Control-Allow-Origin': '*'
    }

    request_json = request.get_json(silent=True)
    request_args = request.args

    response = {}

    if request_json and 'utility' in request_json:
        utility = request_json['utility']
    elif request_args and 'utility' in request_args:
        utility = request_args['utility']
    else:
        return f'No utility specified', 400, headers

    if utility == "tepco":
        response['data'] = tepco.daily_intensity_by_month()
        return response, 200, headers


def daily_carbon_intensity_by_month_and_weekday(request):
    # Set CORS headers for the preflight request
    if request.method == 'OPTIONS':
        # Allows GET requests from any origin with the Content-Type
        # header and caches preflight response for an 3600s
        headers = {
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'GET',
            'Access-Control-Allow-Headers': 'Content-Type',
            'Access-Control-Max-Age': '3600'
        }

        return ('', 204, headers)

    # Set CORS headers for the main request
    headers = {
        'Access-Control-Allow-Origin': '*'
    }

    request_json = request.get_json(silent=True)
    request_args = request.args

    response = {}

    if request_json and 'utility' in request_json:
        utility = request_json['utility']
    elif request_args and 'utility' in request_args:
        utility = request_args['utility']
    else:
        return f'No utility specified', 400, headers

    if utility == "tepco":
        response['data'] = tepco.daily_intensity_by_month_and_weekday()
        return response, 200, headers
