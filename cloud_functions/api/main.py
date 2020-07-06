import json
import logging
from google.cloud import storage
from google.cloud import bigquery
from google.api_core import retry

import tepco.analysis.tepco_carbon_intensity as tci


def daily_carbon_intensity(request):
    request_json = request.get_json(silent=True)
    request_args = request.args

    if request_json and 'utility' in request_json:
        utility = request_json['utility']
    elif request_args and 'utility' in request_args:
        utility = request_args['utility']
    else:
        return f'No utility specified', 400

    if utility == "tepco":
        return f'Tep My Co'
