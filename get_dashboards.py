import re
import os
import subprocess
import argparse
import deepy.cfg
import deepy.deepui
import deepy.dimensions.util

import pandas as pd
import deepy.log as log

from subprocess import check_output as run

import requests
from requests.packages.urllib3.exceptions import InsecureRequestWarning
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
import json

# Get the pipedream version
pipedreamVersion = str(deepy.cfg.slice_config.get("build_updates", {}).get("revision"))

# Define Regex paterns to extract query-fields
# pattern to extract the boundary fields from a boundary dimension 
boundarydim_regex = re.compile(r"boundary\.([\w.-]*)\.(\w*)")

def getDashboardInfo(): 
    apiKey = deepy.deepui.get_root_api_keys()[0]
    url = 'https://localhost/api/dashboards?api_key=' + apiKey
    response = requests.get(url, verify=False)
    dashboards = response.json()
    all_dashboards = list()
    for dashboard in dashboards:
        url = 'https://localhost/api/dashboards/' + dashboard['slug'] + '?api_key=' + apiKey
        response = requests.get(url, verify=False)
        dashboard_info = response.json()
        all_dashboards.append(dashboard_info)
    return all_dashboards

def main():

    global dashboardInfo
    dashboardInfo = getDashboardInfo()
    with open('dashboards.json', 'w') as f:
        json.dump(dashboardInfo, f)


if __name__ == "__main__":
    main()
