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
# Grab the dimension database
ddb = deepy.dimensions.ddb.get_local_ddb()

# Define Regex paterns to extract query-fields
# pattern to extract the boundary fields from a boundary dimension 
boundarydim_regex = re.compile(r"boundary\.([\w.-]*)\.(\w*)")

def getViewDimensionsAndBoundaries(view, context):
    named_dimensions = []
    named_boundaries = []
    view_type = 'simple'
    dimensions = view.get("dimensions")
    if dimensions is None:
        dimensions = context.get("dimensions")
    if dimensions is None:
        dimensions = []
    for dim in dimensions:
        named_dim = deepy.dimensions.util.dim_id_to_name(ddb, dim)
        mo = boundarydim_regex.search(named_dim)
        if mo:
            view_type = 'explicit_boundary'
            named_boundary = 'boundary.' + boundaryMap[int(mo.group(1))] + '.' + mo.group(2)
            named_boundaries.append(named_boundary)
        elif named_dim == 'all_boundary_columns_macro':
            for boundary in boundaryMap.values():
                split_boundary = [ 'boundary.' + boundary + '.input', 'boundary.' + boundary + '.output' ] 
                named_boundaries = named_boundaries + split_boundary
        else:
            named_dimensions.append(deepy.dimensions.util.dim_id_to_name(ddb, dim))

    view_properties = dict()
    view_properties['dimensions'] = sorted(named_dimensions)
    view_properties['boundaries'] = sorted(named_boundaries)
    view_properties['type'] = view_type
    
    return view_properties


def getSqlViews(context_id):
    from deepy.context import sql_context_util
    listOfViews = {}
    context_json = sql_context_util.get_merged_contexts(context=context_id)
    for view in context_json[context_id].get("views", []):
        listOfViews[view.get("uuid", view.get("name"))] = {
            "dimensions": getViewDimensionsAndBoundaries(view, context_json)['dimensions'],
            "boundaries": getViewDimensionsAndBoundaries(view, context_json)['boundaries'],
            "type": getViewDimensionsAndBoundaries(view, context_json)['type'],
            "timesteps": view.get("timesteps"),
            "retention": view.get("retention"),
            "name": view.get("name")
        }
    return listOfViews


def getOldViews(context_id):
    listOfViews = {}
    local_path = deepy.cfg.context_dir + "/%s.json" % context_id
    context_json = deepy.cfg.connector_store.simple_load_json(local_path)
    if not context_json:
        return

    for view in context_json[context_id].get("views", []):
        listOfViews[view.get("uuid")] = {
            "dimensions": getViewDimensionsAndBoundaries(view, context_json)['dimensions'],
            "boundaries": getViewDimensionsAndBoundaries(view, context_json)['boundaries'],
            "type": getViewDimensionsAndBoundaries(view, context_json)['boundaries'],
            "timesteps": view.get("timesteps"),
            "retention": view.get("retention")
        }
    return listOfViews

def getBoundaryMap():
    apiKey = deepy.deepui.get_root_api_keys()[0]
    total_size = None
    url = 'https://localhost/api/boundaries?api_key=' + apiKey
    response = requests.get(url, verify=False)
    boundaryMap = dict()
    for boundary in response.json():
        boundaryMap[boundary['id']] = boundary['name'].lower()
    return boundaryMap

def storeAllContextViewInfo(contextsToEvaluate=['traffic', 'backbone', 'big_cube']):
    allTheThings = {}
    if pipedreamVersion.startswith("5"):
        lookForViewsInMysql = True
    else:
        lookForViewsInMysql = False

    for context in contextsToEvaluate:
        if lookForViewsInMysql:
            allTheThings[context] = getSqlViews(context)
        else:
            allTheThings[context] = getOldViews(context)
    return allTheThings

def main():

    global boundaryMap 
    boundaryMap = getBoundaryMap()

    global allContextViewInfo
    allContextViewInfo = storeAllContextViewInfo(['traffic','backbone','big_cube'])

    with open('context.json', 'w') as f:
        json.dump(allContextViewInfo, f)

if __name__ == "__main__":
    main()
