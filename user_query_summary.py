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

def parse_args():
    p = argparse.ArgumentParser(
        description="""
        A utility to inspect view coverage for all logged queries.
        Output stored to querysummary.csv.
        """,
        formatter_class=argparse.RawTextHelpFormatter,
    )
    p.add_argument(
        "--force",
        dest="force",
        default=False,
        action="store_true",
        help="Rescrape the logs for queries.",
    )
    p.add_argument(
        "--support-user",
        dest="support",
        default=False,
        action="store_true",
        help="Do you want to include queries using the suppot user apikey?",
    )
    p.add_argument(
          "--top",
          dest="top",
          default=20,
          type=int,
          action="store",
          help="How many rows per context to display?",
    )
    p.add_argument(
        "--extra-context",
        dest="contexts",
        action="append",
        choices=['subscriber', 'video_stream', 'flowdump'],
        default=['traffic', 'backbone', 'big_cube'],
        help="Extra contexts to include?",
    )
    p.add_argument(
         "--timestamp",
         dest="timestamp",
         default=False,
         action="store_true",
         help="Do you want to query timestamps?",
    )

    args = p.parse_args()

    return args


# Parse args up here once to make help load faster, and I get to
# put args at the top so people can edit the defaults more easily.
parse_args()


# Static junk
logDir = '/pipedream/log/'
uiLogName = 'ui.log'
# log of all queries
queriesFile = './queries_from_logs.txt'
# result-file with query-counts
querySummaryfile = 'querysummary.csv'
# Get the pipedream version
pipedreamVersion = str(deepy.cfg.slice_config.get("build_updates", {}).get("revision"))
# Grab the dimension database
ddb = deepy.dimensions.ddb.get_local_ddb()


# Define Regex paterns to extract query-fields
# type (cube or count) query
type_regex = re.compile(r"/(cube|count)/")
# pattern to ID cube and the dimensions
cube_regex = re.compile(r"cube/(\w*)\.")
# pattern to ID cube and the dimensions
dim_regex = re.compile(r"dimensions=([\w,.-]*)&")
# pattern to ID the timestamp slice
timestamp_regex = re.compile(r"slice=timestamp([\w\-:()]*)&?")
# pattern to ID the dimensions in slice
slice_regex = re.compile(r"slice=([\w.\-]*)&?")
# pattern to ID the APIkey
apikey_regex = re.compile(r"api_key=([\w,.-]*)&?")
# pattern to ID the boundary slice
boundaryslice_regex = re.compile(r"bs=\(([\w,-.()]*)\)&?") 
# pattern to extract each boundary from the boundary slice
boundary_regex = re.compile(r"\((boundary\.[\w.-]*),.*\)")
# pattern to extract the boundary fields from a boundary dimension 
boundarydim_regex = re.compile(r"boundary\.([\w.-]*)\.(\w*)")

def getListOfLogFiles():
    filesToConsume = []
    for thisFile in os.listdir(logDir):
        if uiLogName in thisFile:
            filesToConsume.append(logDir + str(thisFile))
    log.info("Found files:" + str(filesToConsume))
    return filesToConsume


def scrapeLogs(listOfFiles=['/pipedream/log/ui.log.1'], rescrapeLogs=False):
    if os.path.exists(queriesFile) and not rescrapeLogs:
        log.info("Using existing logs. Use --force to rescrape the logs.")
        return
    elif os.path.exists(queriesFile):
        log.info("Removing existing logs.")
        os.remove(queriesFile)
    for thisFile in listOfFiles:
        log.info("Processing: " + thisFile)
        grepString = ' | grep "200 GET" | grep -v datasource | grep -v loginfo | grep -v queue=system | grep -v estimate | grep "/cube/\|count" >>'
        if thisFile.endswith('.gz'):
            cmd = 'sudo zcat ' + thisFile + grepString + queriesFile
        else:
            cmd = 'sudo cat ' + thisFile + grepString + queriesFile
        run(cmd, stderr=subprocess.STDOUT, shell=True)
    log.info("Saving query logs to " + queriesFile)


def getQueryInfoFromLogs():
    queries = []
    count = 0
    with open(queriesFile) as f:
        for line in f:
            mo = type_regex.search(line)
            if mo:
                queryType = mo.group(1)
            mo = cube_regex.search(line)
            if mo:
                cube = mo.group(1)
            mo = dim_regex.search(line)
            if mo:
                dims = mo.group(1)
            else:
                dims = ''
            # Split string in list, and then convert to set to
            # combine with dims in slice.
            dimset = set(dims.split(','))
            # Find all dims in slices, convert to set,and than combine with
            # dimset to get unique list of all dims in query.
            sliceset = set(slice_regex.findall(line))
            alldimset = dimset | sliceset
            # Next line removes the 'timestamp' dim to simplify query-summary.
            alldimset = {x.replace('timestamp', '') for x in alldimset}
            # Next line will remove empty strings in case
            # no dim was found in the query.
            alldimset = list(filter(None, alldimset))
            # Apply similar logic to extract the boundaries
            boundarysliceset = set(boundaryslice_regex.findall(line))
            boundarysliceset = list(filter(None, boundarysliceset))
            boundaries = list()
            if len(boundarysliceset) == 0:
                boundaries = []
            else:
                for boundaryslice in boundarysliceset:
                    mo = boundary_regex.search(boundaryslice)
                    if mo:
                         boundaries.append(mo.group(1).lower())
            mo = timestamp_regex.search(line)
            if mo:
                timestamp = mo.group(1)
            else:
                timestamp = ''
            mo = apikey_regex.search(line)
            if mo:
                apikey = mo.group(1)
            else:
                apikey = ''
            queries.append((queryType, cube, tuple(sorted(alldimset)), tuple(sorted(boundaries)), timestamp, apikey))
            count = count+1
    log.info('Processing ' + str(count) + ' queries.')
    pd.set_option('display.max_colwidth', -1)
    queriesDataFrame = pd.DataFrame(queries, columns =['type', 'context', 'dimensions', 'boundaries', 'timestamp', 'apikey'])
    return queriesDataFrame


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


def view_uuid(row):
    viewCandidate = {"name": "No Match", "uuid": "-99", "precision": 99000}

    for aView in allContextViewInfo[row['context']]:
        viewDimensionsSet = set(map(lambda x:x.lower(), allContextViewInfo[row['context']][aView].get("dimensions", [])))
        queriesDimensionsSet = set(map(lambda x:x.lower(), row['dimensions']))
        viewBoundariesSet = set(map(lambda x:x.lower(), allContextViewInfo[row['context']][aView].get("boundaries", [])))
        queriesBoundariesSet = set(map(lambda x:x.lower(), row['boundaries']))
        viewType = allContextViewInfo[row['context']][aView].get("type")
        if (viewDimensionsSet|viewBoundariesSet).issuperset(queriesDimensionsSet) and viewBoundariesSet.issuperset(queriesBoundariesSet):
            if viewType == 'explicit_boundary':
                if len(queriesBoundariesSet) == 0: continue
            dimensionsDifference = len(viewDimensionsSet.difference(queriesDimensionsSet))
            boundariesDifference = len(viewBoundariesSet.difference(queriesBoundariesSet))
            difference = 1000 * dimensionsDifference + boundariesDifference
            if difference < viewCandidate['precision']:
                viewCandidate['uuid'] = aView
                viewCandidate['name'] = allContextViewInfo[row['context']][aView].get("name", "None")
                viewCandidate['precision'] = difference
    row['uuid'] = viewCandidate['uuid']
    row['name'] = viewCandidate['name']
    return row


def analyizeQueries(queriesDF, args):
    supportKeys = deepy.deepui.get_root_api_keys()
    contextsToEvaluate = args.contexts
    if args.support:
        log.info("Keeping the Support user queries and doing some math.")
        slicedQueries = queriesDF.query("context == @contextsToEvaluate")
    else:
        log.info("Removing the Support user queries and doing some math.")
        slicedQueries = queriesDF.query("context == @contextsToEvaluate and apikey != @supportKeys")

    if args.timestamp:
        countedQueries = slicedQueries.groupby(["context", "dimensions", "boundaries", "timestamp"])["context", "dimensions", "boundaries", "timestamp"]\
            .size().to_frame('count').reset_index()
    else:
        countedQueries = slicedQueries.groupby(["context", "dimensions", "boundaries"])["context", "dimensions", "boundaries"]\
            .size().to_frame('count').reset_index()

    log.info("Mapping view UUID to queries.")
    taggedQueries = countedQueries.apply(view_uuid, axis=1)
    taggedQueries.to_csv(querySummaryfile, index=False)

    for aContext in contextsToEvaluate:
        print("*** Top queried dimensions for " + str(aContext) + ".")
        df = taggedQueries.query("context == [@aContext]")

        if args.timestamp:
            print(df[["context", "timestamp", "uuid", "name", "count"]].sort_values(['count'], ascending=False).head(queryThreshold))
        else:
            print(df[["context", "dimensions", "boundaries", "uuid", "name", "count"]].sort_values(['count'], ascending=False).head(queryThreshold))


def main():
    args = parse_args()

    # Storing as global because it's hard to pass a value into a dataframe apply.
    global queryThreshold
    queryThreshold = args.top

    # Storing all information about views in the global namespace.
    global allContextViewInfo

    scrapeLogs(getListOfLogFiles(), args.force)

    global boundaryMap 
    boundaryMap = getBoundaryMap()

    allContextViewInfo = storeAllContextViewInfo(args.contexts)
    scrapeLogs(getListOfLogFiles(), args.force)

    analyizeQueries(getQueryInfoFromLogs(), args)


if __name__ == "__main__":
    main()
