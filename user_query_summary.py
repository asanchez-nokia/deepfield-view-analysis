import re
import os
import subprocess
import argparse
import deepy.cfg
import deepy.deepui
import get_context

import pandas as pd
import numpy as np
import deepy.log as log

from subprocess import check_output as run
from datetime import datetime
import requests
from requests.packages.urllib3.exceptions import InsecureRequestWarning
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

args = None

def parse_args():
    p = argparse.ArgumentParser(
        description="""
        A utility to inspect view coverage for all logged queries.
        Output stored to querysummary.csv.
        """,
        formatter_class=argparse.RawTextHelpFormatter,
    )
    p.add_argument(
        "--rescrape-logs",
        dest="force",
        default=False,
        action="store_true",
        help="Rescrape the logs for queries.",
    )
    p.add_argument(
        "--no-cache",
        dest="nocache",
        default=False,
        action="store_true",
        help="Reevaluate all definitions for contexts, views, and dimensions.",
    )
    p.add_argument(
        "--support-user",
        dest="support",
        default=False,
        action="store_true",
        help="Do you want to include queries using the support user?",
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

# Define Regex paterns to extract query-fields
# time of log message
time_regex = re.compile(r"^(\S+\s+\S+\s+\S+\s+\S+)")
# pattern to ID cube
cube_regex = re.compile(r"cube/(\w*)\.")
explore_regex = re.compile(r"explore\-beta/timegraph/(\w*)\?")
# pattern to ID dimensions
dim_regex = re.compile(r"(dimensions|d)=([\w,.-]*)&")
# pattern to ID the timestamp slice
timerange_regex = re.compile(r"(slice|s)=timestamp([\w\-:()]*)&?")
timestamp_relative_regex = re.compile(r"^\-(\d+)([a-z]+)$")
timestamp_absolute_regex = re.compile(r"^(\d+)\-(\d+)\-(\d+)T(.*)$")
# pattern to ID the timestep
timestep_regex = re.compile(r"(apply|a)=timestep\((auto|[a-zA-Z0-9-]+)[,)]")
# pattern to ID the seconds to execute
seconds_regex = re.compile(r"took (\d+) seconds to load")
# pattern to ID the user running the query
user_regex = re.compile(r"for User ([a-f0-9]+) Status")
# pattern to ID the dimensions in slice
slice_regex = re.compile(r"(slice|&s|\?s)=([\w.\-]*)&?")
# pattern to ID the APIkey
apikey_regex = re.compile(r"api_key=([\w,.-]*)&?")
# pattern to ID the boundary slice
boundaryslice_regex = re.compile(r"bs=\(([\w,-.()]*)\)&?")
# pattern to extract each boundary from the boundary slice
boundary_regex = re.compile(r"\((boundary\.[\w.-]*),.*\)")

def getListOfLogFiles():
    filesToConsume = []
    for thisFile in os.listdir(logDir):
        if uiLogName in thisFile:
            filesToConsume.append(logDir + str(thisFile))
    log.info("Found files:" + str(filesToConsume))
    return filesToConsume


def scrapeLogs(listOfFiles=['/pipedream/log/ui.log.1'], rescrapeLogs=False):
    if os.path.exists(queriesFile) and not rescrapeLogs:
        log.info("Using existing logs. Use --rescrape-logs to rescrape the logs.")
        return
    elif os.path.exists(queriesFile):
        log.info("Removing existing logs.")
        os.remove(queriesFile)
    for thisFile in listOfFiles:
        log.info("Processing: " + thisFile)
        grepString = ' | grep "Status: 200" | grep -v datasource | grep -v loginfo | grep -v queue=system | grep -v estimate | grep "seconds to load" | grep json | egrep "/cube/|/explore-beta/|count" >>'
        if thisFile.endswith('.gz'):
            cmd = 'sudo zcat ' + thisFile + grepString + queriesFile
        else:
            cmd = 'sudo cat ' + thisFile + grepString + queriesFile
        run(cmd, stderr=subprocess.STDOUT, shell=True)
    log.info("Saving query logs to " + queriesFile)


def getQueryInfoFromLogs():
    global args
    queries = []
    count = 0
    api_key = deepy.deepui.get_root_api_keys()[0]
    users = dict()
    users_data = requests.get('https://localhost/api/users?api_key=' + api_key, verify = False).json()
    for user in users_data['users'].keys():
        users[user] = users_data['users'][user]['email']
    with open(queriesFile) as f:
        for line in f:
            mo = time_regex.search(line)
            if mo:
                queryTime = mo.group(1)
                queryTimestamp = int(datetime.strptime(queryTime,'%b %d %H:%M:%S %Y').strftime('%s'))
            mo = cube_regex.search(line)
            if mo:
                cube = mo.group(1)
            mo = explore_regex.search(line)
            if mo:
                cube = mo.group(1)
            if cube not in args.contexts:
                continue
            mo = dim_regex.search(line)
            if mo:
                dims = mo.group(2)
            else:
                dims = ''
            # Split string in list, and then convert to set to
            # combine with dims in slice.
            dimset = set(dims.split(','))
            dimset.discard('none')
            # Find all dims in slices, convert to set,and than combine with
            # dimset to get unique list of all dims in query.
            slices = slice_regex.findall(line)
            slicelist = list()
            for slice in slices:
                slicelist.append(slice[1])
            sliceset = set(slicelist)
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
            mo = timerange_regex.search(line)
            if mo:
                timerange = mo.group(2)
            else:
                timerange = '(-24h:now)'
            try:
                timestampRange = timerange.split('(')[1].split(')')[0].split(':')
                startTime = timestampRange[0]
                endTime = timestampRange[1]
                startTimestamp = parseTimestamp(queryTimestamp,startTime)
                endTimestamp = parseTimestamp(queryTimestamp,endTime)
                if startTimestamp == -1 or endTimestamp == -1:
                    continue
                timeDelta = endTimestamp - startTimestamp
                if timeDelta < 0:
                    continue
                if timeDelta <= 7200:
                    autoTimestep = '10s'
                elif timeDelta <= 172800:
                    autoTimestep = '5min'
                elif timeDelta <= 604800:
                    autoTimestep = '30min'
                elif timeDelta <= 2678400:
                    autoTimestep = '2hour'
                else:
                    autoTimestep = 'day'

            except IndexError:
                continue
            mo = timestep_regex.search(line)
            explicitTimestep = ''
            if mo:
                explicitTimestep = mo.group(2)
                if explicitTimestep == 'auto':
                    explicitTimestep = ''
                    pass
                elif explicitTimestep in ['10', '10s', '10sec', '10second', '10seconds']:
                    explicitTimestep = '10s'
                elif explicitTimestep in ['300', '5m', '5min', '5minute', '5minutes']:
                    explicitTimestep = '5min'
                elif explicitTimestep in ['1800', '30m', '30min', '30minute', '30minutes', 'hour', '1hour', '1hours']:
                    explicitTimestep = '30min'
                elif explicitTimestep in ['7200', '2h', '2hour', '2hours']:
                    explictTimestep = '2hour'
                elif explicitTimestep in ['86400', 'day', '1day', '1days']:
                    explicitTimestep = 'day'
            mo = seconds_regex.search(line)
            if mo:
                seconds = int(mo.group(1))
            else:
                seconds = ''
            mo = user_regex.search(line)
            if mo:
                user = users[mo.group(1)]
            else:
                user = ''

            queries.append((cube, tuple(sorted(alldimset)), tuple(sorted(boundaries)), autoTimestep, explicitTimestep, seconds, user))
            count = count+1
    log.info('Processing ' + str(count) + ' queries.')
    pd.set_option('display.max_colwidth', -1)
    queriesDataFrame = pd.DataFrame(queries, columns =['context', 'dimensions', 'boundaries', 'autoTimestep', 'explicitTimestep', 'seconds', 'user'])
    return queriesDataFrame


def analyzeQueries(queriesDF, args):
    contextsToEvaluate = args.contexts
    if args.support:
        log.info("Keeping the Support user queries and doing some math.")
        slicedQueries = queriesDF.query("context == @contextsToEvaluate")
    else:
        log.info("Removing the Support user queries and doing some math.")
        slicedQueries = queriesDF.query("context == @contextsToEvaluate and user != 'support@deepfield.net'")

    countedQueries = slicedQueries.groupby(["context", "dimensions", "boundaries", "autoTimestep", "explicitTimestep"])\
            .agg({'user': lambda x: ', '.join(set(x)),'seconds':['mean','count']}).reset_index()

    log.info("Mapping view UUID to queries.")
    taggedQueries = countedQueries.apply(allContextInfo.view_uuid, axis=1)
    sortedQueries = taggedQueries.sort_values(by=('seconds','count'), ascending=False).head(queryThreshold).to_csv(sep=';', index=False)

    print("*** Top queries:")

    print(sortedQueries)

def parseTimestamp (queryTimestamp,targetTimestamp):
    if targetTimestamp == 'now':
        return queryTimestamp
    matched = False
    mo = re.search(timestamp_relative_regex,targetTimestamp)
    if mo:
        matched = True
        unit = mo.group(2)
        if unit in ['s', 'sec', 'secs', 'second', 'seconds']:
            timestamp = queryTimestamp - int(mo.group(1))
        elif unit in ['m', 'min', 'mins', 'minute', 'minutes']:
            timestamp = queryTimestamp - int(mo.group(1)) * 60
        elif unit in ['h', 'hour', 'hours']:
            timestamp = queryTimestamp - int(mo.group(1)) * 3600
        elif unit in ['d', 'day', 'days']:
            timestamp = queryTimestamp - int(mo.group(1)) * 86400
        elif unit in ['w', 'week', 'weeks']:
            timestamp = queryTimestamp - int(mo.group(1)) * 604800
        elif unit in ['month', 'months']:
            timestamp = queryTimestamp - int(mo.group(1)) * 2592000
        elif unit in ['y', 'year', 'years']:
            timestamp = queryTimestamp - int(mo.group(1)) * 31536000
        else:
            return -1
    mo = re.search(timestamp_absolute_regex,targetTimestamp)
    if mo:
        matched = True
        year = int(mo.group(1))
        month = int(mo.group(2))
        day = int(mo.group(3))
        time = mo.group(4)
        try:
            if time is None:
                timestamp = datetime(year,month,day,0,0,0).strftime('%s')
            elif re.match(r"^(\d+)$", time):
                hour = int(time)
                timestamp = datetime(year,month,day,hour,0,0).strftime('%s')
            elif re.match(r"^(\d+)\-(\d+)$", time):
                hour = int(time.split('-')[0])
                minute = int(time.split('-')[1])
                timestamp = datetime(year,month,day,hour,minute,0).strftime('%s')
            elif re.match(r"^(\d+)\-(\d+)\-(\d+)$", time):
                hour = int(time.split('-')[0])
                minute = int(time.split('-')[1])
                sec = int(time.split('-')[2])
                timestamp = datetime(year,month,day,hour,minute,sec).strftime('%s')
            else:
                return -1
        except ValueError:
            return -1
    if not matched:
        return -1
    return int(timestamp)

def main():
    global args
    args = parse_args()

    scrapeLogs(getListOfLogFiles(), args.force)

    # Storing as global because it's hard to pass a value into a dataframe apply.
    global queryThreshold
    queryThreshold = args.top
    pd.set_option('display.max_rows', None)

    reEvaluate = True if args.nocache else False

    # Storing all information about views in the global namespace.
    global allContextInfo
    global boundaryMap
    global allContextViewInfo

    allContextInfo = get_context.Context(contextList=args.contexts, reEvaluate=args.nocache, callingProgram='user_query_summary')
    boundaryMap = allContextInfo.boundaryMap
    allContextViewInfo = allContextInfo.allContextViewInfo

    analyzeQueries(getQueryInfoFromLogs(), args)


if __name__ == "__main__":
    main()
