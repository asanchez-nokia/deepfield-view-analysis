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

# Define Regex paterns to extract query-fields
# pattern to extract the boundary fields from a boundary dimension 
boundarydim_regex = re.compile(r"boundary\.([\w.-]*)\.(\w*)")

def view_uuid(query, dashboards):
    viewCandidate = {"name": "No Match", "uuid": "-99", "precision": 99000, "dimensions_and_boundaries": ''}
    for aView in allContextViewInfo[query['context']]:
        viewDimensionsSet = set(allContextViewInfo[query['context']][aView].get("dimensions", []))
        queriesDimensionsSet = set(query['dimensions'])
        viewBoundariesSet = set(allContextViewInfo[query['context']][aView].get("boundaries", []))
        queriesBoundariesSet = set(query['boundaries'])
        viewType = allContextViewInfo[query['context']][aView].get("type")
        if (viewDimensionsSet|viewBoundariesSet).issuperset(queriesDimensionsSet) and viewBoundariesSet.issuperset(queriesBoundariesSet):
            if viewType == 'explicit_boundary':
                if len(queriesBoundariesSet) == 0: continue
            dimensionsDifference = len(viewDimensionsSet.difference(queriesDimensionsSet))
            boundariesDifference = len(viewBoundariesSet.difference(queriesBoundariesSet))
            difference = 1000 * dimensionsDifference + boundariesDifference
            if difference < viewCandidate['precision']:
                viewCandidate['uuid'] = aView
                viewCandidate['name'] = allContextViewInfo[query['context']][aView].get("name", "None")
                viewCandidate['precision'] = difference
                viewCandidate['dimensions_and_boundaries'] = sorted(tuple(viewDimensionsSet | viewBoundariesSet))
    #if viewCandidate['uuid'] == '-99': import pdb; pdb.set_trace()
    query['dimensions_and_boundaries'] = sorted(tuple(queriesDimensionsSet | queriesBoundariesSet))
    query['view_uuid'] = viewCandidate['uuid']
    query['view_name'] = viewCandidate['name']
    print(str(dashboards) + ';' + query['context'] + ';' + str(query['dimensions_and_boundaries']) + ';' + str(viewCandidate['dimensions_and_boundaries']) + ';' + query['view_name'] + ';' + query['view_uuid'])


def getDashboardQueries():
    all_queries_hashes = list()
    all_queries_info = list()
    all_queries_dashboards = list() 
    for dashboard in dashboardInfo:
        for query in dashboard['queries']:
            query_info = dict()
            query_info['context'] = query['context']
            boundaries = set()
            dimensions_and_slices = list()
            dimensions = set()
            for apply in query['applys']:
                if apply['function'] == 'timestep': query_info['timestep'] = apply['positionalArguments'][0]
            if 'boundaries' in query.keys():
                for boundary in query['boundaries']:
                    boundaries.add(boundary['boundary'].lower())
            for dimension in query['dimensions']:
                dimensions_and_slices.append(dimension)
            if 'slices' in query.keys():
                for slice in query['slices']:
                    if slice['dimension'] not in dimensions_and_slices:
                        dimensions_and_slices.append(slice['dimension'])
            for dimension in dimensions_and_slices:
                if dimension['base'] == 'timestamp': continue
                if 'split' in dimension.keys(): 
                    dimensions.add(dimension['base'].lower() + '.' + dimension['split'])
                else: dimensions.add(dimension['base'].lower())
            query_info['dimensions'] = sorted(tuple(dimensions))
            query_info['boundaries'] = sorted(tuple(boundaries))
            query_hash = str(query_info)
            if query_hash in all_queries_hashes:
                index = all_queries_hashes.index(query_hash)
                if dashboard['name'] not in all_queries_dashboards[index]:
                    all_queries_dashboards[index].append(dashboard['name'])
            else:
                all_queries_info.append(query_info)
                all_queries_dashboards.append([dashboard['name']])
                all_queries_hashes.append(query_hash)
    queries = dict()
    queries['info'] = all_queries_info
    queries['dashboards'] = all_queries_dashboards
    return queries

def main():

    global allContextViewInfo
    with open('context.json') as f:
        allContextViewInfo = json.load(f)

    global dashboardInfo
    with open('dashboards.json') as f:
        dashboardInfo = json.load(f) 
    
    global dashboardQueries
    dashboardQueries = getDashboardQueries()

    print('dashboard_list;context;query_dimensions_and_boundaries;best_matching_view_dimensions_and_boundaries;best_matching_view_name;best_matching_view_uuid')

    for i in range(len(dashboardQueries['dashboards'])):
        query = dashboardQueries['info'][i] 
        dashboards = dashboardQueries['dashboards'][i]
        if query['context'] in ['traffic','backbone','big_cube']:
            view_uuid(query, dashboards)


if __name__ == "__main__":
    main()
