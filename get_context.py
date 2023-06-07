import re
import deepy.cfg
import deepy.deepui
import deepy.dimensions.util
import deepy.log as log

import os
import requests
from requests.packages.urllib3.exceptions import InsecureRequestWarning
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
import json

# Define Regex paterns to extract query-fields
# pattern to extract the boundary fields from a boundary dimension
boundarydim_regex = re.compile(r"boundary\.([\w.-]*)\.(\w*)")


class Context():

    def __init__(self, contextList, reEvaluate = False, callingProgram = 'user_query_summary'):
        if not os.path.exists('context.json') or (not os.path.exists('dashboards.json') and callingProgram=='get_dashboards'):
            reEvaluate = True
        self.contextsToEvaluate = contextList
        self.boundaryMap = self.getBoundaryMap()
        if not reEvaluate:
            with open('context.json') as f:
                self.allContextViewInfo = json.load(f)
                for context in contextList:
                     if context not in self.allContextViewInfo.keys():
                         reEvaluate = True
                         return
            log.info("Loaded context, view, and dimension info from local file 'context.json', use --no-cache if you want fresher info.")
            if callingProgram == 'get_dashboards':
                log.info("Loading dashboard definitions from local file 'dashboards.json', use --no-cache if you want fresher info.")
                with open('dashboards.json') as f:
                    self.dashboardInfo = json.load(f)
        if reEvaluate:
            log.info("Loading context, view, and dimension info from Deepfield API, please wait...")
            self.ddb = deepy.dimensions.ddb.get_local_ddb()
            self.allContextViewInfo = self.storeAllContextViewInfo(self.contextsToEvaluate)
            log.info("Storing context, view, and dimension info into local file 'context.json' for future caching")
            with open('context.json', 'w') as f:
                json.dump(self.allContextViewInfo, f)
            if callingProgram == 'get_dashboards':
                log.info("Loading dashboard definitions from Deepfield API")
                self.dashboardInfo = self.getDashboardInfo()
                log.info("Storing dashboard definitions into local file 'dashboards.json' for future caching")
                with open('dashboards.json', 'w') as f:
                    json.dump(self.dashboardInfo, f)

    def storeAllContextViewInfo(self, contextsToEvaluate):
        allTheThings = {}

        for context in contextsToEvaluate:
            allTheThings[context] = self.getSqlViews(context)
        return allTheThings

    def getViewDimensionsAndBoundaries(self, view, context):
        named_dimensions = []
        named_boundaries = []
        dimensions = view.get("dimensions")
        if dimensions is None:
            dimensions = context['traffic']['dimensions'] + ['all_boundary_columns_macro']
        for dim in dimensions:
            named_dim = deepy.dimensions.util.dim_id_to_name(self.ddb, dim)
            mo = boundarydim_regex.search(named_dim)
            if mo:
                named_boundary = 'boundary.' + self.boundaryMap[int(mo.group(1))] + '.' + mo.group(2)
                named_boundaries.append(named_boundary)
            elif named_dim == 'all_boundary_columns_macro':
                for boundary in self.boundaryMap.values():
                    split_boundary = [ 'boundary.' + boundary + '.input', 'boundary.' + boundary + '.output' ]
                    named_boundaries = named_boundaries + split_boundary
            else:
                named_dimensions.append(deepy.dimensions.util.dim_id_to_name(self.ddb, dim))

        view_properties = dict()
        view_properties['dimensions'] = sorted(named_dimensions)
        view_properties['boundaries'] = sorted(named_boundaries)

        return view_properties


    def getSqlViews(self, context_id):
        from deepy.context import sql_context_util
        listOfViews = {}
        context_json = sql_context_util.get_merged_contexts(context=context_id)
        dimensions = context_json[context_id]['dimensions']
        for view in context_json[context_id].get("views", []):
            viewDimensionsAndBoundaries = self.getViewDimensionsAndBoundaries(view, context_json)
            listOfViews[view.get("uuid", view.get("name"))] = {
                "dimensions": viewDimensionsAndBoundaries['dimensions'],
                "boundaries": viewDimensionsAndBoundaries['boundaries'],
                "timesteps": view.get("timesteps"),
                "retention": view.get("retention"),
                "name": view.get("name")
            }
        return {"views": listOfViews, "dimensions": dimensions}

    def getBoundaryMap(self):
        apiKey = deepy.deepui.get_root_api_keys()[0]
        total_size = None
        url = 'https://localhost/api/boundaries?api_key=' + apiKey
        response = requests.get(url, verify=False)
        boundaryMap = dict()
        for boundary in response.json():
            boundaryMap[boundary['id']] = boundary['name'].lower()
        return boundaryMap

    def getDashboardInfo(self):
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

    def view_uuid(self, row):
        dimensions = row['dimensions'].to_string().strip(' ()').rstrip(',').replace(' ','')
        dimensions = [] if dimensions == '' else dimensions.split(',')
        boundaries = row['boundaries'].to_string().strip(' ()').rstrip(',').replace(' ','')
        boundaries = [] if boundaries == '' else boundaries.split(',')
        context = row['context'].to_string().strip(' ')
        for dimension in dimensions:
            if dimension not in self.allContextViewInfo[context]['dimensions']:
                row['name'] = 'Invalid Dimensions List'
                row['uuid'] = ''
                return row
        viewCandidate = {"name": "No_Match", "uuid": "", "precision": 999000, "dimensions_and_boundaries": ''}
        explicitTimestep = row["explicitTimestep"].to_string().strip(' ')
        if explicitTimestep != '':
            timestep = explicitTimestep
        else:
            timestep = row["autoTimestep"].to_string().strip(' ')
        if timestep == '10s':
            row["name"] = 'traffic_step10'
            row["uuid"] = ''
            return row
        for aView in self.allContextViewInfo[context]['views']:
            if timestep not in self.allContextViewInfo[context]['views'][aView].get("timesteps", []):
                continue
            viewDimensionsSet = set(map(lambda x:x.lower(), self.allContextViewInfo[context]['views'][aView].get("dimensions", [])))
            queriesDimensionsSet = set(map(lambda x:x.lower(), dimensions))
            viewBoundariesSet = set(map(lambda x:x.lower(), self.allContextViewInfo[context]['views'][aView].get("boundaries", [])))
            queriesBoundariesSet = set(map(lambda x:x.lower(), boundaries))
            if (viewDimensionsSet|viewBoundariesSet).issuperset(queriesDimensionsSet) and viewBoundariesSet.issuperset(queriesBoundariesSet):
                dimensionsDifference = len(viewDimensionsSet.difference(queriesDimensionsSet))
                boundariesDifference = len(viewBoundariesSet.difference(queriesBoundariesSet))
                difference = 1000 * dimensionsDifference + boundariesDifference
                if difference < viewCandidate['precision']:
                    viewCandidate['uuid'] = aView
                    viewCandidate['name'] = self.allContextViewInfo[context]['views'][aView].get("name", "None")
                    viewCandidate['precision'] = difference
                    viewCandidate['dimensions_and_boundaries'] = sorted(tuple(viewDimensionsSet | viewBoundariesSet))
        row['uuid'] = viewCandidate['uuid']
        row['name'] = viewCandidate['name']
        row['matching_view_dimensions'] = viewCandidate['dimensions_and_boundaries']
        return row
