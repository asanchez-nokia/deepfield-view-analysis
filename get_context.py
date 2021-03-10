import re
import deepy.cfg
import deepy.deepui
import deepy.dimensions.util

import requests
from requests.packages.urllib3.exceptions import InsecureRequestWarning
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
import json
import os

# Define Regex paterns to extract query-fields
# pattern to extract the boundary fields from a boundary dimension 
boundarydim_regex = re.compile(r"boundary\.([\w.-]*)\.(\w*)")


class Context():

    contextsToEvaluate = ['traffic','backbone','big_cube']

    def __init__(self, contextList = None, reEvaluate = True):
        if contextList is not None:
            self.contextsToEvaluate = list(set(contextList) | set(self.contextsToEvaluate))
        self.boundaryMap = self.getBoundaryMap()
        if not reEvaluate and os.path.exists('context.json'):
            with open('context.json') as f:
                self.allContextViewInfo = json.load(f)
            with open('dashboards.json') as f:
                self.dashboardInfo = json.load(f)
        else:
            self.ddb = deepy.dimensions.ddb.get_local_ddb()
            self.allContextViewInfo = self.storeAllContextViewInfo(self.contextsToEvaluate)
            with open('context.json', 'w') as f:
                json.dump(allContextViewInfo, f)
            self.dashboardInfo = self.getDashboardInfo()
            with open('dashboards.json', 'w') as f:
                json.dump(self.dashboardInfo, f)

    def storeAllContextViewInfo(self, contextsToEvaluate):
        allTheThings = {}
        # Get the pipedream version
        pipedreamVersion = str(deepy.cfg.slice_config.get("build_updates", {}).get("revision"))
        if pipedreamVersion.startswith("5"):
            lookForViewsInMysql = True
        else:
            lookForViewsInMysql = False
    
        for context in contextsToEvaluate:
            if lookForViewsInMysql:
                allTheThings[context] = self.getSqlViews(context)
            else:
                allTheThings[context] = self.getOldViews(context)
        return allTheThings
    
    def getViewDimensionsAndBoundaries(self, view, context):
        named_dimensions = []
        named_boundaries = []
        view_type = 'simple'
        dimensions = view.get("dimensions")
        if dimensions is None:
            dimensions = context.get("dimensions")
        if dimensions is None:
            dimensions = []
        for dim in dimensions:
            named_dim = deepy.dimensions.util.dim_id_to_name(self.ddb, dim)
            mo = boundarydim_regex.search(named_dim)
            if mo:
                view_type = 'explicit_boundary'
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
        view_properties['type'] = view_type
        
        return view_properties
    
    
    def getSqlViews(self, context_id):
        from deepy.context import sql_context_util
        listOfViews = {}
        context_json = sql_context_util.get_merged_contexts(context=context_id)
        for view in context_json[context_id].get("views", []):
            viewDimensionsAndBoundaries = self.getViewDimensionsAndBoundaries(view, context_json)
            listOfViews[view.get("uuid", view.get("name"))] = {
                "dimensions": viewDimensionsAndBoundaries['dimensions'],
                "boundaries": viewDimensionsAndBoundaries['boundaries'],
                "type": viewDimensionsAndBoundaries['type'],
                "timesteps": view.get("timesteps"),
                "retention": view.get("retention"),
                "name": view.get("name")
            }
        return listOfViews
    
    
    def getOldViews(self, context_id):
        listOfViews = {}
        local_path = deepy.cfg.context_dir + "/%s.json" % context_id
        context_json = deepy.cfg.connector_store.simple_load_json(local_path)
        if not context_json:
            return
    
        for view in context_json[context_id].get("views", []):
            viewDimensionsAndBoundaries = self.getViewDimensionsAndBoundaries(view, context_json)
            listOfViews[view.get("uuid")] = {
                "dimensions": viewDimensionsAndBoundaries['dimensions'],
                "boundaries": viewDimensionsAndBoundaries['boundaries'],
                "type": viewDimensionsAndBoundaries['boundaries'],
                "timesteps": view.get("timesteps"),
                "retention": view.get("retention")
            }
        return listOfViews
    
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
