import re
import argparse

import deepy.log as log
import get_context

# Define Regex paterns to extract query-fields
# pattern to extract the boundary fields from a boundary dimension 
boundarydim_regex = re.compile(r"boundary\.([\w.-]*)\.(\w*)")

def parse_args():
    p = argparse.ArgumentParser(
        description="""
        A utility to inspect view coverage for all dashboards.
        """,
        formatter_class=argparse.RawTextHelpFormatter,
    )
    p.add_argument(
        "--no-cache",
        dest="nocache",
        default=False,
        action="store_true",
        help="Reevaluate all definitions for contexts, views, and dimensions.",
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

def getDashboardQueries():
    log.info('Matching dashboard queries to existing views')
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

    args = parse_args()

    # Storing all information about views in the global namespace.
    global allContextInfo
    global boundaryMap
    global allContextViewInfo

    allContextInfo = get_context.Context(contextList=args.contexts, reEvaluate=args.nocache, callingProgram='get_dashboards')
    boundaryMap = allContextInfo.boundaryMap
    allContextViewInfo = allContextInfo.allContextViewInfo

    global dashboardInfo 
    global dashboardQueries

    dashboardInfo = allContextInfo.dashboardInfo
    dashboardQueries = getDashboardQueries()

    print('dashboard_list;context;query_dimensions_and_boundaries;best_matching_view_dimensions_and_boundaries;best_matching_view_name;best_matching_view_uuid')

    for i in range(len(dashboardQueries['dashboards'])):
        query = dashboardQueries['info'][i] 
        dashboards = dashboardQueries['dashboards'][i]
        if query['context'] in ['traffic','backbone','big_cube']:
            query = allContextInfo.view_uuid(query)
            print(str(dashboards) + ';' + query['context'] + ';' + str(sorted(tuple(set(query['dimensions'])|set(query['boundaries'])))) + ';' + str(query['matching_view_dimensions'])
                  + ';' + query['name'] + ';' + query['uuid'])

if __name__ == "__main__":
    main()
