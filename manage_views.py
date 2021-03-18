import argparse
import requests
from requests.packages.urllib3.exceptions import InsecureRequestWarning
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
import json
import sys
import deepy.deepui

parser = argparse.ArgumentParser()
parser.add_argument("--action",
                            help="show, add-dimension, remove-dimension, or change-timesteps")
parser.add_argument("--context",
                            help="context name")
parser.add_argument("--view",
                            help="view name")
parser.add_argument("--dimension",
                            help="dimension name")
parser.add_argument("--timesteps",
                            help="timestep dictionary, for example '{\"5min\":32,\"30min\":93,\"2hour\":190}'")

args = parser.parse_args()

action = args.action
if action not in ["add-dimension", "remove-dimension", "show", "change-timesteps"]:
    print ("--action can take values add-dimension, remove-dimension, show, or change-timesteps")
    sys.exit(1)

context = args.context
if context is None:
    print ("--context is mandatory")
    sys.exit(1)

view = args.view
if view is None:
    print ("--view is mandatory. Use the view name and not the UUID")
    sys.exit(1)

dimension = args.dimension
if dimension is None and action in [ "add-dimension", "remove-dimension" ]:
    print(action)
    print ("--dimension is mandatory if you try to change the dimension list of a view")
    sys.exit(1)

timesteps = args.timesteps
if timesteps is None and action in [ "change-timesteps" ]:
    print(action)
    print ("--timesteps <timestep_dictionary> is required when action is --change-timesteps, for example --timesteps '{\"5min\":32,\"30min\":93,\"2hour\":190}'")
    sys.exit(1)

base_url = 'https://localhost/api/data_views/'
key = 'api_key=' + deepy.deepui.get_root_api_keys()[0]

def get():
    o = requests.get(base_url + '?' + key, verify=False)
    data = o.json()
    return data

def get_view(data):
    try:
        return [x for x in data['data'] if x['name'] == view and x['context'] == context][0]
    except IndexError as error:
        print('Program exited with error: {}. Have you passed the view uuid instead of the view name?'.format(error))
        sys.exit(1)

def process_view(view, action, dimension):
    view = view.copy()
    splitDim = dimension.split('.')
    if len(splitDim) in [ 1, 3 ]:
        prettyDim = {'base':dimension}
    elif len(splitDim) == 2:
        prettyDim = {'base':splitDim[0],'split':splitDim[1]}
    else:
        print ("error in dimension format")
        sys.exit(1)
    if action == 'remove-dimension':
        view['dimensions'].remove(prettyDim)
    elif action == 'add-dimension':
        view['dimensions'].append(prettyDim)
    print ('dimensions: ' + str(view['dimensions']))
    if action == 'show':
        print ('retention :' + str(view['timesteps']))
    return view

def put_view(view):
    view = view.copy()
    uuid = view['uuid']
    content_type = "Content-Type: application/json"
    url = base_url + uuid + '?' + key

    if action != 'change-timesteps':
        retention = {}
        for step, value in view['timesteps'].items():
            retention[step] = value['retention_days']
        view['timestep_retention_days'] = retention
        del view['timesteps']

        if action == 'add-dimension':
            view['comment'] = 'Adding dimension ' + dimension + ' to view ' + view['name']
        if action == 'remove-dimension':
            view['comment'] = 'Removing dimension ' + dimension + ' from view ' + view['name']
        return requests.put(url, headers={'content_type':content_type}, data=json.dumps(view), verify=False)

    else:
        view['timestep_retention_days'] = json.loads(args.timesteps)
        del view['timesteps']
        view['comment'] = 'Adjusting retention to align to automatic timesteps'
        return requests.patch(url, headers={'content_type':content_type}, data=json.dumps(view), verify=False)


data = get()
view = get_view(data)
if dimension is None:
    dimension = ' '
view = process_view(view,action,dimension)
if action != 'show':
    resp = put_view(view)
    print(resp)
