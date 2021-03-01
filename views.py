import argparse
import requests
from requests.packages.urllib3.exceptions import InsecureRequestWarning
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
import json
import sys

parser = argparse.ArgumentParser()
parser.add_argument("--action",
                            help="add, remove, or show")
parser.add_argument("--context",
                            help="context name")
parser.add_argument("--view",
                            help="view name")
parser.add_argument("--dimension",
                            help="dimension name")

args = parser.parse_args()

action = args.action
if action not in ["add", "remove", "show"]:
    print ("--action can take values add, remove, or show")
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
if dimension is None and action != "show":
    print(action)
    print ("--dimension is mandatory")
    sys.exit(1)

base_url = 'https://localhost/api/data_views/'
key = 'api_key=1ruBGjlbKOYTW8'

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
    if len(splitDim) == 1:
        prettyDim = {'base':dimension}
    elif len(splitDim) == 2:
        prettyDim = {'base':splitDim[0],'split':splitDim[1]}
    else:
        print ("error in dimension format")
        sys.exit(1)
    if action == 'remove':
        view['dimensions'].remove(prettyDim)
    elif action == 'add':
        view['dimensions'].append(prettyDim)
    print (view['dimensions'])
    return view

def put_view(view):
    view = view.copy()
    uuid = view['uuid']

    retention = {}
    for step, value in view['timesteps'].items():
        retention[step] = value['data_age_days']
    view['timestep_retention_days'] = retention
    del view['timesteps']

    if action == 'add':
        view['comment'] = 'Adding dimension ' + dimension + ' to view ' + view['name']
    if action == 'remove':
        view['comment'] = 'Removing dimension ' + dimension + ' from view ' + view['name']

    content_type = "Content-Type: application/json"
    url = base_url + uuid + '?' + key

    return requests.put(url, headers={'content_type':content_type}, data=json.dumps(view), verify=False)

data = get()
view = get_view(data)
if dimension is None:
    dimension = ' '
view = process_view(view,action,dimension)
if action != 'show':
    resp = put_view(view)
    print(resp)