# DB hostname to connect
"""
config for global variable in this project
"""
import yaml
import os
from pymongo import MongoClient
import sys
import re
from subprocess import Popen, call, check_output

# Default values if key is not specified in the yaml
DEFAULT_CONFIG = {
    'tmp_path': './tmp',
    'mongo_user': None,
    'mongo_pwd': None,
    'localserver_port': 24680,
    'mongo_db': 'fable'
}

def config(key):
    return var_dict.get(key)


def set_var(key, value):
    exec(f'{key.upper()} = {value}', globals())
    exec(f'{var_dict[key.upper()]} = {value}', globals())


def unset(key):
    # TODO: Also unset the defined variables
    try:
        del(var_dict[key])
    except: pass


def back_default():
    global var_dict
    var_dict = default_var_dict.copy()
    # TODO Update defined variables

var_dict = {}
default_var_dict = {}
CONFIG_PATH = os.environ['FABLE_CONFIG'] if 'FABLE_CONFIG' in os.environ else os.path.dirname(__file__)
# if not os.path.exists(os.path.join(CONFIG_PATH, 'config.yml')):
#     raise Exception("No config yaml file find at:", CONFIG_PATH)
# else:
config_yml = yaml.load(open(os.path.join(CONFIG_PATH, 'config.yml'), 'r'), Loader=yaml.FullLoader)
var_dict.update(config_yml)
locals().update({k.upper(): v for k, v in var_dict.items()})
if config_yml.get('proxies') is not None:
    PROXIES = [{'http': ip, 'https': ip } for ip in \
            config_yml.get('proxies')]
else: PROXIES = []
PROXIES = PROXIES + [{}]  # One host do not have to use proxy
var_dict['proxies'] = PROXIES

default_var_dict = var_dict.copy()

for dc, dc_value in DEFAULT_CONFIG.items():
    if dc not in var_dict:
        dc = dc.upper()
        locals().update({dc: dc_value})
        var_dict.update({dc: dc_value})

if 'mongo_url' not in var_dict:
    DB_CONN = eval(f"MongoClient(MONGO_HOSTNAME, username=MONGO_USER, password=MONGO_PWD, authSource='admin')")
    DB = eval(f"MongoClient(MONGO_HOSTNAME, username=MONGO_USER, password=MONGO_PWD, authSource='admin').{MONGO_DB}")
else:
    DB_CONN =  eval(f"MongoClient('{MONGO_URL}')")
    DB =  eval(f"MongoClient('{MONGO_URL}').{MONGO_DB}")


NULL = open('/dev/null', 'w')
def localserver(PORT):
    """
    Create tmp dir at $PROJ_HOME, copy domdistiller.js into the repo
    Serve a local server at port if it not occupied by any others
    """
    cur_path = os.path.dirname(__file__)
    call(['mkdir', '-p', TMP_PATH])
    if not os.path.exists(os.path.join(TMP_PATH, 'utils', 'domdistiller.js')):
        call(['cp', os.path.join(cur_path, 'utils', 'domdistiller.js'), TMP_PATH])
    port_occupied = re.compile(":{}".format(LOCALSERVER_PORT)).findall(check_output(['netstat', '-nlt']).decode())
    if len(port_occupied) <= 0:
        Popen(['http-server', '-a', 'localhost', '-p', str(PORT), TMP_PATH], stdout=NULL, stderr=NULL)
    else:
        print(f"Port {LOCALSERVER_PORT} occupied by other process", file=sys.stderr)

localserver(LOCALSERVER_PORT)