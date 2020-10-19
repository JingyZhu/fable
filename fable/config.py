# DB hostname to connect
"""
config for global variable in this project
"""
import yaml
import os
from pymongo import MongoClient

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
if not os.path.exists(os.path.join(os.path.dirname(__file__), 'config.yml')):
    print("No config yaml file find")
else:
    config_yml = yaml.load(open(os.path.join(os.path.dirname(__file__), 'config.yml'), 'r'), Loader=yaml.FullLoader)
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

DB = eval(f"MongoClient(MONGO_HOSTNAME, username=MONGO_USER, password=MONGO_PWD, authSource='admin').{MONGO_DB}")