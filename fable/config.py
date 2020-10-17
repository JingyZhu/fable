# DB hostname to connect
"""
config for global variable in this project
"""
import yaml
import os

MONGO_HOSTNAME='redwings.eecs.umich.edu'

LOCALSERVER_PORT=24680

# TODO: Make this initialization dynamic

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

def apply_custom(key):
    custom_yml = yaml.load(open(os.path.join(os.path.dirname(__file__), 'custom_setups.yml'), 'r'), Loader=yaml.FullLoader)
    setups = custom_yml.get(key)
    if setups is None: return
    for k, v in setups.items():
        set_var(k, v)

def back_default():
    global var_dict
    var_dict = default_var_dict.copy()
    # TODO Update defined variables