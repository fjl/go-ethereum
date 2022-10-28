#!/usr/bin/env python3

import sys
from pprint import pformat
import os
import atexit

from python.header import *
from python.network import *
import python.workload_gen2 as workload_gen
import python.analyse_logs as analyse_logs

import argparse

def parseArguments():
    parser = argparse.ArgumentParser()
    parser.add_argument("--docker", help="enable docker testbed", action=argparse.BooleanOptionalAction)
    parser.add_argument("--analysis", help="run analysis after test", default=True, action=argparse.BooleanOptionalAction)
    parser.add_argument("--config", help="run chosen configuration", type=str)
    parser.add_argument("--name", help="experiment name", type=str)
    return parser.parse_args()

# params_to_dir turns a running config into a folder name
def params_to_dir(params, type):
    namedict = {}
    for param in sorted(params):
        if param in features and (features[param]['type'] == type):
            namedict[param] = params[param]
    result = ""
    for key in namedict:
        if result != "":
            result += "_"
        result += key + "-" + str(namedict[key])
    return result

def load_config_file(file):
    with open(file) as f:
        config = json.load(f)
    assert isinstance(config, dict)

    for key in features:
        if key not in config:
            config[key] = features[key]['default']

    return config

def create_result_directory(cliargs: argparse.Namespace, params: dict,attack):
    if cliargs.name:
        dir_base = cliargs.name
    elif attack:
        dir_base = os.path.join("attack", params_to_dir(params, type='attack'))
    else:
        dir_base = os.path.join("benign", params_to_dir(params, type='benign'))

    global result_dir
    dir = os.path.join(os.getcwd(), result_dir, dir_base) + "/"
    os.makedirs(dir, exist_ok=True)
    return dir

def run_it(network: Network, cliargs: argparse.Namespace, params: dict, attack=False):
    out_dir = create_result_directory(cliargs, params,attack)

    run_testbed(network, out_dir, params)

    workload_gen.run_workload(network, params, out_dir)
    print("Workload done.")
    network.stop()

    if cliargs.analysis:
        analyse_logs.analyse(out_dir)

def main(args) -> int:
    network = NetworkLocal()
    if args.docker:
        network = NetworkDocker()
    atexit.register(network.stop)

    params = {}
    already_run = set()
    is_attack = False

    if args.config:
        params = load_config_file(args.config)
        run_it(network, args, params)
        return

    for main_feature in features.keys():
        is_attack_feature = features[main_feature]['type'] == 'attack'
        #don't iterate benign features for attack configs
        #and vice-versa
        if( is_attack != is_attack_feature):
            continue

        for val in features[main_feature]['vals']:
            params[main_feature] = val
            #set default values for the remaining features
            for feature in features.keys():
                if feature != main_feature:
                    #use defaultAttack for attack features during an attack scenario
                    if(is_attack and (features[feature]['type'] == 'attack') ):
                        params[feature] = features[feature]['defaultAttack']
                    else:
                        params[feature] = features[feature]['default']

            #print("params:", params)
            #by default you can't have a set of dictionaries
            #pformat turns a dictionary into a string that can be added to the set
            if(pformat(params) not in already_run):
                already_run.add(pformat(params))
                run_it(network, args, params, attack=is_attack)


if __name__ == '__main__':
    args = parseArguments()
    sys.exit(main(args))
