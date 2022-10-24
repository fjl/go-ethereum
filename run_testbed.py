#!/usr/bin/env python3

import sys
from pprint import pformat
import os
import atexit

from python.header import *
from python.network import *
import python.workload_gen as workload
import python.analyse_logs as analysis

import argparse

def parseArguments():
    # Create argument parser
    parser = argparse.ArgumentParser()

    # Print version
    parser.add_argument("--docker", help="enable docker testbed",action=argparse.BooleanOptionalAction)

    # Parse arguments
    args = parser.parse_args()

    return args

# params_to_dir turns a running config into a folder name
def params_to_dir(params, type):
    result = ""
    for param in params:
        if(features[param]['type'] == type):
            result += "_" + param + "-" + str(params[param])
    return result


def main(docker) -> int:
    already_run = set()
    params = {}
    already_run = set()
    is_attack = False

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
                if(is_attack):
                    out_dir = os.getcwd()+result_dir + "/attack/" + params_to_dir(params, type='attack') + "/"
                else:
                    out_dir = os.getcwd()+result_dir + "/benign/" + params_to_dir(params, type='benign') + "/"
                os.system('mkdir -p ' + out_dir)

                network = NetworkLocal(params)
                if docker:
                    network = NetworkDocker(params)

                atexit.register(network.stop)

                run_testbed(network, out_dir, params)

                workload.run_workload(network, params, out_dir)
                print("Workload done.")
                network.stop()
                atexit.unregister(network.stop)
                analysis.analyze(out_dir)


if __name__ == '__main__':
    args = parseArguments()
    sys.exit(main(args.docker))
