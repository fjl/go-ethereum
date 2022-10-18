import sys
from pprint import pformat
import os
from python.header import *
from python.network import *
from python.workload_gen import *

#turn a running config into a folder name
def params_to_dir(params, type):
    result = ""
    for param in params:
        if(features[param]['type'] == type):
            result += "_" + param + "-" + str(params[param])
    return result

def run_workload(out_dir,params):
    print("Starting registrations...")
    zipf = TruncatedZipfDist(zipf_exponent, params['topic'])

    config = read_config(out_dir)

    node_to_topic = register_topics(zipf, config)

    # wait for registrations to complete
    print("Searching for topics...")
    # search
    time.sleep(10)
    search_topics(zipf, config, node_to_topic)
    for future in PROCESSES:
        try:
            result = future.result()
        except Exception:
            traceback.print_exc()
            print('Unable to get the result')

    write_logs_to_file(os.path.join(out_dir, "logs", "logs.json"))

def main() -> int:

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
                    out_dir = result_dir + "/attack/" + params_to_dir(params, type='attack') + "/"
                else:    
                    out_dir = result_dir + "/benign/" + params_to_dir(params, type='benign') + "/"
                os.system('mkdir -p ' + out_dir)

                run_testbed(out_dir,params)
                wait_for_nodes_ready(read_config(out_dir),node_neighbor_count)

                run_workload(out_dir,params)
                print("Workload done.")
                stop_testbed(params)

if __name__ == '__main__':
    sys.exit(main())