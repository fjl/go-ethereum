import sys
from pprint import pformat
import os
from python.header import *
from python.network import *
from python.workload_gen import *
from python.analyse_logs import *

import argparse

def parseArguments():
    # Create argument parser
    parser = argparse.ArgumentParser()

    # Print version
    parser.add_argument("--docker", help="enable docker testbed",action=argparse.BooleanOptionalAction)

    # Parse arguments
    args = parser.parse_args()

    return args

#turn a running config into a folder name
def params_to_dir(params, type):
    result = ""
    for param in params:
        if(features[param]['type'] == type):
            result += "_" + param + "-" + str(params[param])
    return result

def run_workload(config,out_dir,params,docker):
    print("Starting registrations...")
    zipf = TruncatedZipfDist(zipf_exponent, params['topic'])

    node_to_topic = register_topics(zipf, config,docker)

    # wait for registrations to complete
    # search
    #print("Waiting adlifetime...")
    time.sleep(params['adLifetimeSeconds'])
    print("Searching for topics...")
    search_topics(zipf, config, node_to_topic,docker)
    for future in PROCESSES:
        try:
            result = future.result()
        except Exception:
            traceback.print_exc()
            print('Unable to get the result')

    write_logs_to_file(os.path.join(out_dir, "logs", "logs.json"))


def analyze(out_dir,params):

    logs_dir = out_dir+"logs/"
    fig_dir = out_dir+'figs/'

    if not os.path.exists(fig_dir):
        os.mkdir(fig_dir)

    op_df = get_op_df(logs_dir)

    msg_df = get_msg_df(logs_dir, op_df)
    msg_df = msg_df.dropna(subset=['opid'])

    plot_operation_returned(fig_dir,op_df)

    plot_operation_times(fig_dir,op_df)

    plot_msg_operation(fig_dir, msg_df)

    plot_msg_sent_recv(fig_dir,msg_df)

    plot_msg_sent_recv2(fig_dir,msg_df)

    plot_msg_sent_distrib(fig_dir,msg_df)

    plot_msg_topic(fig_dir,msg_df)

    plot_times_discovered(fig_dir,op_df)

    plot_search_results(fig_dir,op_df)

    plot_waiting_time(fig_dir,msg_df)

    storage_df = get_storage_df(logs_dir)
#print('Storage_df:', storage_df)
    plot_storage_per_node_over_time(fig_dir, storage_df)    

def main(dock) -> int:

    already_run = set()
    params = {}
    already_run = set()
    is_attack = False
    global docker
    docker = dock

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
                run_testbed(out_dir,params,docker)
                config = read_config(out_dir)
                if not docker:
                    wait_for_nodes_ready(config,node_neighbor_count)
                else :
                    wait_for_nodes_ready(config,node_neighbor_count_docker)
                run_workload(config,out_dir,params,docker)
                print("Workload done.")
                stop_testbed(params,docker)
                analyze(out_dir,params)

if __name__ == '__main__':

        # Parse the arguments
    args = parseArguments()

    sys.exit(main(args.docker))
