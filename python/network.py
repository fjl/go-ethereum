import os
import json
import time
import subprocess
import signal

run_param={'adCacheSize','adLifetimeSeconds','regBucketSize','searchBucketSize'}
proc = []
# get_node_url returns the enode URL of node $i.
def get_node_url(path,udpBasePort,n):
    port = udpBasePort + n
    url = os.popen("./devp2p key to-enr --ip 127.0.0.1 --tcp 0 --udp "+str(port)+" "+path+"keys/node-"+str(n)+".key").read().split('\n')
    return url[0]

def build():
    result = os.system("go build ./cmd/devp2p")
    assert(result == 0)

# make_keys creates all node keys.
def make_keys(config_path,n):
    print("building keys...")
    result = os.makedirs(config_path+"keys/",exist_ok=True)

    for i in range(1,n+1):
        file=config_path+"keys/node-"+str(i)+".key"
        if not os.path.exists(file):
            result = os.system("./devp2p key generate "+file)
            assert(result == 0)

def filter_params(params):
    result={}
    for param in params:
        if(param in run_param):
            result[param]=params[param]
    return result

def write_experiment(config_path,params):
    print("writing experiments params..")
    if os.path.exists(config_path+"logs/"):
        for filename in os.listdir(config_path+"logs/"):
            result = os.remove(config_path+"logs/"+filename)
    else:
        result = os.mkdir(config_path+"logs/")

    with open(config_path+'config.json', 'w') as f:
        f.write(json.dumps(filter_params(params)))
    with open(config_path+'experiment.json', 'w') as f:
        f.write(json.dumps(params))
        
def start_nodes(config_path,params,json):

    print("Starting all nodes..")
    bootnode=get_node_url(config_path,params['udpBasePort'],1)
    print("Bootstrap node: "+str(bootnode))

    n = params['nodes']
    udpBasePort = params['udpBasePort']
    rpcBasePort = params['rpcBasePort']

    for i in range(1,n+1):
        #print("starting node "+str(i))
        port=udpBasePort+i
        rpc=rpcBasePort+i

        keyfile=config_path+"keys/node-"+str(i)+".key"
        logfile=config_path+"logs/node-"+str(i)+".log"
        logflags="--verbosity 5"
        if json:
            logflags+=" --log.json"

        nodekey=open(keyfile,"r").read()
        #print("port:"+str(port)+" rpc:"+str(rpc))

        nodeflags="--bootnodes "+bootnode+" --nodekey "+nodekey+" --addr 127.0.0.1:"+str(port)+" --rpc 127.0.0.1:"+str(rpc) 
        nodeflags+=" --config "+config_path+"config.json"

        #os.system("./devp2p $logflags discv5 listen $nodeflags 2>"+logfile+" >"+logfile+" &")
        log = open(logfile, 'a')
        #print("./devp2p "+logflags+" discv5 listen "+nodeflags)
        proc.append(subprocess.Popen(["./devp2p "+logflags+" discv5 listen "+nodeflags],stdout=log,stderr=log,shell=True))

    print("Nodes started")

def stop_nodes(n):
    print("Stopping nodes..")
    for i in range(n):
        #print(proc[i].pid)
        proc[i].kill()
        #os.kill(proc[i].pid, signal.SIGKILL

def run_testbed(config_path,params):
    
    build()
    make_keys(config_path,params['nodes'])
    write_experiment(config_path,params)
    start_nodes(config_path,params,True)

def stop_testbed(params):
    stop_nodes(params['nodes'])
