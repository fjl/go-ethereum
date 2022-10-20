import os
import json
import time
import subprocess
import psutil

IP1=172
IP2=20
IP3=0

run_param={'adCacheSize','adLifetimeSeconds','regBucketSize','searchBucketSize'}
proc = []
# get_node_url returns the enode URL of node $i.
def get_node_url(path,udpBasePort,n):
    port = udpBasePort + n
    url = os.popen("./devp2p key to-enr --ip 127.0.0.1 --tcp 0 --udp "+str(port)+" "+path+"keys/node-"+str(n)+".key").read().split('\n')
    return url[0]

def get_node_url_docker(path,udpBasePort,n):
    port = udpBasePort
    url = os.popen("./devp2p key to-enr --ip "+get_network(n)+" --tcp 0 --udp "+str(port)+" "+path+"keys/node-"+str(n)+".key").read().split('\n')
    return url[0]


def get_network(node):
    IP3=IP3+node
    while IP3 > 255:
      IP3=IP3-256
      IP2=IP2+1

    while IP2 < 255:
      IP2=IP2-256
      IP1=IP1+1

    ip=str(IP1)+"."+str(IP2)+"."+str(IP3)
    return ip

def start_network(nodes):
	for i in range(1,nodes+1):
		os.system('docker network create -d bridge node'+i+'-network --subnet='+get_network(i)+'.0/24 --gateway='+get_network(i)+'.1')

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
        
def start_nodes(config_path,params,docker):

    print("Starting all nodes..")
    if docker:
        bootnode=get_node_url_docker(config_path,params['udpBasePort'],1)
    else :
        bootnode=get_node_url(config_path,params['udpBasePort'],1)

    print("Bootstrap node: "+str(bootnode))

    n = params['nodes']
    udpBasePort = params['udpBasePort']
    rpcBasePort = params['rpcBasePort']

    if docker:
        start_network(n)

    for i in range(1,n+1):
        #print("starting node "+str(i))

        keyfile=config_path+"keys/node-"+str(i)+".key"
        logfile=config_path+"logs/node-"+str(i)+".log"
        logflags="--verbosity 5"
        logflags+=" --log.json"

        nodekey=open(keyfile,"r").read()
        #print("port:"+str(port)+" rpc:"+str(rpc))

        #os.system("./devp2p $logflags discv5 listen $nodeflags 2>"+logfile+" >"+logfile+" &")
        log = open(logfile, 'a')
        #print("./devp2p "+logflags+" discv5 listen "+nodeflags)
        if docker:
            start_network(n)
            logfile="/go-ethereum/discv5-test/logs/node-"+str(i)+".log"
            nodeflags="--bootnodes "+bootnode+" --nodekey "+nodekey+" --addr 127.0.0.1:"+str(udpBasePort)+" --rpc 127.0.0.1:"+str(rpcBasePort) 
            nodeflags+=" --config "+config_path+"config.json"
            os.system("docker run --network node"+i+"-network --cap-add=NET_ADMIN --name node"+i+" --mount type=bind,source="+config_path+",target=/go-ethereum/discv5-test devp2p sh -c './devp2p "+logflags+" discv5 listen "+nodeflags+" 2>&1 | tee "+logfile+" ' &")
        else:
            port=udpBasePort+i
            rpc=rpcBasePort+i
            nodeflags="--bootnodes "+bootnode+" --nodekey "+nodekey+" --addr 127.0.0.1:"+str(port)+" --rpc 127.0.0.1:"+str(rpc) 
            nodeflags+=" --config "+config_path+"config.json"
            process = subprocess.Popen(["./devp2p "+logflags+" discv5 listen "+nodeflags],stdout=log,stderr=log,shell=True, preexec_fn=os.setsid)
            proc.append(process)

    print("Nodes started")

def stop_nodes(n):
    print("Stopping nodes..")
    for p in proc:
        kill(p.pid)

def stop_docker_nodes(n):
    print("Stopping nodes..")
    for p in proc:
        kill(p.pid)

def run_testbed(config_path,params,docker):
    
    build()
    make_keys(config_path,params['nodes'])
    write_experiment(config_path,params)
    start_nodes(config_path,params,docker)
    time.sleep(10)

def stop_testbed(params,docker):
    if docker:
        stop_nodes(params['nodes'])
    else :
        stop_docker_nodes(params['nodes'])

def kill(proc_pid):
    #print("Process:"+str(proc_pid))
    process = psutil.Process(proc_pid)
    for proc in process.children(recursive=True):
        proc.kill()
    process.kill()
