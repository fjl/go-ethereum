import json
import os
import os.path
import random
import subprocess
import time


run_param={'adCacheSize','adLifetimeSeconds','regBucketSize','searchBucketSize'}

class Network:
    proc = []

    def stop(self):
        for p in self.proc:
            p.kill()
        self.proc = []


class NetworkLocal(Network):
    config = None

    def __init__(self, config=None):
        self.config = config

    # node_enr returns the ENR of node n.
    def node_enr(self, path, n):
        port = self.config['udpBasePort'] + n
        url = os.popen("./devp2p key to-enr --ip 127.0.0.1 --tcp 0 --udp "+str(port)+" "+path+"keys/node-"+str(n)+".key").read().split('\n')
        return url[0]

    # node_api_url returns the RPC URL of node n.
    def node_api_url(self, n):
        port = self.config['rpcBasePort'] + n
        #print("port:"+str(port))
        url = 'http://127.0.0.1:' + str(port)
        return url

    def start_node(self, n: int, bootnodes=[], log=None, nodekey=None, config_path=None):
        assert log is not None
        assert nodekey is not None
        assert config_path is not None

        port = self.config['udpBasePort'] + n
        rpc = self.config['rpcBasePort'] + n
        nodeflags = [
            "--bootnodes", ','.join(bootnodes),
            "--nodekey", nodekey,
            "--addr", "127.0.0.1:"+str(port),
            "--rpc", "127.0.0.1:"+str(rpc),
            "--config", os.path.join(config_path, "config.json"),
        ]
        logflags = ["--verbosity", "5", "--log.json"]
        argv = ["./devp2p", *logflags, "discv5", "listen", *nodeflags]
        p = subprocess.Popen(argv, stdout=log, stderr=log, preexec_fn=os.setsid)
        self.proc.append(p)


class NetworkDocker(Network):
    config = None
    proc = []

    def __init__(self, config=None):
        self.config = config

    # get_node_url returns the ENR of node n.
    def node_enr(self, path, n):
        ip = self._node_ip(node)
        url = os.popen("./devp2p key to-enr --ip "+(1)+".2 --tcp 0 --udp "+str(port)+" "+path+"keys/node-"+str(1)+".key").read().split('\n')
        return url[0]

    # node_api_url returns the RPC URL of node n.
    def node_api_url(self, n):
        port = self.config['rpcBasePort']
        url = "http://" + self._node_ip_prefix(n) + ".2:" + str(port)
        return url

    def start_node(self, n: int, bootnodes=[], log=None, nodekey=None, config_path=None):
        assert log is not None
        assert nodekey is not None
        assert config_path is not None

        # create node command line
        port = self.config['udpBasePort']
        rpc = self.config['rpcBasePort']
        ip = self._node_ip_prefix(n) + '.2'
        nodeflags = [
            "--bootnodes", ','.join(bootnodes),
            "--nodekey", nodekey,
            "--addr", ip+':'+str(port),
            "--rpc", ip+':'+str(rpc),
            "--config", "/go-ethereum/discv5-test/config.json",
        ]
        logflags = ["--verbosity", "5", "--log.json"]
        node_cmdline = ' '.join(["./devp2p", *logflags, "discv5", "listen", *nodeflags])

        # start the docker container
        d_network = "node"+str(n)+"network"
        d_name = "node"+str(n)
        argv = [
            "docker" "run",
            "--name", d_name,
            "--network", network,
            "--cap-add", "NET_ADMIN",
            "--mount", "type=bind,source="+config_path+",target=/go-ethereum/discv5-test",
            "devp2p",
            "sh", "-c", d_cmdline,
        ]
        p = subprocess.Popen(argv, stdout=log, stderr=log, preexec_fn=os.setsid)
        self.proc.append(p)

    def create_docker_networks(self, nodes):
        for node in range(1,nodes+1):
            prefix = self._node_ip_prefix(node)
            os.system('docker network create -d bridge node'+str(node)+'-network --subnet='+prefix+'.0/24 --gateway='+prefix+'.1')

    def _node_ip_prefix(node):
        IP1=172
        IP2=20
        IP3=0
        IP3=IP3+node-1
        while IP3 > 255:
            IP3=IP3-256
            IP2=IP2+1
        while IP2 > 255:
            IP2=IP2-256
            IP1=IP1+1

        ip=str(IP1)+"."+str(IP2)+"."+str(IP3)
        return ip


def start_nodes(network: Network, config_path, params):
    n = params['nodes']
    udpBasePort = params['udpBasePort']
    rpcBasePort = params['rpcBasePort']

    print("Starting all nodes..")

    if isinstance(network, NetworkDocker):
        network.create_docker_networks(n)
        os.system("sudo iptables --flush DOCKER-ISOLATION-STAGE-1")

    # construct bootstrap node list
    first_node = network.node_enr(config_path, 1)
    bootnodes_list = [ first_node ]
    for node in random.sample(range(2, n+1), min(n//3, 20)):
        bootnodes_list.append(network.node_enr(config_path, node))

    bootnode_arg = ','.join(bootnodes_list)
    print("Using", len(bootnodes_list), "bootstrap nodes")

    for i in range(1,n+1):
        #print("starting node "+str(i))
        keyfile=config_path+"keys/node-"+str(i)+".key"
        logfile=config_path+"logs/node-"+str(i)+".log"

        nodekey=open(keyfile,"r").read()
        log = open(logfile, 'a')
        network.start_node(i, bootnodes=bootnodes_list, log=log, nodekey=nodekey, config_path=config_path)

    print("Nodes started")


def build():
    result = os.system("go build ./cmd/devp2p")
    assert(result == 0)

# make_keys creates all node keys.
def make_keys(config_path, n):
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

def write_experiment(config_path, params):
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

def run_testbed(network: Network, config_path, params):
    build()
    make_keys(config_path, params['nodes'])
    write_experiment(config_path, params)
    start_nodes(network, config_path, params)
    time.sleep(10)
