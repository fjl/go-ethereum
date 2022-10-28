import json
import os
import os.path
import random
import subprocess
import time
import glob


# This is the list of config parameters supported by cmd/devp2p.
run_param = {
    'adCacheSize',
    'adLifetimeSeconds',
    'regBucketSize',
    'regBucketStandbySize',
    'searchBucketSize',
    'bucketRefreshInterval',
    'rpcSearchTimeoutSeconds',
}


network_config_defaults = {
    'rpcBasePort': 20200,
    'udpBasePort': 30200,
}

MIN_LATENCY=2
MAX_LATENCY=40

class Network:
    config = {}

    def __init__(self, config=network_config_defaults):
        assert isinstance(config, dict)
        assert isinstance(config.get('rpcBasePort'), int)
        assert isinstance(config.get('udpBasePort'), int)
        self.config = config

    def build(self):
        print('Compiling devp2p tool')
        result = os.system("go build ./cmd/devp2p")
        assert(result == 0)

    def stop(self):
        pass


class NetworkLocal(Network):
    proc = []

    node_env = os.environ.copy()
    node_env['GOMAXPROCS'] = '1'

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

    def start_node(self, n: int, bootnodes=[], nodekey=None, config_path=None):
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
        logfile = os.path.join(config_path, "logs", "node-"+str(n)+".log")
        logflags = ["--verbosity", "5", "--log.json", "--log.file", logfile]
        argv = ["./devp2p", *logflags, "discv5", "listen", *nodeflags]

        print("Starting node", str(n))
        p = subprocess.Popen(argv, stdout=subprocess.DEVNULL, stderr=None, env=self.node_env)
        self.proc.append(p)

    def stop(self):
        if self.proc: print('Stopping network')
        for p in self.proc:
            p.terminate()
        for p in self.proc:
            p.wait()
        self.proc = []


class NetworkDocker(Network):
    containers = []
    networks = []

    def build(self):
        super().build()

        # remove stuff from previous runs
        #os.system('docker network prune -f')
        #os.system('docker container prune -f')

        # print("Building docker container")
        #result = os.system("docker build --tag devp2p -f Dockerfile.topdisc .")
        #assert(result == 0)

    def stop(self):
        super().stop()

        print("Stopping docker containers")
        for container_id in self.containers:
            os.system('docker kill ' + container_id)
            os.system('docker rm ' + container_id)
        self.containers = []

        for network_id in self.networks:
            os.system('docker network rm ' + network_id)
        self.networks = []

    # get_node_url returns the ENR of node n.
    def node_enr(self, path, n):
        ip = self._node_ip(n)
        port = self.config['udpBasePort']
        url = os.popen("./devp2p key to-enr --ip " + ip + " --tcp 0 --udp "+str(port)+" "+path+"keys/node-"+str(1)+".key").read().split('\n')
        return url[0]

    # node_api_url returns the RPC URL of node n.
    def node_api_url(self, n):
        port = self.config['rpcBasePort']
        ip = self._node_ip(n)
        return "http://" + ip + ":" + str(port)

    def start_node(self, n: int, bootnodes=[], nodekey=None, config_path=None):
        assert nodekey is not None
        assert config_path is not None

        # create node command line
        port = self.config['udpBasePort']
        rpc = self.config['rpcBasePort']
        nodeflags = [
            "--bootnodes", ','.join(bootnodes),
            "--nodekey", nodekey,
            "--addr", self._node_ip(n)+':'+str(port),
            "--rpc", self._node_ip(n)+':'+str(rpc),
            "--config", "/go-ethereum/discv5-test/config.json",
        ]
        logfile = "/go-ethereum/discv5-test/logs/node-"+str(n)+".log"
        logflags = ["--verbosity", "5", "--log.json", "--log.file", logfile]
        node_args = [*logflags, "discv5", "listen", *nodeflags]

        # start the docker container
        argv = [
            "docker", "run", "-d",
            "--name", "node"+str(n),
            "--network", self._node_network_name(n),
            "--cap-add", "NET_ADMIN",
            "--mount", "type=bind,source="+config_path+",target=/go-ethereum/discv5-test",
            "devp2p", *node_args,
        ]
        p = subprocess.run(argv, capture_output=True, text=True)
        if p.returncode != 0:
            print(p.stderr)
            p.check_returncode()

        container_id = p.stdout.split('\n')[0]
        print('started node', n, 'container:', container_id)
        self.containers.append(container_id)
        #self._config_network(n)

    def create_docker_networks(self, nodes):
        for node in range(1,nodes+1):
            network = self._node_network_name(node)
            prefix = self._node_ip_prefix(node)
            subnet = prefix + '.0/24'
            gateway = prefix+'.1'
            argv = [
                'docker', 'network', 'create', network,
                '-d', 'bridge',
                '--subnet=' + subnet,
                '--gateway=' + gateway,
            ]
            p = subprocess.run(argv, capture_output=True, text=True)
            if p.returncode != 0:
                print('docker network create failed:')
                print(p.stderr)
            else:
                self.networks.append(p.stdout.split('\n')[0])

    def _node_network_name(self, node):
        return 'node' + str(node) + '-network'

    def _node_ip(self, node):
        return self._node_ip_prefix(node) + '.2'

    def _node_ip_prefix(self, node):
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

    def config_network(self, node):
        latency = random.randint(MIN_LATENCY,MAX_LATENCY)
        subprocess.Popen("docker exec node"+str(node)+" sh -c 'tc qdisc add dev eth0 root netem delay "+str(latency)+"ms'", stdout=subprocess.DEVNULL, stderr=None,shell=True)

        #command = 'sh -c "tc qdisc add dev eth0 root netem delay '+str(latency)+'ms"'
        #argv = ["docker","exec","node"+str(node),command]
        #p = subprocess.run(argv, capture_output=True, text=True)
        #if p.returncode != 0:
        #    print(p.stderr)
        #    p.check_returncode()


def nodekey_to_id(keyfile):
    argv = ["./devp2p", "key", "to-id", keyfile]
    p = subprocess.run(argv, capture_output=True, text=True)
    p.check_returncode()
    return p.stdout.split('\n')[0]

# create_nodeid_index writes a node_id -> node index file in the logs directory.
def create_nodeid_index(config_path):
    keys_dir = os.path.join(config_path, "keys")
    if not os.path.isdir(keys_dir):
        raise FileNotFoundError("keys/ directory does not exist: " + keys_dir)

    print("Creating node ID index...")
    index = {}
    for key_file in glob.glob(os.path.join(keys_dir, "node-*.key")):
        node = int(os.path.basename(key_file).split('-')[1].split('.')[0])
        node_id = nodekey_to_id(key_file)
        index[node_id] = node

    index_file = os.path.join(keys_dir, "node_id_index.json")
    with open(index_file, 'w') as f:
        json.dump(index, f)
        f.write("\n")

    return index

def load_nodeid_index(config_path):
    keys_dir = os.path.join(config_path, "keys")
    index_file = os.path.join(keys_dir, "node_id_index.json")

    if not os.path.isfile(index_file):
        return create_nodeid_index(config_path)

    with open(index_file, 'r') as f:
        return json.load(f)


def create_enrs(network: Network, config_path, n: int):
    return [ network.node_enr(config_path, node) for node in range(1, n+1) ]

def select_bootnodes(enrs):
    return [ enrs[0] ] + random.sample(enrs[1:], min(len(enrs)//3, 20))

def start_nodes(network: Network, config_path: str, params: dict):
    n = params['nodes']

    print("Creating ENRs...")
    enrs = create_enrs(network, config_path, n)
    create_nodeid_index(config_path)

    print("Starting", n, "nodes...")

    if isinstance(network, NetworkDocker):
        network.create_docker_networks(n)
        os.system("sudo iptables --flush DOCKER-ISOLATION-STAGE-1")

    for i in range(1,n+1):
        keyfile = os.path.join(config_path, "keys", "node-"+str(i)+".key")
        with open(keyfile, "r") as f:
            nodekey = f.read()
        bn = select_bootnodes(enrs)
        network.start_node(i, bootnodes=bn, nodekey=nodekey, config_path=config_path)

        if isinstance(network, NetworkDocker):
            network.config_network(i)
    print("Nodes started")

# make_keys creates all node keys.
def make_keys(config_path, n):
    print("Building keys...")
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
    logs_dir = os.path.join(config_path, "logs")
    if os.path.exists(logs_dir):
        print("Removing old logs...")
        for filename in os.listdir(logs_dir):
            os.remove(os.path.join(logs_dir, filename))
    else:
        os.mkdir(logs_dir)

    node_config = filter_params(params)
    print("Experiment parameters:", params)
    # print("Node config:", node_config)
    with open(config_path+'config.json', 'w') as f:
        f.write(json.dumps(node_config))
    with open(config_path+'experiment.json', 'w') as f:
        f.write(json.dumps(params))

def run_testbed(network: Network, config_path, params):
    network.build()
    make_keys(config_path, params['nodes'])
    write_experiment(config_path, params)
    start_nodes(network, config_path, params)
