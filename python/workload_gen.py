import requests
import json
import random
import sys
import time
import re
import hashlib
import traceback

import os.path

import math
import collections

import numpy as np
import scipy.stats as ss

import json
import time
import queue
from python.network import *

from concurrent.futures import ThreadPoolExecutor, as_completed

def_url_prefix = 'http://localhost'
request_rate = 10  # request rate per second
num_topics = 1
zipf_exponent = 1.0

NODE_ID = 0
OP_ID = 100
LOGS = queue.Queue()
PROCESSES = []
MAX_REQUEST_THREADS = 128

def gen_op_id():
    global OP_ID
    OP_ID += 1
    return OP_ID

def gen_node_id():
    global NODE_ID
    NODE_ID += 1
    return NODE_ID

# Write json logs to a file
def write_logs_to_file(fname):
    global LOGS
    LOGS.put(None)
    print("LOGS: ", LOGS)
    with open(fname, 'w+') as f:
        for data in iter(LOGS.get, None):
            print("Data: ", data)
            json.dump(data, f)
            f.write("\n")

# get current time in milliseconds
def get_current_time_msec():
    return round(time.time() * 1000)

def get_topic_digest(topicStr):
    topic_digest = hashlib.sha256(topicStr.encode('utf-8')).hexdigest()
    # json cannot unmarshal hex string without 0x so adding below
    topic_digest = '0x' + topic_digest
    return topic_digest

def send_register(node, topic, config, op_id):
    global LOGS
    topic_digest = get_topic_digest(topic)
    payload = {
        "method": "discv5_registerTopic",
        "params": [topic_digest, op_id],
        "jsonrpc": "2.0",
        "id": op_id,
    }
    payload["opid"] = op_id
    payload["time"] = get_current_time_msec()
    LOGS.put(payload)

    print('Node:', node, 'is registering topic:', topic, 'with hash:', topic_digest)
    resp = requests.post(node_api_url(node, config), json=payload).json()
    resp["opid"] = op_id
    resp["time"] = get_current_time_msec()
    LOGS.put(resp)

def send_register_docker(node, topic, config, op_id):
    global LOGS
    topic_digest = get_topic_digest(topic)
    payload = {
        "method": "discv5_registerTopic",
        "params": [topic_digest, op_id],
        "jsonrpc": "2.0",
        "id": op_id,
    }
    payload["opid"] = op_id
    payload["time"] = get_current_time_msec()
    LOGS.put(payload)

    print('Node:', node, 'is registering topic:', topic, 'with hash:', topic_digest)
    resp = requests.post(node_api_url_docker(node, config), json=payload).json()
    resp["opid"] = op_id
    resp["time"] = get_current_time_msec()
    LOGS.put(resp)


# Following is used to generate random numbers following a zipf distribution
# Copied below from icarus simulator
class DiscreteDist(object):
    """Implements a discrete distribution with finite population.

    The support must be a finite discrete set of contiguous integers
    {1, ..., N}. This definition of discrete distribution.
    """

    def __init__(self, pdf, seed=None):
        """
        Constructor

        Parameters
        ----------
        pdf : array-like
            The probability density function
        seed : any hashable type (optional)
            The seed to be used for random number generation
        """
        if np.abs(sum(pdf) - 1.0) > 0.001:
            raise ValueError('The sum of pdf values must be equal to 1')
        random.seed(seed)
        self._pdf = np.asarray(pdf)
        self._cdf = np.cumsum(self._pdf)
        # set last element of the CDF to 1.0 to avoid rounding errors
        self._cdf[-1] = 1.0

    def __len__(self):
        """Return the cardinality of the support

        Returns
        -------
        len : int
            The cardinality of the support
        """
        return len(self._pdf)

    @property
    def pdf(self):
        """
        Return the Probability Density Function (PDF)

        Returns
        -------
        pdf : Numpy array
            Array representing the probability density function of the
            distribution
        """
        return self._pdf

    @property
    def cdf(self):
        """
        Return the Cumulative Density Function (CDF)

        Returns
        -------
        cdf : Numpy array
            Array representing cdf
        """
        return self._cdf

    def rv(self):
        """Get rand value from the distribution
        """
        rv = random.random()
        # This operation performs binary search over the CDF to return the
        # random value. Worst case time complexity is O(log2(n))
        return int(np.searchsorted(self._cdf, rv))

class TruncatedZipfDist(DiscreteDist):
    """Implements a truncated Zipf distribution, i.e. a Zipf distribution with
    a finite population, which can hence take values of alpha > 0.
    """

    def __init__(self, alpha=1.0, n=1000, seed=None):
        """Constructor

        Parameters
        ----------
        alpha : float
            The value of the alpha parameter (it must be positive)
        n : int
            The size of population
        seed : any hashable type, optional
            The seed to be used for random number generation
        """
        # Validate parameters
        if alpha <= 0:
            raise ValueError('alpha must be positive')
        if n < 0:
            raise ValueError('n must be positive')
        # This is the PDF i. e. the array that  contains the probability that
        # content i + 1 is picked
        pdf = np.arange(1.0, n + 1.0) ** -alpha
        pdf /= np.sum(pdf)
        self._alpha = alpha
        super(TruncatedZipfDist, self).__init__(pdf, seed)

    @property
    def alpha(self):
        return self._alpha


# waits for all nodes to have a sufficient number of neighbors in the routing table
def wait_for_nodes_ready(config, count):
    min_neighbors = min(config['nodes']-1, 10)
    nodes = list(range(1, config['nodes'] + 1))
    global MAX_REQUEST_THREADS
    with ThreadPoolExecutor(max_workers=MAX_REQUEST_THREADS) as executor:
        while len(nodes) > 0:
            print('waiting for {} nodes to become ready'.format(len(nodes)))
            # submit check requests
            proc = []
            for node in nodes:
                p = executor.submit(count, node, config)
                proc.append((node, p))
            # check results
            for (node, p) in proc:
                try:
                    count = p.result()
                except requests.RequestException as e:
                    print('node {} is not up yet'.format(node))
                else:
                    #print(count)
                    if count >= min_neighbors:
                        nodes.remove(node)
            # wait for a bit before retrying
            time.sleep(1)


# checks if the routing table of a node is sufficiently filled
def node_neighbor_count(node, config):
    payload = {"method": "discv5_nodeTable", "params": [], "jsonrpc": "2.0", "id": 1}
    resp = requests.post(node_api_url(node, config), json=payload).json()
    return len(resp['result'])

def node_neighbor_count_docker(node,config):
    payload = {"method": "discv5_nodeTable", "params": [], "jsonrpc": "2.0", "id": 1}
    resp = requests.post(node_api_url_docker(node, config), json=payload).json()
    return len(resp['result'])

# perform topic registrations
def register_topics(zipf, config, docker):
    node_topic = {}
    time_now = float(time.time())
    nodes = list(range(1, config['nodes'] + 1))
   # print("Nodes:"+str(config['nodes'])+" lifetime:"+str(config['adLifetimeSeconds']))
    request_rate = config['nodes'] / config['adLifetimeSeconds']
    #print(request_rate)
    time_next = time_now + random.expovariate(request_rate)

    # send a registration at exponentially distributed
    # times with average inter departure time of 1/rate

    global MAX_REQUEST_THREADS
    with ThreadPoolExecutor(max_workers=MAX_REQUEST_THREADS) as executor:
        while (len(nodes) > 0):
            time_now = float(time.time())
            if time_next > time_now:
                time.sleep(time_next - time_now)
            else:
                node = random.choice(nodes)
                nodes.remove(node)
                topic = "t" + str(zipf.rv() + 1)
                node_topic[node] = topic
                #send_register(node, topic, config)
                if docker:
                    PROCESSES.append(executor.submit(send_register_docker,node, topic, config, gen_op_id()))
                else:
                    PROCESSES.append(executor.submit(send_register,node, topic, config, gen_op_id()))
                time_next = time_now + random.expovariate(request_rate)

    return node_topic


def search_topics(zipf, config, node_to_topic,docker):
    nodes = list(range(1, config['nodes'] + 1))
    node = random.choice(nodes)
    nodes = list(range(1, config['nodes'] + 1))
          
    time_now = float(time.time())

    request_rate = config['nodes'] / config['adLifetimeSeconds']
    #print(request_rate)
    time_next = time_now + random.expovariate(request_rate)

    #global MAX_REQUEST_THREADS
    #with ThreadPoolExecutor(max_workers=MAX_REQUEST_THREADS) as executor:
    #    for node in nodes:
    #        topic = node_to_topic[node]
    #        PROCESSES.append(executor.submit(send_lookup,node, topic, config, gen_op_id()))
    with ThreadPoolExecutor(max_workers=MAX_REQUEST_THREADS) as executor:
        while (len(nodes) > 0):
            time_now = float(time.time())
            if time_next > time_now:
                time.sleep(time_next - time_now)
            else:
                node = random.choice(nodes)
                nodes.remove(node)
                topic = node_to_topic[node]
                #send_register(node, topic, config)
                if docker:
                    PROCESSES.append(executor.submit(send_lookup_docker,node, topic, config, gen_op_id()))
                else:
                    PROCESSES.append(executor.submit(send_lookup,node, topic, config, gen_op_id()))
                time_next = time_now + random.expovariate(request_rate)

def send_lookup(node, topic, config, op_id):
    global LOGS
    topic_digest = get_topic_digest(topic)
    want_num_results = config['returnedNodes']
    payload = {
        "method": "discv5_topicSearch",
        "params": [topic_digest, want_num_results, op_id],
        "jsonrpc": "2.0",
        "id": op_id,
    }
    payload["opid"] = op_id
    payload["time"] = get_current_time_msec()
    LOGS.put(payload)

    print('Node {} is searching for {} nodes in topic: {}'.format(node, want_num_results, topic))
    resp = requests.post(node_api_url(node, config), json=payload).json()
    resp["opid"] = op_id
    resp["time"] = get_current_time_msec()
    print('Search response: ', resp)
    LOGS.put(resp)

def send_lookup_docker(node, topic, config, op_id):
    global LOGS
    topic_digest = get_topic_digest(topic)
    want_num_results = config['returnedNodes']
    payload = {
        "method": "discv5_topicSearch",
        "params": [topic_digest, want_num_results, op_id],
        "jsonrpc": "2.0",
        "id": op_id,
    }
    payload["opid"] = op_id
    payload["time"] = get_current_time_msec()
    LOGS.put(payload)

    print('Node {} is searching for {} nodes in topic: {}'.format(node, want_num_results, topic))
    resp = requests.post(node_api_url_docker(node, config), json=payload).json()
    resp["opid"] = op_id
    resp["time"] = get_current_time_msec()
    print('Search response: ', resp)
    LOGS.put(resp)

def node_api_url(node, config):
    port = config['rpcBasePort'] + node
    #print("port:"+str(port))
    url = def_url_prefix + ":" + str(port)
    return url

def node_api_url_docker(node, config):
    port = config['rpcBasePort']
    url = "http://" +get_network(node)+".2:" + str(port)
    return url

def read_config(dir):
    file = os.path.join(dir, 'experiment.json')
    print('Reading experiment settings from file:', file)
    config = {}
    with open(file) as f:
        config = json.load(f)
    assert isinstance(config.get('nodes'), int)
    assert isinstance(config.get('rpcBasePort'), int)
    return config


def main():
    # Read experiment parameters.
    directory = "discv5-test"
    if len(sys.argv) > 1:
        directory = sys.argv[1]
    config = read_config(directory)

    wait_for_nodes_ready(config,node_neighbor_count)

    # register
    zipf = TruncatedZipfDist(zipf_exponent, num_topics)
    node_to_topic = register_topics(zipf, config)

    # wait for registrations to complete
    time.sleep(10)

    # search
    search_topics(zipf, config, node_to_topic)
    for future in PROCESSES:
        try:
            result = future.result()
        except Exception:
            traceback.print_exc()
            print('Unable to get the result')

    write_logs_to_file(os.path.join(directory, "logs", "logs.json"))


if __name__ == "__main__":
    main()
