import json
import random
import time
import hashlib
import traceback
import os.path
import io
import functools

import asyncio
import aiohttp

import numpy as np

from python.network import Network, NetworkLocal

ZIPF_EXPONENT = 1.0
REQUEST_CONCURRENCY = 128
SEARCH_CONCURRENCY = 128

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


OP_ID = 100
def gen_op_id():
    global OP_ID
    OP_ID += 1
    return OP_ID

# get current time in milliseconds
def get_current_time_msec():
    return round(time.time() * 1000)

def get_topic_digest(topicStr):
    topic_digest = hashlib.sha256(topicStr.encode('utf-8')).hexdigest()
    # json cannot unmarshal hex string without 0x so adding below
    topic_digest = '0x' + topic_digest
    return topic_digest

def open_logs(out_dir):
    return open(os.path.join(out_dir, "logs", "logs.json"), 'a')

def rpc(method, *args):
    return {"method": method, "params": args, "jsonrpc": "2.0", "id": 1}


# Workload performs the request.
class Workload:
    config: dict
    network: Network
    out_dir: str

    zipf: TruncatedZipfDist
    client: aiohttp.ClientSession
    logs_file: io.IOBase

    def __init__(self, network, config, out_dir):
        self.config = config
        self.network = network
        self.out_dir = out_dir
        self.zipf = TruncatedZipfDist(ZIPF_EXPONENT, config['topic'])
        self.logs_file = open_logs(self.out_dir)

        conn = aiohttp.TCPConnector(limit=REQUEST_CONCURRENCY)
        self.client = aiohttp.ClientSession(raise_for_status=True, connector=conn)

    # close terminates the workload.
    async def close(self):
        await self.client.close()
        self.client = None
        self.logs_file.close()
        self.logs_file = None

    # for 'async with'
    async def __aenter__(self):
        return self
    async def __aexit__(self, exc_type, exc, tb):
        await self.close()

    # _post_json sends a request to a node.
    async def _post_json(self, node: int, payload: dict, show_errors=True, retries=1, timeout=5):
        last_error = None
        for attempt in range(0, retries):
            if attempt > 1:
                # delay before retrying
                await asyncio.sleep(random.uniform(0.5, 5))

            url = self.network.node_api_url(node)
            timeout_cfg = aiohttp.ClientTimeout(total=timeout)
            try:
                async with self.client.post(url, json=payload, timeout=timeout_cfg) as req:
                    return await req.json()
            except aiohttp.ClientConnectionError as e:
                last_error e
            except aiohttp.ClientError as e:
                last_error = e
                break # no retry

        if show_errors:
            print('Node {} HTTP error (retries={}): {}'.format(node, retries, str(last_error)))
        return None

    # _write_event appends an event to logs.json.
    def _write_event(self, obj):
        json.dump(obj, self.logs_file)
        self.logs_file.write('\n')
        self.logs_file.flush()

    # waits for all nodes to have a sufficient number of neighbors in the routing table
    async def wait_for_nodes_ready(self):
        min_neighbors = min(self.config['nodes']-1, 10)
        nodes = list(range(1, self.config['nodes'] + 1))

        while True:
            requests = [ self._get_neighbor_count(node) for node in nodes ]
            counts = await asyncio.gather(*requests)

            # check results
            notup = 0
            bootstrapped = 0
            stats = {}
            for (node, count) in counts:
                if count == -1:
                    notup += 1
                else:
                    stats[count] = stats.get(count, 0) + 1
                    if count >= min_neighbors:
                        bootstrapped += 1

            if bootstrapped == len(nodes):
                return
            if notup > 0:
                print('{} nodes are not up yet'.format(notup))
                continue

            sstats = {'ok': 0}
            for k in sorted(stats, reverse=True):
                if k > min_neighbors:
                    sstats['ok'] += stats[k]
                else:
                    sstats[k] = stats[k]
            print('waiting for {} nodes to become ready.'.format(len(nodes)))
            print('stats: {}'.format(sstats))

            # wait for a bit before retrying
            await asyncio.sleep(1)

    async def _get_neighbor_count(self, node: int):
        payload = rpc("discv5_nodeTable")
        resp = await self._post_json(node, payload, show_errors=False)
        if resp is None:
            return (node, -1)
        return (node, len(resp['result']))

    # register_topic performs topic registrations
    async def register_topics(self):
        # send a registration at exponentially distributed
        # times with average inter departure time of 1/rate
        nodes = list(range(1, self.config['nodes'] + 1))
        request_rate = self.config['nodes'] / self.config['adLifetimeSeconds']
        node_topic = {}

        tasks = []
        while len(nodes) > 0:
            await asyncio.sleep(random.expovariate(request_rate))

            node = random.choice(nodes)
            nodes.remove(node)
            topic = "t" + str(self.zipf.rv() + 1)
            node_topic[node] = topic

            taskname = 'register-node-' + str(node)
            t = asyncio.create_task(self._register_request(node, topic), name=taskname)
            tasks.append(t)

        # wait for pending requests to finish.
        await asyncio.wait(tasks)
        return node_topic

    async def _register_request(self, node: int, topic: str):
        op_id = gen_op_id()
        topic_digest = get_topic_digest(topic)
        print('Node:', node, 'is registering topic:', topic, 'with hash:', topic_digest)
        payload = rpc("discv5_registerTopic", topic_digest, op_id)
        payload["opid"] = op_id
        payload["time"] = get_current_time_msec()
        self._write_event(payload)

        resp = await self._post_json(node, payload, retries=25)
        if resp is not None:
            resp["opid"] = op_id
            resp["time"] = get_current_time_msec()
            self._write_event(resp)

    # search_topics runs searches
    async def search_topics(self, node_to_topic: dict):
        total_requests = self.config['nodes'] * self.config['searchIterations']
        nodes = set(range(1, self.config['nodes'] + 1))
        searches = { node: 0 for node in nodes }
        tasks = set()

        def search_done(node: int, task: asyncio.Task):
            tasks.discard(task)
            searches[node] += 1
            if searches[node] < self.config['searchIterations']:
                nodes.add(node)

        i = 0
        while len(nodes) > 0 or len(tasks) > 0:
            # when all nodes are busy, we need to wait for the next task to finish
            if len(nodes) == 0 or len(tasks) >= SEARCH_CONCURRENCY:
                await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)

            if len(nodes) > 0:
                i += 1

                # pick a random node
                node = random.choice(list(nodes))
                topic = node_to_topic[node]
                nodes.discard(node)

                # launch the search. when it's done, node is added back into nodes by search_done.
                print('[{}/{}] Node {} is searching for topic: {}'.format(i, total_requests, node, topic))
                taskname = 'search-node-' + str(node)
                t = asyncio.create_task(self._search_request(node, topic))
                t.add_done_callback(functools.partial(search_done, node))
                tasks.add(t)

    async def _search_request(self, node: int, topic: str):
        want_num_results = self.config['returnedNodes']
        op_id = gen_op_id()
        topic_digest = get_topic_digest(topic)

        payload = rpc("discv5_topicSearch", topic_digest, want_num_results, op_id)
        payload["opid"] = op_id
        payload["time"] = get_current_time_msec()
        self._write_event(payload)

        timeout = self.config.get('rpcSearchTimeoutSeconds', 120) + 10
        resp = await self._post_json(node, payload, retries=25, timeout=timeout)
        if resp is not None:
            resp["opid"] = op_id
            resp["time"] = get_current_time_msec()
            result = resp.get("result") or []
            d = resp['time'] - payload['time']
            print('Search response: node', node, 'found', len(result), 'nodes in', str(d)+'ms')
            self._write_event(resp)


async def _run_workload(network: Network, params: dict, out_dir):
    async with Workload(network, params, out_dir) as w:
        await w.wait_for_nodes_ready()

        print("Starting registrations...")
        node_to_topic = await w.register_topics()

        # wait for registrations to complete
        print('Registrations completed, waiting...')
        await asyncio.sleep(params['adLifetimeSeconds'])

        # search
        print("Searching...")
        await w.search_topics(node_to_topic)


def run_workload(network: Network, params, out_dir):
    asyncio.run(_run_workload(network, params, out_dir))
