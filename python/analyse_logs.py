import json
import os
import os.path
import sys
from dateutil.parser import parse
import pandas as pd
import hashlib
import numpy
import matplotlib.pyplot as plt
from matplotlib.lines import Line2D
import seaborn as sns
import heapq # to sort register removal events
import time
import concurrent.futures

#log_path = "./discv5-test/logs"
#log_path = "./discv5_test_logs/benign/_nodes-100_topic-1_regBucketSize-10_searchBucketSize-3_adLifetimeSeconds-60_adCacheSize-500_rpcBasePort-20200_udpBasePort-30200_returnedNodes-5/logs/"
form = 'pdf'
log_path = "../discv5-test/logs"

def get_storage_and_advertisement_dist_df(log_path):
    topic_mapping = {} #reverse engineer the topic hash
    for i in range(1, 1000):
        topic_mapping[hashlib.sha256(('t'+str(i)).encode('utf-8')).hexdigest()] = i
    ad_expiry_heap = []
    reg_events_heap = []
    nodes = set()

    # Read all the registration events and add them to a heap
    for log_file in os.listdir(log_path):
        if (not log_file.startswith("node-")):
            continue
        print("Reading", log_file)
        registrar = int(log_file.split('-')[1].split('.')[0]) #node-10.log
        nodes.add(registrar)

        for line in open(log_path + '/' + log_file, 'r').readlines():
            if(line[0] != '{'):
                #not a json line
                continue
            jsons = json.loads(line)

            if('adlifetime' not in jsons):
                continue
            msg_type = jsons['msg'].split(' ')[1]
            if msg_type != 'REGCONFIRMATION/v5':
                continue
            in_out_s = jsons['msg'].split(' ')[0]
            if(in_out_s != '>>'):
                continue
            ok = jsons['ok']
            if (ok != 'true'):
                continue

            adlifetime = int(jsons['adlifetime']) / 1000 # get in seconds
            advertiser = int(jsons['addr'].split(':')[1]) - 30200

            if(registrar == advertiser):
                print('This should not happen - node is sending itself a REGCONFIRMATION: ', line)
            # parse time
            dt = parse(jsons['t'])
            unix_time = int(time.mktime(dt.timetuple()))

            heapq.heappush(reg_events_heap, (unix_time, registrar, adlifetime, advertiser))


    table_size = {} # {node : node's table size}
    table_size_ot = {} # {timestamp : { node : node's table size} }
    init_time = None
    num_adverts = {} # {node : number of active adverts by the node}
    num_adverts_ot = {} # {timestamp : {node : number of adverts by the node}}

    # Read the registration events in order
    while len(reg_events_heap) > 0:
        timestamp, registrar, adlifetime, advertiser = heapq.heappop(reg_events_heap)

        # the first event will have timestamp of 0
        if init_time is None:
            init_time = timestamp
            timestamp = 0
        else:
            timestamp -= init_time

        #print("Registration event at Registrar: ", registrar, "at time:", timestamp, "by advertiser:", advertiser)

        while len(ad_expiry_heap) > 0:
            tupl = ad_expiry_heap[0]
            expiry_time = tupl[0]

            if(expiry_time is not None and expiry_time <= timestamp):
                expiry_time, expRegistrar, expAdvertiser = heapq.heappop(ad_expiry_heap)
                table_size[expRegistrar] = table_size[expRegistrar] - 1
                num_adverts[expAdvertiser] = num_adverts[expAdvertiser] - 1
                if expiry_time not in table_size_ot.keys():
                    table_size_ot[expiry_time] = {}
                if expiry_time not in num_adverts_ot.keys():
                    num_adverts_ot[expiry_time] = {}

                table_size_ot[expiry_time][expRegistrar] = table_size[expRegistrar]
                num_adverts_ot[expiry_time][expAdvertiser] = num_adverts[expAdvertiser]
                #print('Ad expiration  at registrar: ', node, 'at time:', expiry_time, 'setting table size to', table_size[node])
            else:
                break

        if registrar not in table_size.keys():
            table_size[registrar] = 1
        else:
            table_size[registrar] = table_size[registrar] + 1

        if advertiser not in num_adverts.keys():
            num_adverts[advertiser] = 1
        else:
            num_adverts[advertiser] = num_adverts[advertiser] + 1

        if timestamp not in table_size_ot.keys():
            table_size_ot[timestamp] = {}
        table_size_ot[timestamp][registrar] = table_size[registrar]

        if timestamp not in num_adverts_ot.keys():
            num_adverts_ot[timestamp] = {}
        num_adverts_ot[timestamp][advertiser] = num_adverts[advertiser]

        #print('Timestamp:', timestamp, 'registrar:', registrar, 'size:', table_size[registrar], 'adlifetime:', adlifetime)
        heapq.heappush(ad_expiry_heap, (timestamp + adlifetime, registrar, advertiser))

    rows_storage = []
    rows_ad_dist = []
    times = list(table_size_ot.keys())
    times = sorted(times)
    #print('table_size_ot: ', table_size_ot)
    table_size = {}
    for node in list(nodes):
        table_size[node] = 0
        num_adverts[node] = 0
    #print('nodes:', list(nodes))

    for timestamp in times:
        #print ('Timestamp:', timestamp)
        reg_events = table_size_ot[timestamp]
        for registrar in reg_events.keys():
            table_size[registrar] = reg_events[registrar]
            #print('setting registar', registrar, 'table size to', reg_events[registrar])
        for node in list(nodes):
            row = {}
            row['timestamp'] = timestamp
            row['registrar'] = node
            row['num_ads_stored'] = table_size[node]
            #print('Setting row node', str(node), 'to', table_size[node])

            rows_storage.append(row)

    storage_df = pd.DataFrame(rows_storage)

    for timestamp in times:
        #print ('Timestamp:', timestamp)
        advert_events = num_adverts_ot[timestamp]
        for advertiser in advert_events.keys():
            num_adverts[advertiser] = advert_events[advertiser]
            #print('setting registar', registrar, 'table size to', reg_events[registrar])
        for node in list(nodes):
            row = {}
            row['timestamp'] = timestamp
            row['advertiser'] = node
            row['num_ads_registered'] = num_adverts[node]
            #print('Setting row node', str(node), 'to', table_size[node])

            rows_ad_dist.append(row)

    ad_dist_df = pd.DataFrame(rows_ad_dist)

    return storage_df, ad_dist_df


def get_msg_df(log_path, op_df):
    topic_mapping = {} #reverse engineer the topic hash
    for i in range(1, 100):
        topic_mapping[hashlib.sha256(('t'+str(i)).encode('utf-8')).hexdigest()] = i

    op_info = {}
    for opid in set([i for i in op_df['opid']]):
        op_type = op_df.loc[op_df['opid'] == opid, 'method'].values[0]
        topic = op_df.loc[op_df['opid'] == opid, 'topic'].values[0]
        op_info[opid] = {'op_type':op_type, 'topic':topic}

    rows = []
    with concurrent.futures.ProcessPoolExecutor() as executor:
        # launch parser threads
        rows_f = []
        for log_file in os.listdir(log_path):
            if (not log_file.startswith("node-")):
                continue
            print("Reading", log_file)
            fname = os.path.join(log_path, log_file)
            rows_f.append(executor.submit(parse_msg_logs, fname, topic_mapping, op_info))

        # concatenate parsing results
        for f in concurrent.futures.as_completed(rows_f):
            rows += f.result()

    rows.sort(key=lambda row: row['timestamp'])
    assign_missing_op_info(rows, op_info)

    print('Constructing the dataframe')
    msg_df = pd.DataFrame(rows)
    msg_df.dropna(subset=['opid'], inplace=True)
    return msg_df

# this function adds op_id, topic, op_type based on req_id.
def assign_missing_op_info(rows: list, op_info: dict):
    print('Propagating message op_ids')
    mapping = {} # req_id -> opid
    for row in rows:
        if 'opid' in row:
            mapping[row['req_id']] = row['opid']
            continue # fields already set by process_message
        if 'req_id' in row:
            req = row['req_id']
            if req in mapping:
                op = mapping[req]
                row['opid'] = op
                row['topic'] = op_info[op]['topic']
                row['op_type'] = op_info[op]['op_type']
    return rows


def parse_msg_logs(fname: str, topic_mapping: dict, op_info: dict):
    rows = []
    def process_message(node_id, jsons):
         msg = jsons['msg']
         if not msg.startswith('>> '):
             return # it's not a message sent between peers

         row = {
             'node_id': node_id,
             # get peer ID from the port number
             'peer_id': int(jsons['addr'].split(':')[1]) - 30200,
             'timestamp': parse(jsons['t']),
             'msg_type': msg.split(' ')[1].split(':')[0],
         }
         if('req' in jsons):
             row['req_id'] = jsons['req']
         if('total-wtime' in jsons):
             row['total_wtime'] = jsons['total-wtime']
         if('wtime' in jsons):
             row['wtime'] = jsons['wtime']
         if('ok' in jsons):
             row['ok'] = jsons['ok']

         if('opid' in jsons):
             op = jsons['opid']
             row['opid'] = op
             # add other attributes known about this operation
             row['topic'] = op_info[op]['topic']
             row['op_type'] = op_info[op]['op_type']

         # we have a key to the message specified
         # currently it can only be the topic
         if('topic' in jsons):
             # replace topic digest by topic name
             topic = jsons['topic']
             row['key'] = topic_mapping[topic]

         rows.append(row)

    fname_base = os.path.basename(fname) # node-10.log
    node_id = fname_base.split('-')[1].split('.')[0]
    with open(fname, 'r') as f:
        for line in f:
            if line[0] == '{':
                jsons = json.loads(line)
                if 'addr' in jsons:
                    process_message(node_id, jsons)

    return rows


def get_op_df(log_path):
    topic_mapping = {} #reverse engineer the topic hash
    for i in range(1, 100):
        topic_mapping[hashlib.sha256(('t'+str(i)).encode('utf-8')).hexdigest()] = i

    operations = {} #indexed by opid
    for line in open(log_path + '/logs.json', 'r').readlines():
        #not a json line
        if(line[0] != '{'):
            continue
        #print("###line:")
        #print(line)
        row = {}
        jsons = json.loads(line)
        #it's a RPC request
        opid = jsons['opid']
        if('method' in jsons):
            #print("opid:", opid, "req")
            #we can't have 2 operations with the same ID
            assert (opid not in operations)
            row = {}
            row['opid'] = opid
            row['method'] = jsons['method']
            row['reply_received'] = False

            jsons['params'][0] = int(topic_mapping[jsons['params'][0][2:]])#drop the 0x at the begining
            row['params'] = jsons['params']
            row['start_time'] = jsons['time']
        #it's a RPC reply
        else:
            #we shouldn't receive a reply without seeing a request
            assert (opid in operations)
            #print("opid:", opid, "reply")
            row = operations[opid]
            #we should have only one reply per request
            assert(row['reply_received'] == False)
            row['reply_received'] = True
            row['result'] = jsons['result']
            row['end_time'] = jsons['time']
            row['time'] = row['end_time'] - row['start_time']
            assert(row['time'] >= 0)

        #print("~~~row:")
        #print(row)
        operations[opid] = row

    #print(operations)

    op_df = pd.DataFrame(operations.values())
    for i, row in op_df.iterrows():
        op_df.at[i, 'topic'] = row['params'][0]


    return op_df


def plot_operation_returned(fig_dir,op_df):
    ax = op_df['reply_received'].value_counts().plot(kind = 'pie', autopct='%1.0f%%', legend=True, title='Operation returned')
    ax.figure.savefig(fig_dir + 'op_returned.'+form,format=form)


def plot_operation_times(fig_dir,op_df):
    fig, axes = plt.subplots()
    df = op_df[~op_df['time'].isna()]
    sns.violinplot(x='method',y='time', data=df, ax = axes, cut=0)
    fig.savefig(fig_dir + 'operation_time.'+form,format=form)


def plot_msg_operation(fig_dir,msg_df):
    colors = ['red', 'green', 'blue', 'yellow']

    for op_type, group_op_type in msg_df.groupby('op_type'):

        fig, ax = plt.subplots()
        legend_elements = []
        added = set()
        for opid, group_opid in group_op_type.groupby('opid'):
            #print("\t", op_type)
            i = 0
            sum = 0
            for msg_type, group_msg_type in group_opid.groupby('msg_type'):
                val = len(group_msg_type)
                ax.bar(opid, val, color=colors[i], bottom = sum)
                sum += val
            # print("\t\t", msg_type, len(group_msg_type))
                if(msg_type not in added):
                    added.add(msg_type)
                    legend_elements.append(Line2D([0], [0], color=colors[i], lw=4, label=msg_type))
                i += 1
        ax.legend(handles=legend_elements)
        ax.set_title(op_type)

        fig.savefig(fig_dir + op_type+'.'+form,format=form)


def plot_msg_topic(fig_dir,msg_df):
    fig, ax = plt.subplots()
    ax = msg_df['msg_type'].value_counts().plot(kind='bar')
    ax.figure.savefig(fig_dir + 'msg_type_count.'+form,format=form,bbox_inches="tight")


def plot_msg_op_topic(fig_dir,msg_df):
    fig, ax = plt.subplots()

    for op_type, group_op_type in msg_df.groupby('op_type'):
        print(op_type)
        fig, ax = plt.subplots()
        group_op_type['topic'].value_counts().plot(kind='bar', title=op_type)
        ax.set_ylabel("#Messages")
        fig.savefig(fig_dir + op_type+'_msg_per_topic.'+form,format=form)


def plot_times_discovered(fig_dir,op_df):
    op_df_exploded = op_df.copy()
    op_df_exploded = op_df_exploded.explode('result')
    fig, axes = plt.subplots()
    op_df_exploded['result'].value_counts().plot(ax = axes,kind='bar')
    axes.set_xticklabels([])
    axes.set_xlabel("Discovered Nodes")
    axes.set_ylabel("Count")
    axes.set_yticks(list(op_df_exploded['result'].value_counts()))
    fig.savefig(fig_dir + 'times_discovered.'+form,format=form)


def plot_search_results(fig_dir,op_df):
    op_df_exploded = op_df.copy()
    op_df_exploded = op_df_exploded.explode('result')
    op_df_droppedNone = op_df_exploded.dropna(subset=['result'])
    fig, axes = plt.subplots()
    op_df_droppedNone['opid'].value_counts().plot(ax=axes, kind='bar')
    axes.set_xlabel("Topic search operation")
    axes.set_ylabel("Number of results")
    axes.set_yticks(list(op_df_droppedNone['opid'].value_counts()))

    fig.savefig(fig_dir + 'discovered_search.'+form,format=form)


def plot_waiting_time(fig_dir,msg_df):
    # consider only final REGTOPIC message
    df = msg_df.dropna(subset=['ok', 'topic', 'total_wtime'], inplace=False)
    fig, ax = plt.subplots()
    sns.violinplot(x='topic',y='total_wtime', data=df, ax = ax, cut = True)
    fig.savefig(fig_dir + 'waiting_time.'+form,format=form)


def plot_times_registered(fig_dir, msg_df):
    # consider only final REGTOPIC message
    df = msg_df.dropna(subset=['ok', 'topic', 'total_wtime'], inplace=False)
    df = df.groupby('peer_id')

    fig, axes = plt.subplots()
    df['ok'].value_counts().plot(ax=axes, kind='bar')
    axes.set_xticklabels([])
    axes.set_xlabel("Advertiser Node")
    axes.set_ylabel("Successful Registration Count")
    fig.savefig(fig_dir + 'times_registered.'+form,format=form)


def plot_storage_per_node_over_time(fig_dir, storage_df):
    fig, axes = plt.subplots()
    axes.set_xlabel("Time (msec)")
    axes.set_ylabel("Number of active registrations stored")
    for column_name in storage_df:
        if 'node' in column_name:
            storage_df.plot(ax=axes, x='timestamp', y=column_name)
    lgd = axes.legend(loc=9, bbox_to_anchor=(0.5,-0.09), ncol=4)
    fig.savefig(fig_dir + 'storage_time.'+form,format=form, bbox_extra_artists=(lgd,), bbox_inches='tight')

def plot_ads_per_node_over_time(fig_dir, adverts_df):
    fig, axes = plt.subplots()
    axes.set_xlabel("Time (msec)")
    axes.set_ylabel("Number of active advertisements")
    for column_name in adverts_df:
        if 'node' in column_name:
            adverts_df.plot(ax=axes, x='timestamp', y=column_name)
    lgd = axes.legend(loc=9, bbox_to_anchor=(0.5,-0.09), ncol=4)
    fig.savefig(fig_dir + 'advertisement_time.'+form,format=form, bbox_extra_artists=(lgd,), bbox_inches='tight')

def plot_mean_waiting_time(fig_dir, msg_df):
    wtime_df = msg_df.dropna(subset=['wtime'])

    fig, ax = plt.subplots()
    wtime_df.groupby("node_id").wtime.mean().plot(kind='bar', ax=ax, title="Average issued wtime per Node")
    ax.set_xlabel("Node ID")
    ax.set_ylabel("Average issued waiting time")
    fig.savefig(fig_dir + 'waiting_time_issued_avg.'+form, format=form)

    fig, ax = plt.subplots()
    wtime_df.groupby("peer_id").wtime.mean().plot(kind='bar', ax=ax, title="Average Received wtime per Node")
    ax.set_xlabel("Node ID")
    ax.set_ylabel("Average received waiting time")
    fig.savefig(fig_dir + 'waiting_time_recv_avg.'+form, format=form)

#storage_df, advert_dist_df = get_storage_and_advertisement_dist_df(log_path)
#print('Storage_df:', storage_df)
#plot_storage_per_node_over_time('./', storage_df)

#op_df = get_op_df('./discv5-test/logs')
#print("op_df")
#print(op_df)
#msg_df = get_msg_df('./discv5-test/logs', op_df)
#msg_df = msg_df.dropna(subset=['tmp'])
#print("msg_df")
#print(msg_df)



#df = logs_into_df(log_path)

#print(df['msg_type'].value_counts())
#print(df['in_out'].value_counts())



def create_dfs(out_dir):
    logs_dir = os.path.join(out_dir, 'logs') + "/"
    df_dir = os.path.join(out_dir, 'dfs')
    if not os.path.exists(df_dir):
        os.mkdir(df_dir)

    print('Computing op_df')
    op_df = get_op_df(logs_dir)
    op_df.to_json(os.path.join(df_dir, 'op_df.json'))
    print('Written to op_df.json')

    print('Computing msg_df')
    msg_df = get_msg_df(logs_dir, op_df)
    msg_df = msg_df.dropna(subset=['opid'])
    msg_df.to_json(os.path.join(df_dir, 'msg_df.json'))
    print('Written to msg_df.json')

    print('Computing storage_df, advert_dist_df')
    storage_df, advert_dist_df = get_storage_and_advertisement_dist_df(logs_dir)
    storage_df.to_json(os.path.join(df_dir, 'storage_df.json'))
    print('Written to storage_df.json')
    advert_dist_df.to_json(os.path.join(df_dir, 'advert_dist_df.json'))
    print("Written to advert_dist_df.json")

def plot_dfs(out_dir):
    fig_dir = os.path.join(out_dir, 'figs') + "/"
    df_dir = os.path.join(out_dir, 'dfs')
    msg_df = pd.read_json(os.path.join(df_dir, 'msg_df.json'))
    op_df = pd.read_json(os.path.join(df_dir, 'op_df.json'))

    plot_operation_returned(fig_dir,op_df)

    plot_operation_times(fig_dir,op_df)

    plot_msg_operation(fig_dir, msg_df)

    plot_msg_topic(fig_dir,msg_df)

    plot_times_discovered(fig_dir,op_df)

    plot_times_registered(fig_dir, msg_df)

    plot_search_results(fig_dir,op_df)

    plot_waiting_time(fig_dir,msg_df)

    plot_mean_waiting_time(fig_dir,msg_df)
    plt.close()


def plot_new(out_dir):
    fig_dir = os.path.join(out_dir, 'figs') + "/"
    df_dir = os.path.join(out_dir, 'dfs')
    advert_dist_df = pd.read_csv(os.path.join(df_dir, 'advert_dist_df.json'))
    storage_df = pd.read_csv(os.path.join(df_dir, 'storage_df.json'))

    means = []
    errs = []
    keys = []
    mins = []
    maxs = []
    fig, ax = plt.subplots()
    for key, group in storage_df.groupby('timestamp'):
        print("key", key)

        print("mean", group['num_ads_stored'].mean(), "stderr", group['num_ads_stored'].std())
        keys.append(key)
        means.append(group['num_ads_stored'].mean())
        errs.append(group['num_ads_stored'].std())

        max_val = group['num_ads_stored'].max()
        min_val = group['num_ads_stored'].min()
        val_cnt = group['num_ads_stored'].value_counts()
        print(val_cnt)
        maxs.append(max_val)
        mins.append(min_val)

        ax.annotate(val_cnt[max_val], (key, max_val))
        ax.annotate(val_cnt[min_val], (key, min_val))


    ax.errorbar(keys, means, errs)
    ax.plot(keys, maxs)
    ax.plot(keys, mins)

    plt.show()
    #print(storage_df)

    # TODO update plotting (ones below won't work anymore) - dataframes have been updated for heat maps
    #plot_storage_per_node_over_time(fig_dir, storage_df)
    #plot_ads_per_node_over_time(fig_dir, advert_dist_df)

def analyze(out_dir):
    create_dfs(out_dir)
    plot_dfs(out_dir)


def main():
    directory = "../discv5-test"
    if len(sys.argv) > 1:
        directory = sys.argv[1]
    create_dfs(directory)
    plot_dfs(directory)
    #plot_new(directory)

if __name__ == "__main__":
    main()
