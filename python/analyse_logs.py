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
import numpy
import heapq # to sort register removal events 
import time 

#log_path = "./discv5-test/logs"
#log_path = "./discv5_test_logs/benign/_nodes-100_topic-1_regBucketSize-10_searchBucketSize-3_adLifetimeSeconds-60_adCacheSize-500_rpcBasePort-20200_udpBasePort-30200_returnedNodes-5/logs/"
form = 'pdf'
#log_path = "../discv5-test/logs"

def get_storage_df(log_path):
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
        advertiser = int(log_file.split('-')[1].split('.')[0]) #node-10.log
        nodes.add(advertiser)
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
            registrar = int(jsons['addr'].split(':')[1]) - 30200

            if(registrar == advertiser):
                print('This should not happen - node is sending itself a REGCONFIRMATION: ', line)
            # parse time
            dt = parse(jsons['t'])
            unix_time = int(time.mktime(dt.timetuple()))

            heapq.heappush(reg_events_heap, (unix_time, registrar, adlifetime, advertiser))


    table_size = {}
    table_size_ot = {} # table size over time 
    init_time = None
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
                expiry_time, node = heapq.heappop(ad_expiry_heap)
                table_size[node] = table_size[node] - 1
                if expiry_time not in table_size_ot.keys():
                    table_size_ot[expiry_time] = {}

                table_size_ot[expiry_time][node] = table_size[node]
                #print('Ad expiration  at registrar: ', node, 'at time:', expiry_time, 'setting table size to', table_size[node])
            else:
                break

        if registrar not in table_size.keys():
            table_size[registrar] = 1
        else:
            table_size[registrar] = table_size[registrar] + 1

        if timestamp not in table_size_ot.keys():
            table_size_ot[timestamp] = {}
        table_size_ot[timestamp][registrar] = table_size[registrar]
        #print('Timestamp:', timestamp, 'registrar:', registrar, 'size:', table_size[registrar], 'adlifetime:', adlifetime)
        heapq.heappush(ad_expiry_heap, (timestamp + adlifetime, registrar))

    rows = []
    times = list(table_size_ot.keys())
    times = sorted(times)
    #print('table_size_ot: ', table_size_ot)
    table_size = {}
    for node in list(nodes):
        table_size[node] = 0
    #print('nodes:', list(nodes))
        
    for timestamp in times:
        #print ('Timestamp:', timestamp)
        row = {}
        reg_events = table_size_ot[timestamp]
        for registrar in reg_events.keys():
            table_size[registrar] = reg_events[registrar]
            #print('setting registar', registrar, 'table size to', reg_events[registrar])
        for node in list(nodes):
            row['timestamp'] = timestamp
            row['node' + str(node)] = table_size[node]
            #print('Setting row node', str(node), 'to', table_size[node])
            
        rows.append(row)
    storage_df = pd.DataFrame(rows)

    return storage_df

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
    for log_file in os.listdir(log_path):
        if (not log_file.startswith("node-")):
            continue
        print("Reading", log_file)
        node_id = log_file.split('-')[1].split('.')[0] #node-10.log
        for line in open(log_path + '/' + log_file, 'r').readlines():
            #not a json line
            if(line[0] != '{'):
                continue
            row = {}
            row['node_id'] = node_id
            #print("\t", line)
            jsons = json.loads(line)
            #it's not a message sent between peers
            if('addr' not in jsons):
                continue
            #get peer ID from the port number
            row['peer_id'] = int(jsons['addr'].split(':')[1]) - 30200
            in_out_s = jsons['msg'].split(' ')[0]
            if(in_out_s == '<<'):
                row['in_out'] = 'in'
            elif(in_out_s == '>>'):
                row['in_out'] = 'out'
            else:
                #it's not a message sent between peers
                continue
            row['timestamp'] = parse(jsons['t'])
            row['msg_type'] = jsons['msg'].split(' ')[1].split(':')[0]
            if('req' in jsons):
                row['req_id'] = jsons['req']
            #print(row)
            if('opid' in jsons):
                row['opid'] = jsons['opid']

            if("total-wtime" in jsons):
                row['total_wtime'] = jsons['total-wtime']
            if("wtime" in jsons):
                row['wtime'] = jsons['wtime']
            if("ok" in jsons):
                row['ok'] = jsons['ok']

            #we have a key to the message specified
            #currently it can only be the topic
            if("topic" in jsons):
                #replace topic digest by topic name
                topic = jsons['topic']
                row['key'] = topic_mapping[topic]
            #print(row)
            rows.append(row)


    msg_df = pd.DataFrame(rows)
    #keep only the send messages (as they have the opid)
    msg_df = msg_df[msg_df['in_out'] == 'out']

    mapping = {}
    def process(row):
        op_id = row['opid']
        req_id = row['req_id']

        if(not numpy.isnan(op_id)):
            mapping[req_id] = op_id

        if(req_id in mapping):
            row['tmp'] = mapping[req_id]
            if(not numpy.isnan(row['tmp'])):
                row['topic'] = op_info[row['tmp']]['topic']
                row['op_type'] = op_info[row['tmp']]['op_type']
        else:
            row['tmp'] = numpy.NaN
            row['topic'] = numpy.NaN
            row['op_type'] = numpy.NaN
        return row

    msg_df = msg_df.apply(lambda row : process(row), axis = 1)
    msg_df['opid'] = msg_df['tmp']
    msg_df.drop('tmp', axis=1, inplace=True)
    msg_df = msg_df.dropna(subset=['opid'])

    return msg_df

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

def plot_msg_sent_recv(fig_dir,msg_df):
    ax = msg_df['in_out'].value_counts().plot(kind='pie', autopct='%1.0f%%', legend=True, title='Msgs sent/received')
    ax.figure.savefig(fig_dir + 'msg_sent_received.'+form,format=form)

def plot_msg_sent_recv2(fig_dir,msg_df):
    sent = msg_df[msg_df['in_out'] == 'out']['node_id'].value_counts().to_dict()
    sent = {int(k):int(v) for k,v in sent.items()} #convert IDs to int

    received = msg_df[msg_df['in_out'] == 'out']['peer_id'].value_counts().to_dict()
    received = {int(k):int(v) for k,v in received.items()} #convert IDs to int

    fig, ax = plt.subplots()

    width =0.3
    ax.bar(sent.keys(), sent.values(), width=width, label = 'sent')
    ax.bar([x + width for x in received.keys()], received.values(), width=width, label = 'received')
    ax.legend()
    ax.set_title('Messages exchanged')
    ax.set_xlabel('Node ID')
    ax.set_ylabel('#Messages')
    fig.savefig(fig_dir + 'messages.'+form,format=form)

def plot_msg_sent_distrib(fig_dir,msg_df):
    fig, axes = plt.subplots()

    df_in = msg_df[msg_df['in_out']=='in']['node_id'].value_counts().rename_axis('node_id').reset_index(name='count')
    df_in['in_out'] = 'in'
    df_out = msg_df[msg_df['in_out']=='out']['node_id'].value_counts().rename_axis('node_id').reset_index(name='count')
    df_out['in_out'] = 'out'

    df = pd.concat([df_in, df_out], axis=0)
    sns.violinplot(x='in_out', y='count', data=df, ax = axes, cut=0, title='#Msg received/sent distribution per node')
    fig.savefig(fig_dir + 'msg_rcv_dist.'+form,format=form)


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

    #consider only final regconfig message
    df = msg_df.dropna(subset=['ok'], inplace=False)
    fig, ax = plt.subplots()
    sns.violinplot(x='topic',y='total_wtime', data=df, ax = ax, cut = True)
    fig.savefig(fig_dir + 'waiting_time.'+form,format=form)

def plot_storage_per_node_over_time(fig_dir, storage_df):
    fig, axes = plt.subplots()
    axes.set_xlabel("Time (msec)")
    axes.set_ylabel("Number of active registrations stored")
    for column_name in storage_df:
        if 'node' in column_name:
            storage_df.plot(ax=axes, x='timestamp', y=column_name)
    lgd = axes.legend(loc=9, bbox_to_anchor=(0.5,-0.09), ncol=4)
    fig.savefig(fig_dir + 'storage_time.'+form,format=form, bbox_extra_artists=(lgd,), bbox_inches='tight')

#storage_df = get_storage_df(log_path)
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



def analyze(out_dir):
    logs_dir = os.path.join(out_dir, 'logs') + "/"
    fig_dir = os.path.join(out_dir, 'figs') + "/"

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

    plt.close()


def main():
    directory = "../discv5-test"
    if len(sys.argv) > 1:
        directory = sys.argv[1]
    analyze(directory)

if __name__ == "__main__":
    main()
